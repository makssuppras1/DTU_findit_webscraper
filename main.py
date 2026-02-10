"""
DTU Findit Master Thesis PDF pipeline: enumerate records, download PDFs via Selenium,
upload to GCS, temp-only storage, resumable and rate-limited.
"""

import argparse
import logging
import multiprocessing
import os
import tempfile
import time
from pathlib import Path

from bs4 import BeautifulSoup
from selenium.common.exceptions import InvalidSessionIdException, StaleElementReferenceException

import config
import metadata
import scraper
import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _run_worker(args):
    worker_id, allowed_starts, metadata_lookup = args
    bucket = storage.get_bucket()
    processed = storage.load_progress()
    ok, skip, fail = 0, 0, 0
    count = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        download_dir = Path(tmpdir)
        driver = scraper.get_driver(str(download_dir), worker_id)
        try:
            scraper.get_url_with_retry(driver, config.TARGET_URL)
            scraper.handle_dtu_findit_entry(driver)
            time.sleep(1)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            if len(scraper.extract_detail_urls_from_soup(soup)) == 0:
                log.warning("Worker %d: complete verification/login in browser", worker_id)
                return (worker_id, ok, skip, fail)
            scraper.wait_for_results_list(driver)
            main_handle = driver.current_window_handle
            worker_limit = (config.MAX_RECORDS // config.WORKERS) if config.MAX_RECORDS else None
            for record_id, title, download_btn in scraper.iterate_pages_with_records(driver, allowed_starts):
                if worker_limit and count >= worker_limit:
                    break
                count += 1
                if record_id in processed:
                    skip += 1
                    continue
                if storage.blob_exists(bucket, record_id):
                    processed.add(record_id)
                    storage.save_progress_add(record_id)
                    skip += 1
                    continue
                for attempt in range(config.MAX_RETRIES):
                    try:
                        results_url = driver.current_url
                        if attempt == 0:
                            download_btn_to_use = download_btn
                        else:
                            try:
                                _, _, download_btn_to_use = next(
                                    (rid, t, b) for rid, t, b in scraper.get_records_with_downloads_on_page(driver)
                                    if rid == record_id
                                )
                            except StopIteration:
                                log.warning("Record %s not found on page", record_id)
                                break
                        pdf_path = scraper.click_download_and_wait(
                            driver, download_btn_to_use, download_dir, main_handle, results_url
                        )
                        if not pdf_path:
                            fail += 1
                            break
                        meta = metadata_lookup.get(record_id, {})
                        storage.upload_to_gcs(bucket, pdf_path, record_id, title, metadata=meta or None)
                        pdf_path.unlink(missing_ok=True)
                        processed.add(record_id)
                        storage.save_progress_add(record_id)
                        ok += 1
                        log.info("[W%d] Uploaded %s", worker_id, title[:40] if title else record_id)
                        break
                    except InvalidSessionIdException:
                        log.error("Worker %d: browser session lost", worker_id)
                        raise
                    except StaleElementReferenceException:
                        log.warning("Worker %d: stale element for %s, retrying", worker_id, record_id)
                        if attempt < config.MAX_RETRIES - 1:
                            driver.get(results_url)
                            scraper.wait_for_results_list(driver)
                            time.sleep(2)
                        else:
                            fail += 1
                    except Exception as e:
                        log.warning("Worker %d attempt %d failed for %s: %s", worker_id, attempt + 1, record_id, e)
                        if attempt == config.MAX_RETRIES - 1:
                            fail += 1
                        else:
                            driver.get(results_url)
                            scraper.wait_for_results_list(driver)
                            time.sleep(2)
                time.sleep(config.DELAY_BETWEEN_RECORDS_PARALLEL)
        finally:
            try:
                driver.quit()
            except InvalidSessionIdException:
                pass
    return (worker_id, ok, skip, fail)


def _run_main_single(bucket, metadata_lookup):
    with tempfile.TemporaryDirectory() as tmpdir:
        download_dir = Path(tmpdir)
        driver = scraper.get_driver(str(download_dir))
        processed = storage.load_progress()
        try:
            scraper.get_url_with_retry(driver, config.TARGET_URL)
            scraper.handle_dtu_findit_entry(driver)
            time.sleep(1)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            if len(scraper.extract_detail_urls_from_soup(soup)) == 0:
                log.info("Complete verification and DTU login in the browser if needed.")
                input("When you see the thesis results list, press Enter to continue... ")
            scraper.wait_for_results_list(driver)
            main_handle = driver.current_window_handle
            ok, skip, fail = 0, 0, 0
            count = 0
            session_lost = False
            try:
                records_iter = scraper.iterate_pages_with_records(driver)
                for record_id, title, download_btn in records_iter:
                    if session_lost:
                        break
                    if config.MAX_RECORDS and count >= config.MAX_RECORDS:
                        log.info("Reached MAX_RECORDS=%d", config.MAX_RECORDS)
                        break
                    count += 1
                    if record_id in processed:
                        skip += 1
                        if count % 100 == 0:
                            log.info("Progress: %d ok=%d skip=%d fail=%d", count, ok, skip, fail)
                        continue
                    if storage.blob_exists(bucket, record_id):
                        processed.add(record_id)
                        storage.save_progress_add(record_id)
                        skip += 1
                        continue
                    for attempt in range(config.MAX_RETRIES):
                        try:
                            results_url = driver.current_url
                            if attempt == 0:
                                download_btn_to_use = download_btn
                            else:
                                try:
                                    _, _, download_btn_to_use = next(
                                        (rid, t, b) for rid, t, b in scraper.get_records_with_downloads_on_page(driver)
                                        if rid == record_id
                                    )
                                except StopIteration:
                                    log.warning("Record %s not found on page", record_id)
                                    break
                            pdf_path = scraper.click_download_and_wait(
                                driver, download_btn_to_use, download_dir, main_handle, results_url
                            )
                            if not pdf_path:
                                fail += 1
                                log.warning("No PDF downloaded: %s", record_id)
                                break
                            meta = metadata_lookup.get(record_id, {})
                            storage.upload_to_gcs(bucket, pdf_path, record_id, title, metadata=meta or None)
                            pdf_path.unlink(missing_ok=True)
                            processed.add(record_id)
                            storage.save_progress_add(record_id)
                            ok += 1
                            log.info("[%d] Uploaded %s", count, title[:50] if title else record_id)
                            if count % 100 == 0:
                                log.info("Progress: ok=%d skip=%d fail=%d", ok, skip, fail)
                            break
                        except InvalidSessionIdException:
                            log.error("Browser session lost, stopping. Progress saved.")
                            session_lost = True
                            break
                        except StaleElementReferenceException:
                            log.warning("Stale element for %s, retrying", record_id)
                            if attempt < config.MAX_RETRIES - 1:
                                driver.get(results_url)
                                scraper.wait_for_results_list(driver)
                                time.sleep(2)
                            else:
                                fail += 1
                        except Exception as e:
                            log.warning("Attempt %d failed for %s: %s", attempt + 1, record_id, e)
                            if attempt == config.MAX_RETRIES - 1:
                                fail += 1
                                log.exception("Failed after retries: %s", record_id)
                            else:
                                driver.get(results_url)
                                scraper.wait_for_results_list(driver)
                                time.sleep(2)
                    time.sleep(config.DELAY_BETWEEN_RECORDS)
            except InvalidSessionIdException:
                log.error("Browser session lost (browser closed?), stopping. Progress saved.")
                session_lost = True
            log.info("Done. Uploaded=%d Skipped=%d Failed=%d%s", ok, skip, fail,
                     " (restart to continue)" if session_lost else "")
        finally:
            try:
                driver.quit()
            except InvalidSessionIdException:
                pass


def _run_main_parallel(bucket, metadata_lookup):
    all_starts = set(range(0, 20000, config.PER_PAGE))
    chunks = [set() for _ in range(config.WORKERS)]
    for i, s in enumerate(all_starts):
        chunks[i % config.WORKERS].add(s)
    args_list = [(w, chunks[w], metadata_lookup) for w in range(config.WORKERS)]
    log.info("Starting %d workers", config.WORKERS)
    with multiprocessing.Pool(config.WORKERS) as pool:
        results = pool.map(_run_worker, args_list)
    total_ok = sum(r[1] for r in results)
    total_skip = sum(r[2] for r in results)
    total_fail = sum(r[3] for r in results)
    log.info("Done. Uploaded=%d Skipped=%d Failed=%d", total_ok, total_skip, total_fail)


def main():
    bucket_name = os.environ.get("GCS_BUCKET")
    if not bucket_name:
        raise SystemExit("Set GCS_BUCKET environment variable.")

    project = storage.get_project()
    log.info("Target: gs://%s (project: %s)", bucket_name, project or "default")
    log.info("MAX_RECORDS=%s", config.MAX_RECORDS or "unlimited")
    if config.START_RECORD:
        log.info("START_RECORD=%d (page %d)", config.START_RECORD, (config.START_RECORD - 1) // config.PER_PAGE + 1)

    try:
        bucket = storage.get_bucket()
        bucket.reload()
        log.info("Connected to GCS bucket: %s", bucket_name)
    except Exception as e:
        log.error("Cannot connect to GCS: %s", e)
        raise

    metadata_lookup = metadata.fetch_metadata_lookup() if config.FULLTEXT_TOKEN else {}

    if config.WORKERS <= 1:
        _run_main_single(bucket, metadata_lookup)
    else:
        _run_main_parallel(bucket, metadata_lookup)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sync-progress", action="store_true", help="Sync progress.json with actual GCS blobs")
    args = parser.parse_args()
    if args.sync_progress:
        storage.sync_progress_with_bucket()
    else:
        main()
