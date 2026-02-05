"""
DTU Findit Master Thesis PDF pipeline: enumerate records, download PDFs via Selenium,
upload to GCS, temp-only storage, resumable and rate-limited.
"""

import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Config
TARGET_URL = "https://findit.dtu.dk/en/catalog?dtu=student_theses&q=&type=thesis_master"
BASE_URL = "https://findit.dtu.dk"
PER_PAGE = 20
WAIT_TIMEOUT = 120
PAGE_WAIT = 10
DOWNLOAD_WAIT = 60
DELAY_BETWEEN_RECORDS = 2
MAX_RETRIES = 3
GCS_PREFIX = os.environ.get("GCS_PREFIX", "dtu_findit/master_thesis")
PROGRESS_FILE = "progress.json"
MAX_RECORDS = int(os.environ.get("MAX_RECORDS", "0")) or None  # 0 or unset = no limit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def get_gcs_client(): # returns a GCS client object
    return storage.Client()

def get_bucket(): # returns a GCS bucket object
    name = os.environ.get("GCS_BUCKET")
    if not name:
        raise SystemExit("Set GCS_BUCKET environment variable.")
    return get_gcs_client().bucket(name)

def blob_key(record_id: str) -> str: # returns the key for the blob
    return f"{GCS_PREFIX}/{record_id}.pdf"

def blob_exists(bucket, record_id: str) -> bool: # returns True if the blob exists
    blob = bucket.blob(blob_key(record_id))
    return blob.exists()

def load_progress() -> set[str]: # returns a set of processed record IDs
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
            return set(data.get("processed_record_ids", []))
    return set()

def save_progress(processed: set[str]): # saves the processed record IDs to the progress file
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"processed_record_ids": list(processed)}, f, indent=2)

def get_driver(download_dir: str):
    opts = Options()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    return webdriver.Chrome(options=opts)


def wait_for_results_list(driver):
    def has_result_links(d):
        soup = BeautifulSoup(d.page_source, "html.parser")
        return len(extract_detail_urls_from_soup(soup)) > 0

    WebDriverWait(driver, WAIT_TIMEOUT).until(has_result_links)


def extract_detail_urls_from_soup(soup) -> list[tuple[str, str]]:
    """Return [(record_id, url), ...] for thesis detail pages."""
    seen = set()
    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        full = urljoin(BASE_URL, href)
        parsed = urlparse(full)
        if parsed.netloc != "findit.dtu.dk":
            continue
        path = parsed.path.rstrip("/")
        m = re.match(r"^/en/catalog/([^/]+)$", path)
        if m and "?" not in m.group(1) and m.group(1) != "export":
            rid = m.group(1)
            if rid not in seen:
                seen.add(rid)
                out.append((rid, full))
    return out


def catalog_page_url(start: int) -> str:
    sep = "&" if "?" in TARGET_URL else "?"
    return f"{TARGET_URL}{sep}start={start}"


def enumerate_all_records(driver) -> list[tuple[str, str]]:
    """Paginate through catalog and yield (record_id, url) for each thesis."""
    all_records = []
    start = 0
    seen_ids = set()

    while True:
        url = catalog_page_url(start)
        driver.get(url)
        WebDriverWait(driver, PAGE_WAIT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        records = extract_detail_urls_from_soup(soup)
        if not records:
            break
        for rid, u in records:
            if rid not in seen_ids:
                seen_ids.add(rid)
                all_records.append((rid, u))
        if len(records) < PER_PAGE:
            break
        start += PER_PAGE
        time.sleep(DELAY_BETWEEN_RECORDS)

    return all_records


def find_download_button(driver):
    """Locate Download button/link. Update selectors if DTU Findit UI changes."""
    for by, val in [
        (By.LINK_TEXT, "Download"),
        (By.PARTIAL_LINK_TEXT, "Download"),
        (By.XPATH, "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]"),
        (By.CSS_SELECTOR, "a[href*='.pdf']"),
        (By.CSS_SELECTOR, "a[href*='fulltext-gateway']"),
        (By.XPATH, "//a[contains(@class,'download')]"),
        (By.XPATH, "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]"),
    ]:
        try:
            el = driver.find_element(by, val)
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def wait_for_downloaded_pdf(download_dir: Path, timeout: int = DOWNLOAD_WAIT) -> Path | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pdfs = list(download_dir.glob("*.pdf"))
        crdownload = list(download_dir.glob("*.crdownload"))
        if crdownload:
            time.sleep(0.5)
            continue
        if pdfs:
            return pdfs[0]
        time.sleep(0.5)
    return None


def download_pdf_for_record(driver, url: str, download_dir: Path) -> Path | None:
    driver.get(url)
    WebDriverWait(driver, PAGE_WAIT).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(1)
    btn = find_download_button(driver)
    if not btn:
        log.warning("No Download button found for %s", url)
        return None
    for f in download_dir.glob("*"):
        f.unlink(missing_ok=True)
    btn.click()
    return wait_for_downloaded_pdf(download_dir)


def upload_to_gcs(bucket, local_path: Path, record_id: str):
    key = blob_key(record_id)
    blob = bucket.blob(key)
    blob.upload_from_filename(str(local_path), content_type="application/pdf")


def main():
    bucket_name = os.environ.get("GCS_BUCKET")
    if not bucket_name:
        raise SystemExit("Set GCS_BUCKET environment variable.")

    with tempfile.TemporaryDirectory() as tmpdir:
        download_dir = Path(tmpdir)
        driver = get_driver(str(download_dir))
        bucket = get_bucket()
        processed = load_progress()

        try:
            driver.get(TARGET_URL)
            log.info("Complete verification and DTU login in the browser.")
            input("When you see the thesis results list, press Enter to continue... ")
            wait_for_results_list(driver)

            records = enumerate_all_records(driver)
            if MAX_RECORDS:
                records = records[:MAX_RECORDS]
                log.info("Limited to %d records (MAX_RECORDS)", MAX_RECORDS)
            log.info("Enumerated %d master thesis records", len(records))

            ok, skip, fail = 0, 0, 0
            for i, (record_id, url) in enumerate(records):
                if record_id in processed:
                    skip += 1
                    if (i + 1) % 100 == 0:
                        log.info("Progress: %d/%d ok=%d skip=%d fail=%d", i + 1, len(records), ok, skip, fail)
                    continue
                if blob_exists(bucket, record_id):
                    processed.add(record_id)
                    save_progress(processed)
                    skip += 1
                    continue

                for attempt in range(MAX_RETRIES):
                    try:
                        pdf_path = download_pdf_for_record(driver, url, download_dir)
                        if not pdf_path:
                            fail += 1
                            log.warning("No PDF downloaded: %s %s", record_id, url)
                            break
                        upload_to_gcs(bucket, pdf_path, record_id)
                        pdf_path.unlink(missing_ok=True)
                        processed.add(record_id)
                        save_progress(processed)
                        ok += 1
                        log.info("[%d/%d] Uploaded %s", i + 1, len(records), record_id)
                        if (i + 1) % 100 == 0:
                            log.info("Progress: ok=%d skip=%d fail=%d", ok, skip, fail)
                        break
                    except Exception as e:
                        log.warning("Attempt %d failed for %s: %s", attempt + 1, record_id, e)
                        if attempt == MAX_RETRIES - 1:
                            fail += 1
                            log.error("Failed after retries: %s %s", record_id, url)

                time.sleep(DELAY_BETWEEN_RECORDS)

            log.info("Done. Uploaded=%d Skipped=%d Failed=%d", ok, skip, fail)
        finally:
            driver.quit()


if __name__ == "__main__":
    main()
