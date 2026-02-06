"""Test download + upload for one record. Run: uv run python test_download.py"""

from dotenv import load_dotenv
load_dotenv()

import os
import tempfile
import time
from pathlib import Path

import config
import scraper
import storage
from bs4 import BeautifulSoup


def main():
    if not os.environ.get("GCS_BUCKET"):
        print("Set GCS_BUCKET in .env")
        return

    bucket = storage.get_bucket()
    with tempfile.TemporaryDirectory() as tmpdir:
        download_dir = Path(tmpdir)
        driver = scraper.get_driver(str(download_dir))
        try:
            driver.get(config.TARGET_URL)
            scraper.handle_dtu_findit_entry(driver)
            time.sleep(2)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            if len(scraper.extract_detail_urls_from_soup(soup)) == 0:
                try:
                    input("Log in if needed, then press Enter... ")
                except EOFError:
                    pass
            scraper.wait_for_results_list(driver)

            records = scraper.get_records_with_downloads_on_page(driver)
            print(f"Found {len(records)} records with download on this page")
            if not records:
                print("No download buttons found. Check page structure.")
                try:
                    input("Press Enter to close...")
                except EOFError:
                    pass
                return

            record_id, title, btn = records[0]
            print(f"Testing: {record_id} - {title[:50]}...")

            main_handle = driver.current_window_handle
            pdf_path = scraper.click_download_and_wait(driver, btn, download_dir, main_handle, driver.current_url)
            if not pdf_path:
                print("FAIL: No PDF downloaded")
                try:
                    input("Press Enter to close...")
                except EOFError:
                    pass
                return

            print(f"Downloaded: {pdf_path.name} ({pdf_path.stat().st_size} bytes)")
            storage.upload_to_gcs(bucket, pdf_path, record_id, title)
            print(f"Uploaded to gs://{os.environ['GCS_BUCKET']}/{storage.blob_key(record_id, title)}")
            print("SUCCESS")
        finally:
            try:
                input("Press Enter to close browser...")
            except EOFError:
                pass
            driver.quit()


if __name__ == "__main__":
    main()
