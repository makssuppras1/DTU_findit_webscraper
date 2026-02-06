"""Test record counts on page 1 and 2. Run: uv run python test_page_counts.py"""

from dotenv import load_dotenv
load_dotenv()

import tempfile
import time
from pathlib import Path

import config
import scraper

def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        driver = scraper.get_driver(str(tmpdir))
        try:
            driver.get(config.TARGET_URL)
            scraper.handle_dtu_findit_entry(driver)
            time.sleep(2)

            for page, expected in [(1, 4), (2, 8)]:
                url = scraper.catalog_page_url(page)
                driver.get(url)
                time.sleep(4)
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(driver.page_source, "html.parser")
                all_ids = scraper.extract_detail_urls_from_soup(soup)
                print(f"  URL: {driver.current_url}")
                print(f"  All record IDs on page: {all_ids}")
                records = scraper.get_records_with_downloads_on_page(driver)
                print(f"Page {page}: found {len(records)} with download (expected {expected})")
                for rid, title, _ in records:
                    print(f"  - {rid} {title[:50]}...")
        finally:
            driver.quit()

if __name__ == "__main__":
    main()
