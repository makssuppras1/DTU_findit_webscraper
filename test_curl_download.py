"""Test if the DTU Findit CSV curl API allows downloading actual PDF files.
Run: FULLTEXT_TOKEN=xxx uv run python test_curl_download.py
"""

import csv
import io
import os
import sys
from pathlib import Path

import requests

# Config from env
FULLTEXT_TOKEN = os.environ.get("FULLTEXT_TOKEN", "498c619d505987ceb6c52678748a360b")
BASE = "https://findit.dtu.dk/en/catalog/download.csv"
PARAMS = {
    "access_type": "dtu",
    "dtu": "student_theses",
    "fulltext_token": FULLTEXT_TOKEN,
    "page": 1,
    "per_page": 5,
    "separator": ",",
    "sort": "id asc",
    "start": 0,
    "type": "thesis_master",
}
HEADERS = {"Cookie": "shunt_hint=anonymous"}


def main():
    print("1. Fetching CSV metadata...")
    r = requests.get(BASE, params=PARAMS, headers=HEADERS, timeout=30)
    r.raise_for_status()
    print(f"   Status: {r.status_code}, Content-Type: {r.headers.get('Content-Type')}")

    reader = csv.DictReader(io.StringIO(r.text))
    rows = list(reader)
    print(f"   Rows: {len(rows)}")
    if not rows:
        print("   No data rows.")
        return

    # Find columns that might contain URLs
    sample = rows[0]
    url_cols = []
    for col, val in sample.items():
        if val and ("http://" in val or "https://" in val):
            url_cols.append(col)

    if not url_cols:
        print("2. No URL columns in CSV (Reports, Backlink, etc. are empty for thesis_master).")
        print("")
        print("   RESULT: The curl API does NOT allow download of actual PDF files for master theses.")
        print("   - CSV export: metadata only, no file URLs")
        print("   - fulltext-gateway: requires browser auth (redirects to /verify)")
        print("   - PDFs must be fetched via Selenium (logged-in session)")
        return

    print(f"2. Found URL columns: {url_cols}")
    for col in url_cols:
        url = sample.get(col, "").strip()
        if url:
            print(f"   Testing: {url[:80]}...")
            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
                ct = resp.headers.get("Content-Type", "")
                print(f"   -> {resp.status_code}, Content-Type: {ct}")
                if "pdf" in ct:
                    print("   -> PDF download works!")
                elif "text" in ct or "html" in ct:
                    print("   -> Returns text/html, not PDF")
            except Exception as e:
                print(f"   -> Error: {e}")

    # Try adding .pdf to a base URL if we find one
    for col in url_cols:
        url = sample.get(col, "").strip()
        if url and not url.endswith(".pdf"):
            pdf_url = url.rstrip("/") + ".pdf"
            print(f"3. Trying .pdf variant: {pdf_url[:80]}...")
            try:
                resp = requests.get(pdf_url, headers=HEADERS, timeout=15)
                ct = resp.headers.get("Content-Type", "")
                print(f"   -> {resp.status_code}, Content-Type: {ct}")
                if "pdf" in ct:
                    print("   -> PDF download works with .pdf suffix!")
            except Exception as e:
                print(f"   -> Error: {e}")


if __name__ == "__main__":
    main()
