"""Configuration from environment. Load .env before importing."""

import os

from dotenv import load_dotenv

load_dotenv()

# DTU Findit
TARGET_URL = "https://findit.dtu.dk/en/catalog?dtu=student_theses&q=&type=thesis_master"
BASE_URL = "https://findit.dtu.dk"
PER_PAGE = int(os.environ.get("PER_PAGE", "10"))

# Browser
WAIT_TIMEOUT = 120
PAGE_WAIT = 10
DOWNLOAD_WAIT = int(os.environ.get("DOWNLOAD_WAIT", "120"))
CONNECTION_RETRIES = int(os.environ.get("CONNECTION_RETRIES", "5"))
CHROME_PROFILE_DIR = os.environ.get("CHROME_PROFILE_DIR", "")

# Pipeline (increase if you get "too many requests")
DELAY_BETWEEN_RECORDS = float(os.environ.get("DELAY_BETWEEN_RECORDS", "5"))
DELAY_BETWEEN_RECORDS_PARALLEL = float(os.environ.get("DELAY_BETWEEN_RECORDS_PARALLEL", "2"))
DELAY_BETWEEN_PAGES = float(os.environ.get("DELAY_BETWEEN_PAGES", "3"))
MAX_RETRIES = 3
MAX_RECORDS = 20000
WORKERS = int(os.environ.get("WORKERS", "1"))
_start_record = int(os.environ.get("START_RECORD", "0")) or None
_start_page = int(os.environ.get("START_PAGE", "0")) or None
START_RECORD = ((_start_page - 1) * PER_PAGE + 1) if _start_page else _start_record

# GCS
GCS_PREFIX = os.environ.get("GCS_PREFIX", "dtu_findit/master_thesis")
GCS_UPLOAD_TIMEOUT = int(os.environ.get("GCS_UPLOAD_TIMEOUT", "300"))
PROGRESS_FILE = os.environ.get("PROGRESS_FILE", "progress.json")

# Metadata (optional)
FULLTEXT_TOKEN = os.environ.get("FULLTEXT_TOKEN", "")
METADATA_CSV_PAGES = int(os.environ.get("METADATA_CSV_PAGES", "20"))
