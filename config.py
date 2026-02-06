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
DOWNLOAD_WAIT = 60
CHROME_PROFILE_DIR = os.environ.get("CHROME_PROFILE_DIR", "")

# Pipeline (increase if you get "too many requests")
DELAY_BETWEEN_RECORDS = float(os.environ.get("DELAY_BETWEEN_RECORDS", "5"))
DELAY_BETWEEN_RECORDS_PARALLEL = float(os.environ.get("DELAY_BETWEEN_RECORDS_PARALLEL", "2"))
DELAY_BETWEEN_PAGES = float(os.environ.get("DELAY_BETWEEN_PAGES", "3"))
MAX_RETRIES = 3
MAX_RECORDS = 20000
WORKERS = int(os.environ.get("WORKERS", "1"))
START_RECORD = int(os.environ.get("START_RECORD", "0")) or None  # e.g. 323 to skip first 322

# GCS
GCS_PREFIX = os.environ.get("GCS_PREFIX", "dtu_findit/master_thesis")
PROGRESS_FILE = os.environ.get("PROGRESS_FILE", "progress.json")

# Metadata (optional)
FULLTEXT_TOKEN = os.environ.get("FULLTEXT_TOKEN", "")
METADATA_CSV_PAGES = int(os.environ.get("METADATA_CSV_PAGES", "20"))
