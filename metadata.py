"""Fetch metadata from DTU Findit CSV API."""

import csv
import io
import logging

import requests

from config import FULLTEXT_TOKEN, METADATA_CSV_PAGES

log = logging.getLogger(__name__)


def fetch_metadata_lookup() -> dict[str, dict]:
    if not FULLTEXT_TOKEN:
        return {}
    lookup = {}
    url = "https://findit.dtu.dk/en/catalog/download.csv"
    params = {
        "access_type": "dtu",
        "dtu": "student_theses",
        "fulltext_token": FULLTEXT_TOKEN,
        "per_page": 1000,
        "separator": ",",
        "sort": "id asc",
        "start": 0,
        "type": "thesis_master",
    }
    headers = {"Cookie": "shunt_hint=anonymous"}
    for page in range(1, METADATA_CSV_PAGES + 1):
        params["page"] = page
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60)
            r.raise_for_status()
            reader = csv.DictReader(io.StringIO(r.text))
            id_col = next((c for c in reader.fieldnames or [] if c.lower() in ("id", "record_id", "recordid")), None)
            if not id_col:
                id_col = reader.fieldnames[0] if reader.fieldnames else None
            for row in reader:
                rid = row.get(id_col, "").strip() if id_col else None
                if rid:
                    lookup[rid] = {k: str(v)[:1024] for k, v in row.items() if k and v}
        except Exception as e:
            log.warning("Metadata fetch page %d failed: %s", page, e)
    log.info("Loaded metadata for %d records", len(lookup))
    return lookup
