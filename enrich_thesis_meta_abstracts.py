"""Enrich thesis_meta_combined.csv with abstracts from GCS extracted_metadata.json.
Left-joins on Title. Run with GCS_BUCKET set (e.g. thesis_archive_bucket).
"""
import csv
import json
import os
import sys
from pathlib import Path

from google.cloud import storage


def get_bucket():
    name = os.environ.get("GCS_BUCKET", "thesis_archive_bucket")
    try:
        client = storage.Client()
        return client.bucket(name)
    except Exception as e:
        raise SystemExit(
            f"GCS auth failed: {e}. Set GOOGLE_APPLICATION_CREDENTIALS or run: gcloud auth application-default login"
        ) from e


def load_extracted_metadata(bucket, path: str = "extracted_data/extracted_metadata.json") -> dict[str, str]:
    """Download JSON and build title -> abstract map. Normalize title (strip, single spaces)."""
    blob = bucket.blob(path)
    raw = blob.download_as_bytes().decode("utf-8", errors="replace")
    data = json.loads(raw)
    out = {}
    items = data if isinstance(data, list) else (data.get("entries", data) if isinstance(data, dict) else [])
    if isinstance(items, dict):
        items = items.values()
    for item in items:
        if not isinstance(item, dict):
            continue
        title = item.get("title") or item.get("Title")
        abstract = item.get("abstract") or item.get("abstract_ts") or item.get("Abstract")
        if title and abstract:
            key = " ".join(str(title).split()).strip()
            if key and key not in out:
                out[key] = str(abstract).strip()
    return out


def normalize_title(s: str) -> str:
    return " ".join(s.split()).strip() if s else ""


def csv_title_to_lookup_keys(title: str) -> list[str]:
    """Produce keys to look up in the JSON map. Prefer part before '|' (bilingual CSV)."""
    n = normalize_title(title)
    if not n:
        return []
    keys = [n]
    if "|" in n:
        keys.insert(0, normalize_title(n.split("|", 1)[0]))
    return keys


def main():
    csv_path = Path(__file__).resolve().parent / "Thesis_meta" / "thesis_meta_combined.csv"
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    bucket = get_bucket()
    print("Loading extracted_metadata.json from GCS...", file=sys.stderr)
    title_to_abstract = load_extracted_metadata(bucket)
    print(f"Loaded {len(title_to_abstract)} title->abstract entries.", file=sys.stderr)

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        rows = list(reader)

    if "Title" not in header or "abstract_ts" not in header:
        raise SystemExit("CSV must have columns 'abstract_ts' and 'Title'.")
    idx_abstract = header.index("abstract_ts")
    idx_title = header.index("Title")

    filled = 0
    for row in rows:
        if len(row) <= max(idx_abstract, idx_title):
            continue
        if row[idx_abstract].strip():
            continue
        for key in csv_title_to_lookup_keys(row[idx_title]):
            if key in title_to_abstract:
                row[idx_abstract] = title_to_abstract[key]
                filled += 1
                break

    print(f"Filled {filled} missing abstracts.", file=sys.stderr)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(rows)

    print(f"Updated {csv_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
