"""Enrich thesis_meta_combined.csv with abstracts from GCS extracted_metadata.json.
Joins on member_id_ss (ID from filename, e.g. 5d1c8d66d9001d146569a4a4). Run with GCS_BUCKET set.
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
    """Download JSON and build member_id -> abstract map. ID = filename before first '_'."""
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
        filename = item.get("filename") or ""
        abstract = item.get("abstract") or item.get("abstract_ts") or item.get("Abstract")
        if not abstract:
            continue
        if "_" in filename:
            member_id = filename.split("_", 1)[0].strip()
            if member_id and member_id not in out:
                out[member_id] = str(abstract).strip()
    return out


def main():
    csv_path = Path(__file__).resolve().parent / "Thesis_meta" / "thesis_meta_combined.csv"
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    bucket = get_bucket()
    print("Loading extracted_metadata.json from GCS...", file=sys.stderr)
    id_to_abstract = load_extracted_metadata(bucket)
    print(f"Loaded {len(id_to_abstract)} member_id->abstract entries.", file=sys.stderr)

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        rows = list(reader)

    if "abstract_ts" not in header or "member_id_ss" not in header:
        raise SystemExit("CSV must have columns 'abstract_ts' and 'member_id_ss'.")
    idx_abstract = header.index("abstract_ts")
    idx_member_id = header.index("member_id_ss")

    replaced = 0
    for row in rows:
        if len(row) <= max(idx_abstract, idx_member_id):
            continue
        member_id = row[idx_member_id].strip()
        if member_id and member_id in id_to_abstract:
            row[idx_abstract] = id_to_abstract[member_id]
            replaced += 1

    print(f"Replaced abstract for {replaced} rows (from metadata).", file=sys.stderr)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(rows)

    print(f"Updated {csv_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
