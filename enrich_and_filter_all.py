"""
Enrich thesis_meta_combined.csv with abstracts from GCS, then filter to rows whose
member_id_ss exists in thesis_archive_bucket/dtu_findit/master_thesis.

Steps:
  1. Load thesis_meta_combined.csv (semicolon-delimited).
  2. Load extracted_metadata from GCS (extracted_data/extracted_metadata.json or .csv).
     Member ID = filename before first "_"; abstract from "abstract" field.
  3. Fill empty abstract_ts where member_id_ss matches.
  4. List blobs under dtu_findit/master_thesis/; keep rows whose member_id_ss is in that set.
  5. Drop entirely empty rows.
  6. Write enriched+filtered to thesis_meta_combined_filtered.csv (default).

Usage:
  GCS_BUCKET=thesis_archive_bucket python enrich_and_filter_all.py
  python enrich_and_filter_all.py --input Thesis_meta/thesis_meta_combined.csv --output Thesis_meta/thesis_meta_combined_filtered.csv --metadata extracted_data/extracted_metadata.json
"""
import csv
import json
import logging
import os
import sys
from pathlib import Path

from google.cloud import storage

log = logging.getLogger(__name__)

BUCKET_NAME_DEFAULT = "thesis_archive_bucket"
METADATA_PATH_DEFAULT = "extracted_data/extracted_metadata.json"
MASTER_THESIS_PREFIX = "dtu_findit/master_thesis"
CSV_DELIM = ";"


def get_bucket(name: str | None = None):
    n = name or os.environ.get("GCS_BUCKET", BUCKET_NAME_DEFAULT)
    try:
        return storage.Client().bucket(n)
    except Exception as e:
        raise SystemExit(
            f"GCS auth failed: {e}. Set GOOGLE_APPLICATION_CREDENTIALS or run: gcloud auth application-default login"
        ) from e


def load_id_to_abstract_from_json(bucket, path: str) -> dict[str, str]:
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


def load_id_to_abstract_from_csv(bucket, path: str) -> dict[str, str]:
    blob = bucket.blob(path)
    raw = blob.download_as_bytes().decode("utf-8", errors="replace")
    reader = csv.DictReader(raw.splitlines(), delimiter=CSV_DELIM if CSV_DELIM in raw[:500] else ",")
    out = {}
    fn_col = None
    abs_col = None
    for c in (reader.fieldnames or []):
        if fn_col is None and c and "filename" in c.lower():
            fn_col = c
        if fn_col is None and c and ("member_id" in c.lower() or "id" == c.lower()):
            fn_col = c
        if abs_col is None and c and "abstract" in c.lower():
            abs_col = c
    if not abs_col:
        abs_col = next((c for c in (reader.fieldnames or []) if "abstract" in c.lower()), None)
    if not fn_col:
        fn_col = reader.fieldnames[0] if reader.fieldnames else None
    for row in reader:
        fn = (row.get(fn_col) or "").strip()
        abstract = (row.get(abs_col) or "").strip()
        if not abstract:
            continue
        if "_" in fn:
            member_id = fn.split("_", 1)[0].strip()
        else:
            member_id = fn
        if member_id and member_id not in out:
            out[member_id] = abstract
    return out


def load_id_to_abstract(bucket, path: str) -> dict[str, str]:
    if path.lower().endswith(".csv"):
        return load_id_to_abstract_from_csv(bucket, path)
    return load_id_to_abstract_from_json(bucket, path)


def member_ids_in_bucket(bucket, prefix: str) -> set[str]:
    prefix = prefix.rstrip("/") + "/"
    ids = set()
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name
        if name.startswith(prefix):
            rest = name[len(prefix) :]
            if "_" in rest:
                ids.add(rest.split("_", 1)[0])
    return ids


def main():
    import argparse
    p = argparse.ArgumentParser(description="Enrich all.csv with abstracts from GCS, then filter to bucket member_ids.")
    p.add_argument("--input", type=Path, default=Path("Thesis_meta/thesis_meta_combined.csv"), help="Input CSV")
    p.add_argument("--output", type=Path, default=Path("Thesis_meta/thesis_meta_combined_filtered.csv"), help="Output CSV (enriched + filtered)")
    p.add_argument("--metadata", default=METADATA_PATH_DEFAULT, help="GCS path to extracted_metadata.json or .csv")
    p.add_argument("--bucket", default=None, help="GCS bucket name (default: env GCS_BUCKET or thesis_archive_bucket)")
    p.add_argument("--prefix", default=MASTER_THESIS_PREFIX, help="Blob prefix for master thesis PDFs")
    p.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = p.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    inp = args.input
    if not inp.exists():
        raise SystemExit(f"Input not found: {inp}")

    bucket = get_bucket(args.bucket)

    # Load metadata for abstracts
    log.info("Loading metadata from gs://%s/%s", bucket.name, args.metadata)
    id_to_abstract = load_id_to_abstract(bucket, args.metadata)
    log.info("Loaded %d member_id -> abstract entries", len(id_to_abstract))

    # Read input CSV
    with open(inp, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=CSV_DELIM, quotechar='"')
        header = next(reader)
        rows = list(reader)

    if "abstract_ts" not in header or "member_id_ss" not in header:
        raise SystemExit("CSV must have columns 'abstract_ts' and 'member_id_ss'.")
    idx_abstract = header.index("abstract_ts")
    idx_member_id = header.index("member_id_ss")

    # Enrich: replace abstract_ts whenever we have metadata. member_id_ss can be "id1|id2"; use first match.
    replaced = 0
    for row in rows:
        if len(row) <= max(idx_abstract, idx_member_id):
            continue
        raw = row[idx_member_id].strip()
        if not raw:
            continue
        for member_id in raw.split("|"):
            member_id = member_id.strip()
            if member_id and member_id in id_to_abstract:
                row[idx_abstract] = id_to_abstract[member_id]
                replaced += 1
                break
    log.info("Replaced abstract for %d rows (from metadata)", replaced)

    # List member_ids in bucket
    log.info("Listing blobs under %s", args.prefix)
    in_bucket = member_ids_in_bucket(bucket, args.prefix)
    log.info("Found %d member IDs in bucket", len(in_bucket))

    # Filter to rows in bucket: member_id_ss can be a single ID or pipe-separated (id1|id2); keep if any is in bucket
    def row_in_bucket(row: list[str]) -> bool:
        if len(row) <= idx_member_id:
            return False
        raw = row[idx_member_id].strip()
        if not raw:
            return False
        for mid in raw.split("|"):
            if mid.strip() in in_bucket:
                return True
        return False

    filtered = [row for row in rows if row_in_bucket(row)]
    filtered = [row for row in filtered if any(cell.strip() for cell in row)]
    log.info("Keeping %d of %d rows (enriched + filtered)", len(filtered), len(rows))

    # Drop columns that are 100% empty
    ncols = len(header)
    keep_cols = []
    for j in range(ncols):
        if any(len(row) > j and row[j].strip() for row in filtered):
            keep_cols.append(j)
    header = [header[j] for j in keep_cols]
    filtered = [[row[j] if j < len(row) else "" for j in keep_cols] for row in filtered]
    log.info("Dropped %d all-empty columns; %d columns remain", ncols - len(keep_cols), len(header))

    # Write
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=CSV_DELIM, quotechar='"')
        writer.writerow(header)
        writer.writerows(filtered)
    log.info("Wrote %s", args.output)

    # Upload to GCS
    blob_path = "dtu_findit/master_thesis_meta/thesis_meta_combined_filtered.csv"
    bucket.blob(blob_path).upload_from_filename(str(args.output), content_type="text/csv")
    log.info("Uploaded to gs://%s/%s", bucket.name, blob_path)


if __name__ == "__main__":
    main()
