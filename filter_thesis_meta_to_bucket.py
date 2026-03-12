"""Write thesis_meta_combined_filtered.csv with only rows whose member_id_ss exists in the GCS bucket."""
import csv
import os
import sys
from pathlib import Path

from google.cloud import storage

try:
    from config import GCS_PREFIX
except ImportError:
    GCS_PREFIX = os.environ.get("GCS_PREFIX", "dtu_findit/master_thesis")


def get_bucket():
    name = os.environ.get("GCS_BUCKET", "thesis_archive_bucket")
    try:
        return storage.Client().bucket(name)
    except Exception as e:
        raise SystemExit(
            f"GCS auth failed: {e}. Set GOOGLE_APPLICATION_CREDENTIALS or run: gcloud auth application-default login"
        ) from e


def member_ids_in_bucket(bucket) -> set[str]:
    prefix = GCS_PREFIX.rstrip("/") + "/"
    ids = set()
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name
        if name.startswith(prefix):
            rest = name[len(prefix) :]
            if "_" in rest:
                ids.add(rest.split("_", 1)[0])
    return ids


def main():
    base = Path(__file__).resolve().parent / "Thesis_meta" / "thesis_meta_combined.csv"
    out_path = base.parent / "thesis_meta_combined_filtered.csv"
    if not base.exists():
        raise SystemExit(f"CSV not found: {base}")

    bucket = get_bucket()
    print("Listing blobs in bucket...", file=sys.stderr)
    in_bucket = member_ids_in_bucket(bucket)
    print(f"Found {len(in_bucket)} member IDs in bucket.", file=sys.stderr)

    with open(base, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        rows = list(reader)

    if "member_id_ss" not in header:
        raise SystemExit("CSV must have column 'member_id_ss'.")
    idx_member_id = header.index("member_id_ss")

    filtered = [row for row in rows if len(row) > idx_member_id and row[idx_member_id].strip() in in_bucket]
    filtered = [row for row in filtered if any(cell.strip() for cell in row)]
    print(f"Keeping {len(filtered)} of {len(rows)} rows.", file=sys.stderr)

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        writer.writerows(filtered)

    print(f"Wrote {out_path}", file=sys.stderr)

    blob_path = "dtu_findit/master_thesis_meta/thesis_meta_combined_filtered.csv"
    bucket.blob(blob_path).upload_from_filename(str(out_path), content_type="text/csv")
    print(f"Uploaded to gs://{bucket.name}/{blob_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
