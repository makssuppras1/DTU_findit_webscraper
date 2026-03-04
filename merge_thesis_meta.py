"""Append all CSV files in Thesis_meta into one combined CSV and Parquet."""
import csv
from pathlib import Path

import pandas as pd

folder = Path("Thesis_meta")
out_csv = folder / "thesis_meta_combined.csv"
out_parquet = folder / "thesis_meta_combined.parquet"
csv_files = sorted(folder.glob("*.csv"), key=lambda p: (p.stem.split("_")[-1].zfill(3), p.name))

with open(out_csv, "w", newline="", encoding="utf-8") as out:
    writer = None
    for f in csv_files:
        if f == out_csv:
            continue
        with open(f, "r", encoding="utf-8") as inp:
            reader = csv.reader(inp, delimiter=";", quotechar='"')
            rows = list(reader)
        if not rows:
            continue
        if writer is None:
            writer = csv.writer(out, delimiter=";", quotechar='"')
            writer.writerow(rows[0])
            start = 1
        else:
            start = 1
        for row in rows[start:]:
            writer.writerow(row)

df = pd.read_csv(out_csv, sep=";")
df.to_parquet(out_parquet, index=False)

print(out_csv)
print(out_parquet)

# Upload parquet to GCS if GCS_PARQUET_PREFIX is set (folder you choose)
import os
from config import GCS_PARQUET_PREFIX
if os.environ.get("GCS_BUCKET") and GCS_PARQUET_PREFIX:
    import storage
    bucket = storage.get_bucket()
    key = f"{GCS_PARQUET_PREFIX.rstrip('/')}/thesis_meta_combined.parquet"
    storage.upload_file(bucket, out_parquet, key, content_type="application/octet-stream")
    print(f"Uploaded to gs://{os.environ['GCS_BUCKET']}/{key}")
