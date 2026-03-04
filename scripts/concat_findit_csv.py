#!/usr/bin/env python3
"""Concatenate findit_downloads/page_*.csv into one CSV. Keeps header from first file only."""
import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser(description="Concat findit page CSVs into one file")
    p.add_argument("--dir", default="findit_downloads", help="Folder with page_*.csv files")
    p.add_argument("--out", default="findit_metadata.csv", help="Output CSV path")
    p.add_argument("--sep", default=";", help="CSV separator")
    args = p.parse_args()

    folder = Path(args.dir)
    if not folder.is_dir():
        raise SystemExit(f"Not a directory: {folder}")

    paths = sorted(folder.glob("page_*.csv"), key=lambda p: int(p.stem.split("_")[1]))
    if not paths:
        raise SystemExit(f"No page_*.csv files in {folder}")

    chunks = []
    for path in paths:
        try:
            df = pd.read_csv(path, sep=args.sep, encoding="utf-8", on_bad_lines="skip")
            if df.empty:
                continue
            chunks.append(df)
        except Exception:
            continue

    if not chunks:
        raise SystemExit("No valid data in any file")

    out = pd.concat(chunks, ignore_index=True)
    out.to_csv(args.out, index=False, sep=args.sep, encoding="utf-8")
    print(f"Wrote {len(out)} rows to {args.out}")


if __name__ == "__main__":
    main()
