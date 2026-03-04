#!/usr/bin/env python3
"""Clean concatenated findit CSV: strip strings, drop duplicates, drop empty-ID rows."""
import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser(description="Clean findit combined CSV")
    p.add_argument("input", nargs="?", default="findit_metadata.csv", help="Input CSV")
    p.add_argument("--out", default=None, help="Output CSV (default: input with _cleaned suffix)")
    p.add_argument("--sep", default=";", help="Field separator")
    p.add_argument("--drop-dup-col", default="ID", help="Column to drop duplicates by (empty to drop full-row dups only)")
    p.add_argument("--drop-empty-key", action="store_true", help="Drop rows where key column is null/empty")
    args = p.parse_args()

    inp = Path(args.input)
    if not inp.exists():
        raise SystemExit(f"File not found: {inp}")

    out = Path(args.out) if args.out else (inp.parent / f"{inp.stem}_cleaned{inp.suffix}")

    df = pd.read_csv(inp, sep=args.sep, encoding="utf-8", on_bad_lines="skip")
    if df.empty:
        raise SystemExit("DataFrame is empty")

    # Strip whitespace from string columns
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip()
    # Restore empty strings as NaN for key col if we drop empty
    if args.drop_empty_key and args.drop_dup_col and args.drop_dup_col in df.columns:
        df[args.drop_dup_col] = df[args.drop_dup_col].replace("", pd.NA).replace("nan", pd.NA)
        df = df.dropna(subset=[args.drop_dup_col])

    # Drop duplicates
    if args.drop_dup_col and args.drop_dup_col in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=[args.drop_dup_col], keep="first")
        if len(df) < before:
            print(f"Dropped {before - len(df)} duplicate rows (by {args.drop_dup_col})")
    else:
        df = df.drop_duplicates(keep="first")

    df.to_csv(out, index=False, sep=args.sep, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {out}")


if __name__ == "__main__":
    main()
