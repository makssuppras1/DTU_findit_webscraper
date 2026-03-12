"""
Findit thesis metadata pipeline: one script to reproduce thesis_meta_combined from inputs.

Purpose:
  Re-implements the metadata collection + merging that produces thesis_meta_combined.csv/.parquet.
  Replaces the need to run merge_thesis_meta.py plus any notebook steps for this dataset.

Inputs:
  - A directory of Findit export CSVs (e.g. Thesis_meta/ with df61..._1.csv, _2.csv, ...), or
  - A single concatenated CSV (e.g. findit_metadata.csv).
  Optionally: FULLTEXT_TOKEN + METADATA_CSV_PAGES to fetch from Findit CSV API (not used for
  merge path; only if you add a fetch stage).

Outputs:
  - Combined CSV (default: Thesis_meta/all.csv), semicolon-delimited, 40 columns (unenriched).
  - Optional Parquet (same schema).

Maps to previous pipeline:
  - merge_thesis_meta.py: discover CSVs in folder, sort by (trailing_number, name), concat, write.
  - Notebook (thesis part): loads combined CSV; filtering by bucket/abstract/year is downstream.
  - Enrichment (abstracts) is separate: enrich_thesis_meta_abstracts.py. If your existing
    thesis_meta_combined.csv has abstracts filled, it was produced by merge + enrich; this
    script reproduces only the merge. Run enrich_thesis_meta_abstracts.py after for abstracts.

Usage:
  # Unenriched merge output → Thesis_meta/all.csv (default)
  python pipeline_findit_thesis_meta.py --input-dir Thesis_meta

  # Custom output path
  python pipeline_findit_thesis_meta.py --output Thesis_meta/all.csv --input-dir Thesis_meta

  # With optional cleaning and parquet
  python pipeline_findit_thesis_meta.py --input-dir Thesis_meta --write-parquet --clean

  # Limit rows (debug)
  python pipeline_findit_thesis_meta.py --output out.csv --input-dir Thesis_meta --limit 100

Validation:
  Output has same 40 columns and row count as merge_thesis_meta on the same input dir.
  If your thesis_meta_combined.csv was later enriched with abstracts (enrich_thesis_meta_abstracts.py),
  this script reproduces only the merge; run the enrich script afterward to match that artifact.
"""
from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

log = logging.getLogger(__name__)

# Optional pandas for parquet
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# -----------------------------------------------------------------------------
# Config (env / CLI)
# -----------------------------------------------------------------------------

DEFAULT_INPUT_DIR = "Thesis_meta"
DEFAULT_OUTPUT_CSV = "Thesis_meta/all.csv"  # unenriched merge output
CSV_DELIM = ";"
CSV_QUOTE = '"'
def _file_sort_key(p: Path) -> tuple[str, str]:
    """Sort key: (trailing_number.zfill(3), name) to match merge_thesis_meta."""
    stem = p.stem
    parts = stem.split("_")
    last = parts[-1] if parts else ""
    num = last.zfill(3) if last.isdigit() else last
    return (num, p.name)


def discover_input_csvs(input_dir: Path, output_path: Path | None) -> list[Path]:
    """Find CSV files in input_dir, sorted. Exclude output path and thesis_meta_combined*.csv (combined artifacts)."""
    all_csv = list(input_dir.glob("*.csv"))
    exclude = set()
    if output_path:
        try:
            exclude.add(output_path.resolve())
        except Exception:
            pass
    paths = []
    for f in all_csv:
        if f.resolve() in exclude:
            continue
        if f.name.startswith("thesis_meta_combined"):
            continue
        paths.append(f)
    paths.sort(key=_file_sort_key)
    return paths


def read_csv_rows(path: Path, delimiter: str = CSV_DELIM, quotechar: str = CSV_QUOTE) -> list[list[str]]:
    """Read CSV into list of rows (list of cell strings)."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
        return list(reader)


def concatenate_csvs(
    paths: list[Path],
    delimiter: str = CSV_DELIM,
    quotechar: str = CSV_QUOTE,
    limit: int | None = None,
) -> tuple[list[str], list[list[str]]]:
    """Read all CSVs in order; return (header, data_rows). Header from first file only."""
    header: list[str] = []
    rows: list[list[str]] = []
    for path in paths:
        raw = read_csv_rows(path, delimiter=delimiter, quotechar=quotechar)
        if not raw:
            continue
        if not header:
            header = raw[0]
        for row in raw[1:]:
            rows.append(row)
            if limit is not None and len(rows) >= limit:
                return (header, rows)
    return (header, rows)


def clean_rows(
    header: list[str],
    rows: list[list[str]],
    drop_empty_rows: bool = True,
    dedup_by_column: str | None = "ID",
    strip_strings: bool = True,
) -> list[list[str]]:
    """Strip cells, drop all-empty rows, optionally deduplicate by key column (keep first)."""
    if not header:
        return rows
    key_idx = header.index(dedup_by_column) if dedup_by_column and dedup_by_column in header else None

    out: list[list[str]] = []
    seen_keys: set[str] = set()
    for row in rows:
        if len(row) != len(header):
            # Pad or truncate to header length so we don't break columns
            row = list(row) + [""] * (len(header) - len(row)) if len(row) < len(header) else row[: len(header)]
        if strip_strings:
            row = [str(c).strip() for c in row]
        if drop_empty_rows and not any(c for c in row):
            continue
        if key_idx is not None and key_idx < len(row) and dedup_by_column:
            key = row[key_idx].strip()
            if key and key in seen_keys:
                continue
            if key:
                seen_keys.add(key)
        out.append(row)
    return out


def sort_rows_for_determinism(header: list[str], rows: list[list[str]], key_column: str = "ID") -> list[list[str]]:
    """Sort rows by key_column then by row index to get stable order."""
    if not rows or not header or key_column not in header:
        return rows
    idx = header.index(key_column)
    # Secondary key: original index (so order is stable when ID ties)
    indexed = [(i, row) for i, row in enumerate(rows)]
    indexed.sort(key=lambda x: (x[1][idx] if idx < len(x[1]) else "", x[0]))
    return [row for _, row in indexed]


def write_csv(path: Path, header: list[str], rows: list[list[str]], delimiter: str = CSV_DELIM, quotechar: str = CSV_QUOTE) -> None:
    """Write header + rows to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=delimiter, quotechar=quotechar)
        w.writerow(header)
        w.writerows(rows)


def write_parquet(path: Path, csv_path: Path) -> None:
    """Write Parquet from the CSV we just wrote (same schema as merge_thesis_meta)."""
    if not HAS_PANDAS:
        log.warning("pandas not available; skipping parquet")
        return
    df = pd.read_csv(csv_path, sep=CSV_DELIM, encoding="utf-8", low_memory=False)
    df.to_parquet(path, index=False)
    log.info("Wrote parquet rows=%d path=%s", len(df), path)


def run(
    output: Path,
    input_dir: Path | None = None,
    input_file: Path | None = None,
    cache_dir: Path | None = None,
    limit: int | None = None,
    write_parquet_flag: bool = False,
    clean: bool = False,
    sort_output: bool = True,
) -> None:
    # Stage 1: Load config / discover inputs
    if input_file and input_file.exists():
        paths = [input_file]
        log.info("Single input file: %s", input_file)
    elif input_dir and input_dir.is_dir():
        output_abs = output.resolve() if output else None
        paths = discover_input_csvs(input_dir, output_abs)
        log.info("Discovered %d CSV files in %s", len(paths), input_dir)
    else:
        raise FileNotFoundError("Provide --input-dir or --input pointing to existing path")

    if not paths:
        raise ValueError("No input CSV files found")

    # Stage 2: Fetch/read raw (we only read CSVs here)
    header, rows = concatenate_csvs(paths, limit=limit)
    log.info("Concatenated rows=%d columns=%d", len(rows), len(header))

    # Stage 3: Normalize/clean (optional)
    if clean:
        rows = clean_rows(header, rows, drop_empty_rows=True, dedup_by_column="ID", strip_strings=True)
        log.info("After clean: rows=%d", len(rows))

    # Stage 4: Merge is already done; sort for determinism
    if sort_output:
        rows = sort_rows_for_determinism(header, rows, key_column="ID")

    # Stage 5: Validate and write
    if len(header) != 40:
        log.warning("Header has %d columns; expected 40 for thesis_meta_combined schema", len(header))
    write_csv(output, header, rows)
    log.info("Wrote CSV rows=%d path=%s", len(rows), output)

    if write_parquet_flag:
        parquet_path = output.with_suffix(".parquet")
        write_parquet(parquet_path, output)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Findit thesis metadata pipeline: build thesis_meta_combined from input CSVs.",
    )
    parser.add_argument("--output", "-o", type=Path, default=Path(DEFAULT_OUTPUT_CSV), help="Output CSV path")
    parser.add_argument("--input-dir", "-i", type=Path, default=None, help="Directory containing chunk CSVs (e.g. Thesis_meta)")
    parser.add_argument("--input", type=Path, default=None, help="Single input CSV (alternative to --input-dir)")
    parser.add_argument("--cache-dir", type=Path, default=None, help="Optional cache directory (unused in current pipeline)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of data rows (for debugging)")
    parser.add_argument("--write-parquet", action="store_true", help="Also write .parquet next to output CSV")
    parser.add_argument("--clean", action="store_true", help="Strip strings, drop empty rows, deduplicate by ID")
    parser.add_argument("--sort", action="store_true", help="Sort output by ID for determinism (default: keep concat order to match merge_thesis_meta)")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"), help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    run(
        output=args.output,
        input_dir=args.input_dir,
        input_file=args.input,
        cache_dir=args.cache_dir,
        limit=args.limit,
        write_parquet_flag=args.write_parquet,
        clean=args.clean,
        sort_output=args.sort,
    )


if __name__ == "__main__":
    main()
