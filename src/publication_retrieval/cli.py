"""CLI for publication retrieval pipeline."""

import argparse
import logging
import os

from publication_retrieval.config import DEFAULT_PUBLICATION_TYPES, RetrievalConfig
from publication_retrieval.pipeline import run_retrieval_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Retrieve publication records for professors from GCS persons CSV.")
    parser.add_argument(
        "--persons-csv",
        default=os.environ.get("RETRIEVAL_PERSONS_CSV", "dtu_findit/master_thesis_meta/Cleaned/dtu_orbit_persons_cleaned.csv"),
        help="GCS path to persons CSV (default: env RETRIEVAL_PERSONS_CSV or plan default)",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("GCS_BUCKET"),
        help="GCS bucket (default: env GCS_BUCKET)",
    )
    parser.add_argument(
        "--output-dir",
        default="retrieval_output",
        help="Output directory for JSON results",
    )
    parser.add_argument(
        "--raw-dir",
        default=None,
        help="Directory for raw API payloads (default: output_dir/raw/run_id)",
    )
    parser.add_argument(
        "--accept-threshold",
        choices=("high", "medium", "low"),
        default="medium",
        help="Minimum confidence to accept a publication",
    )
    parser.add_argument(
        "--no-strict",
        action="store_true",
        help="Disable strict disambiguation (allow low-confidence accepts)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of persons to process (for testing)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s: %(message)s")

    config = RetrievalConfig(
        persons_csv_gcs_path=args.persons_csv,
        gcs_bucket=args.bucket,
        output_dir=args.output_dir,
        raw_payloads_dir=args.raw_dir,
        accept_threshold=args.accept_threshold,
        strict_disambiguation=not args.no_strict,
        limit=args.limit,
    )
    run_retrieval_pipeline(config)
