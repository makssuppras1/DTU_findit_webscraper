"""CLI: process PDFs from GCS, write one JSON per PDF with resume and optional parallelism."""

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Ensure src is on path when run as __main__ from project root
_SCRIPT_DIR = Path(__file__).resolve().parent
_SRC = _SCRIPT_DIR.parent
if _SRC.name == "src" and _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from storage.gcs_io import (
    BlobMeta,
    gcs_exists,
    list_pdfs,
    open_pdf_bytes,
    upload_json,
)
from pdf_section_extractor.pipeline import doc_id_from_blob_name, run_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _output_is_gcs(output: str) -> bool:
    return output.startswith("gs://")


def _parse_gcs_output(output: str) -> tuple[str, str]:
    """Return (bucket, prefix) for gs://bucket/prefix/."""
    assert output.startswith("gs://")
    rest = output[5:].strip("/")
    if "/" not in rest:
        return rest, ""
    bucket, _, prefix = rest.partition("/")
    return bucket, (prefix.strip("/") + "/" if prefix else "")


def _output_json_exists(output: str, doc_id: str) -> bool:
    if _output_is_gcs(output):
        bucket, prefix = _parse_gcs_output(output)
        blob_name = f"{prefix}{doc_id}.json"
        return gcs_exists(bucket, blob_name)
    else:
        path = Path(output) / f"{doc_id}.json"
        return path.exists()


def _write_output(output: str, doc_id: str, data: dict) -> None:
    if _output_is_gcs(output):
        bucket, prefix = _parse_gcs_output(output)
        blob_name = f"{prefix}{doc_id}.json"
        upload_json(bucket, blob_name, data)
    else:
        Path(output).mkdir(parents=True, exist_ok=True)
        path = Path(output) / f"{doc_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def _process_one(blob: BlobMeta, output: str, resume: bool, debug_write_pages: bool) -> tuple[str, bool, str | None]:
    """Process one PDF. Return (blob_name, skipped, error_msg)."""
    doc_id = doc_id_from_blob_name(blob.name)
    if resume and _output_json_exists(output, doc_id):
        return blob.name, True, None
    try:
        pdf_bytes = open_pdf_bytes(blob.bucket, blob.name)
        data = run_pipeline(pdf_bytes, blob.name, debug_write_pages=debug_write_pages)
        _write_output(output, doc_id, data)
        return blob.name, False, None
    except Exception as e:
        log.exception("Failed %s: %s", blob.name, e)
        return blob.name, False, str(e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract thesis sections from GCS PDFs.")
    parser.add_argument("--bucket", default="thesis_archive_bucket", help="GCS bucket name")
    parser.add_argument(
        "--prefix",
        default="dtu_findit/master_thesis/",
        help="Blob prefix (e.g. dtu_findit/master_thesis/)",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output: local directory path or gs://bucket/prefix/",
    )
    parser.add_argument("--workers", "-j", type=int, default=1, help="Parallel workers")
    parser.add_argument("--limit", "-n", type=int, default=0, help="Max PDFs to process (0 = all)")
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume (reprocess all)",
    )
    parser.add_argument(
        "--debug-write-pages",
        action="store_true",
        help="Include per-page metadata in JSON output for debugging",
    )
    args = parser.parse_args()
    resume = not args.no_resume

    blobs = list(list_pdfs(args.bucket, args.prefix))
    if args.limit:
        blobs = blobs[: args.limit]
    total = len(blobs)
    log.info("Processing %d PDFs from gs://%s/%s", total, args.bucket, args.prefix)

    ok, skip, fail = 0, 0, 0
    if args.workers <= 1:
        for i, blob in enumerate(blobs):
            _, skipped, err = _process_one(blob, args.output, resume, args.debug_write_pages)
            if err:
                fail += 1
            elif skipped:
                skip += 1
            else:
                ok += 1
            if (i + 1) % 10 == 0 or i + 1 == total:
                log.info("Progress %d/%d ok=%d skip=%d fail=%d", i + 1, total, ok, skip, fail)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(_process_one, blob, args.output, resume, args.debug_write_pages): blob
                for blob in blobs
            }
            for i, future in enumerate(as_completed(futures)):
                _, skipped, err = future.result()
                if err:
                    fail += 1
                elif skipped:
                    skip += 1
                else:
                    ok += 1
                if (i + 1) % 10 == 0 or i + 1 == total:
                    log.info("Progress %d/%d ok=%d skip=%d fail=%d", i + 1, total, ok, skip, fail)

    log.info("Done. ok=%d skip=%d fail=%d", ok, skip, fail)


if __name__ == "__main__":
    main()
