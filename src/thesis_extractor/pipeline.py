"""Batch orchestration: list GCS PDFs, extract, section, write. Public API: run_pipeline(config)."""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .config import Config
from . import gcs_io
from .extract import extract_pages
from .sectioning import preclean, detect_hits, assemble_sections
from .utils import doc_id, setup_logging

log = logging.getLogger(__name__)


def _list_pdf_blobs(bucket: str, prefix: str) -> list[tuple[str, str]]:
    blobs = list(gcs_io.list_pdfs(bucket, prefix))
    return [(b.bucket, b.name) for b in blobs]


def _output_exists(cfg: Config, doc_id_val: str) -> bool:
    if cfg.output.target == "gcs":
        key = f"{cfg.output.gcs_prefix.rstrip('/')}/{doc_id_val}.json"
        return gcs_io.gcs_exists(cfg.output.gcs_bucket, key)
    return (Path(cfg.output.local_dir) / f"{doc_id_val}.json").exists()


def _write_output(cfg: Config, doc_id_val: str, data: dict) -> None:
    if cfg.output.target == "gcs":
        key = f"{cfg.output.gcs_prefix.rstrip('/')}/{doc_id_val}.json"
        gcs_io.upload_json(cfg.output.gcs_bucket, key, data)
    else:
        Path(cfg.output.local_dir).mkdir(parents=True, exist_ok=True)
        (Path(cfg.output.local_dir) / f"{doc_id_val}.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )


def _process_one(cfg: Config, bucket: str, blob_name: str) -> tuple[bool, bool, float]:
    doc_id_val = doc_id(blob_name)
    if cfg.runtime.resume and _output_exists(cfg, doc_id_val):
        return True, True, 0.0
    t0 = time.perf_counter()
    try:
        pdf_bytes = gcs_io.open_pdf_bytes(bucket, blob_name)
        pages_data = extract_pages(cfg, pdf_bytes)
        page_texts = [t for _, t, _ in pages_data]
        ocr_count = sum(1 for _, _, p in pages_data if p == "ocr")
        cleaned = preclean(page_texts)
        hits = detect_hits(cfg, cleaned)
        sections, unknown = assemble_sections(cleaned, hits)
        runtime = time.perf_counter() - t0
        data = {
            "doc_id": doc_id_val,
            "source": {"bucket": bucket, "blob": blob_name},
            "page_count": len(page_texts),
            "stats": {"ocr_pages": ocr_count, "runtime_sec": round(runtime, 2)},
            "sections": sections,
            "unknown_sections": unknown,
        }
        _write_output(cfg, doc_id_val, data)
        return True, False, runtime
    except Exception as e:
        log.exception("Failed %s: %s", blob_name, e)
        return False, False, time.perf_counter() - t0


def run_pipeline(config: Config) -> dict:
    """
    Run full extraction pipeline from config. Returns summary dict:
    {processed, skipped, failed, total, elapsed_sec}.
    """
    cfg = config
    gcs = cfg.gcs
    run = cfg.runtime

    if run.log_file:
        setup_logging(run.log_file)

    blobs = _list_pdf_blobs(gcs.bucket, gcs.prefix)
    if run.limit and run.limit > 0:
        blobs = blobs[: run.limit]
    total = len(blobs)

    log.info("Processing %d PDFs from gs://%s/%s", total, gcs.bucket, gcs.prefix)
    t0 = time.perf_counter()
    ok, skip, fail = 0, 0, 0

    if run.workers <= 1:
        for bucket, name in blobs:
            success, skipped, _ = _process_one(cfg, bucket, name)
            if skipped:
                skip += 1
            elif success:
                ok += 1
            else:
                fail += 1
    else:
        with ThreadPoolExecutor(max_workers=run.workers) as ex:
            futures = {ex.submit(_process_one, cfg, b, n): (b, n) for b, n in blobs}
            for fut in as_completed(futures):
                success, skipped, _ = fut.result()
                if skipped:
                    skip += 1
                elif success:
                    ok += 1
                else:
                    fail += 1

    elapsed = time.perf_counter() - t0
    summary = {"processed": ok, "skipped": skip, "failed": fail, "total": total, "elapsed_sec": round(elapsed, 2)}
    log.info("Done. ok=%d skip=%d fail=%d (%.1fs)", ok, skip, fail, elapsed)
    return summary
