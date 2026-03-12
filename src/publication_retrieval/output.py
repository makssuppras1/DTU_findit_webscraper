"""Build retrieval output schema and write JSON + raw payloads."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from publication_retrieval.models import (
    CanonicalPublication,
    ProvenanceEntry,
    RetrievalMeta,
    RetrievalResult,
    UnmatchedItem,
)


def _professor_identity_dict(candidate: dict) -> dict:
    return {k: v for k, v in candidate.items() if v is not None}


def _publication_to_dict(p: CanonicalPublication) -> dict:
    return {
        "ids": p.ids,
        "type": p.type,
        "title": p.title,
        "year": p.year,
        "venue": p.venue,
        "journal_name": p.journal_name,
        "volume": p.volume,
        "issue": p.issue,
        "pages": p.pages,
        "authors": [
            {"name": a.name, "orcid": a.orcid, "affiliation": a.affiliation}
            for a in p.authors
        ],
        "provenance": [
            {
                "source": e.source,
                "source_id": e.source_id,
                "retrieved_at_iso": e.retrieved_at_iso,
                "raw_payload_ref": e.raw_payload_ref,
                "extraction_method": e.extraction_method,
                "confidence": e.confidence,
            }
            for e in p.provenance
        ],
        "deduplication": {
            "status": p.deduplication.status,
            "merged_from": p.deduplication.merged_from,
        },
    }


def build_retrieval_result(
    professor_identity: dict,
    candidate_profiles: list[dict],
    publications: list[CanonicalPublication],
    unmatched: list[UnmatchedItem],
    retrieval_meta: RetrievalMeta,
) -> RetrievalResult:
    return RetrievalResult(
        professor_identity=professor_identity,
        candidate_source_profiles=candidate_profiles,
        publications=publications,
        unmatched_or_uncertain=unmatched,
        retrieval_meta=retrieval_meta,
    )


def result_to_dict(result: RetrievalResult) -> dict:
    return {
        "professor_identity": result.professor_identity,
        "candidate_source_profiles": result.candidate_source_profiles,
        "publications": [_publication_to_dict(p) for p in result.publications],
        "unmatched_or_uncertain": [
            {"raw_record_ref": u.raw_record_ref, "reason": u.reason, "source": u.source}
            for u in result.unmatched_or_uncertain
        ],
        "retrieval_meta": {
            "retrieval_run_id": result.retrieval_meta.retrieval_run_id,
            "retrieved_at_iso": result.retrieval_meta.retrieved_at_iso,
            "config_scope": result.retrieval_meta.config_scope,
            "pipeline_version": result.retrieval_meta.pipeline_version,
            "failed_sources": result.retrieval_meta.failed_sources,
        },
    }


def write_output(
    results: list[RetrievalResult],
    output_dir: str,
    one_file: bool = False,
) -> list[Path]:
    """Write retrieval results to JSON. If one_file, single NDJSON; else one JSON per professor."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    if one_file:
        path = out_path / "retrieval_results.ndjson"
        with open(path, "w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(result_to_dict(r), ensure_ascii=False) + "\n")
        written.append(path)
    else:
        for r in results:
            pid = r.professor_identity.get("input_id", "unknown").replace("/", "_")
            path = out_path / f"retrieval_{pid}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(result_to_dict(r), f, indent=2, ensure_ascii=False)
            written.append(path)
    return written


def write_raw_payloads(
    run_id: str,
    professor_idx: int,
    source: str,
    payloads: list | dict,
    raw_dir: Path,
    gcs_bucket=None,
) -> None:
    """Write raw API payloads to local dir (and optionally GCS). payloads: list of work objects or single response."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    if isinstance(payloads, list):
        for i, p in enumerate(payloads):
            path = raw_dir / f"{source}_{professor_idx}_{i + 1}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(p, f, ensure_ascii=False)
    else:
        path = raw_dir / f"{source}_{professor_idx}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payloads, f, ensure_ascii=False)
    if gcs_bucket:
        try:
            blob = gcs_bucket.blob(f"raw/{run_id}/{path.name}")
            if path.exists():
                blob.upload_from_filename(str(path), content_type="application/json")
        except Exception:
            pass
