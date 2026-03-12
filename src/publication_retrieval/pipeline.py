"""Main retrieval pipeline: load -> discover -> harvest -> disambiguate -> deduplicate -> output."""

import logging
from datetime import UTC, datetime
from pathlib import Path

from publication_retrieval.config import RetrievalConfig
from publication_retrieval.deduplicate import deduplicate
from publication_retrieval.disambiguate import accept_decision, disambiguate
from publication_retrieval.discover import discover_profiles
from publication_retrieval.harvest import harvest_source
from publication_retrieval.load import load_persons_from_gcs
from publication_retrieval.models import (
    RawPublicationRecord,
    RetrievalMeta,
    RetrievalResult,
    UnmatchedItem,
)
from publication_retrieval.output import (
    build_retrieval_result,
    write_output,
    write_raw_payloads,
)

log = logging.getLogger(__name__)


def _identity_dict(candidate) -> dict:
    return {
        "input_id": candidate.input_id,
        "canonical_name": candidate.canonical_name,
        "orcid": candidate.orcid,
        "email": candidate.email,
        "department": candidate.department,
        "institution": candidate.institution,
        "profile_url": candidate.profile_url,
        "orbit_id": candidate.orbit_id,
    }


def run_retrieval_pipeline(config: RetrievalConfig | None = None) -> list[RetrievalResult]:
    config = config or RetrievalConfig()
    run_id = datetime.now(UTC).strftime("run_%Y%m%d_%H%M%S")
    now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    bucket_name = config.gcs_bucket

    persons = load_persons_from_gcs(config.persons_csv_gcs_path, bucket_name)
    if config.limit:
        persons = persons[: config.limit]
    log.info("Loaded %s persons from GCS", len(persons))

    output_dir = Path(config.output_dir)
    raw_dir = Path(config.raw_payloads_dir or str(output_dir / "raw" / run_id))
    raw_dir.mkdir(parents=True, exist_ok=True)

    gcs_bucket = None
    if bucket_name:
        try:
            from publication_retrieval.load import get_bucket
            gcs_bucket = get_bucket(bucket_name)
        except Exception as e:
            log.warning("GCS bucket not available for raw payloads: %s", e)

    results: list[RetrievalResult] = []
    for prof_idx, candidate in enumerate(persons):
        profiles = discover_profiles(candidate)
        if not profiles:
            log.debug("No profiles for %s, skipping", candidate.canonical_name)
            meta = RetrievalMeta(
                retrieval_run_id=run_id,
                retrieved_at_iso=now_iso,
                config_scope=list(config.publication_types),
                pipeline_version=config.pipeline_version,
            )
            results.append(
                build_retrieval_result(
                    _identity_dict(candidate),
                    [],
                    [],
                    [UnmatchedItem(raw_record_ref="", reason="no_source_profiles", source="")],
                    meta,
                )
            )
            continue

        raw_records: list[RawPublicationRecord] = []
        unmatched: list[UnmatchedItem] = []
        failed_sources: list[dict] = []

        for profile in sorted(profiles, key=lambda p: p.tier):
            retrieved_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            raw_ref_template = f"raw/{run_id}/{profile.source}_{prof_idx}_{{work_idx}}.json"
            recs, payloads, err = harvest_source(
                profile,
                config.publication_types,
                retrieved_at,
                raw_ref_template,
                config.per_page_openalex,
                config.max_retries,
                config.retry_backoff_base_seconds,
            )
            if err:
                failed_sources.append({"source": profile.source, "error": err, "timestamp": retrieved_at})
                log.warning("Harvest failed %s: %s", profile.source, err)
                continue

            if payloads:
                write_raw_payloads(run_id, prof_idx, profile.source, payloads, raw_dir, gcs_bucket)

            accepted_so_far = list(raw_records)
            for r in recs:
                decision, confidence = disambiguate(
                    r, candidate, accepted_so_far,
                    config.accept_threshold,
                    config.strict_disambiguation,
                )
                if decision == "reject":
                    unmatched.append(UnmatchedItem(r.raw_payload_ref or str(r.ids), "rejected", r.source))
                    continue
                if accept_decision(decision, confidence, config.accept_threshold):
                    r.authors = r.authors
                    raw_records.append(r)
                    accepted_so_far.append(r)
                else:
                    unmatched.append(
                        UnmatchedItem(r.raw_payload_ref or str(r.ids), "uncertain", r.source)
                    )

        canonical = deduplicate(raw_records)
        meta = RetrievalMeta(
            retrieval_run_id=run_id,
            retrieved_at_iso=now_iso,
            config_scope=list(config.publication_types),
            pipeline_version=config.pipeline_version,
            failed_sources=failed_sources,
        )
        results.append(
            build_retrieval_result(
                _identity_dict(candidate),
                [
                    {
                        "source": p.source,
                        "source_id": p.source_id,
                        "tier": p.tier,
                        "discovered_at_iso": p.discovered_at_iso,
                        "url": p.url,
                    }
                    for p in profiles
                ],
                canonical,
                unmatched,
                meta,
            )
        )

    write_output(results, config.output_dir, one_file=False)
    log.info("Wrote %s result(s) to %s", len(results), config.output_dir)
    return results
