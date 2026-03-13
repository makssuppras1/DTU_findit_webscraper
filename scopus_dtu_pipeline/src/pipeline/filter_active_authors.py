"""Step 4: Filter candidates to active researchers (qualifying pub in last N years)."""
from __future__ import annotations

import datetime
import json
import logging
from pathlib import Path

from src.api.scopus_client import ScopusClient
from src.api.search_api import get_entries_with_doctype
from src.config import get_paths, get_pipeline_config
from src.models.researcher import ResearcherCandidate
from src.utils.field_utils import get_in, safe_int
from src.utils.logging_utils import log_progress
from src.utils.output_utils import write_json

logger = logging.getLogger("scopus_pipeline")


def run(
    client: ScopusClient,
    candidates: list[dict],
    profiles: list[dict],
    out_dir: Path | None = None,
    checkpoint_path: Path | None = None,
    progress_interval: int = 50,
) -> list[ResearcherCandidate]:
    """For each candidate, check last 5y qualifying publications; build ResearcherCandidate list."""
    paths = get_paths()
    raw_dir = Path(out_dir or paths["raw"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = checkpoint_path or raw_dir / "checkpoint_active_researchers.json"
    cfg = get_pipeline_config()
    years = cfg.get("active_years", 5)
    qualifying = cfg.get("qualifying_doctypes", ["article", "conference paper", "review", "book chapter"])
    excluded = cfg.get("excluded_doctypes", ["editorial", "note", "erratum", "letter"])

    profile_by_id = {str(p.get("scopus_author_id")): p for p in profiles if p.get("scopus_author_id")}
    cand_by_id = {str(c.get("scopus_author_id")): c for c in candidates if c.get("scopus_author_id")}

    if checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            data = json.load(f)
            rows = data.get("researchers", [])
            return [_row_to_candidate(r) for r in rows if r.get("active_last_5y")]

    researchers: list[ResearcherCandidate] = []
    current_year = datetime.date.today().year
    for i, cand in enumerate(candidates):
        aid = cand.get("scopus_author_id")
        if not aid:
            continue
        log_progress(logger, i + 1, len(candidates), "Active filter")
        try:
            entries = get_entries_with_doctype(client, str(aid), years, qualifying, excluded)
            qualifying_count = len(entries)
            last_year: int | None = None
            for e in entries:
                y = get_in(e, "prism:coverDate")
                if y and len(str(y)) >= 4:
                    try:
                        yint = int(str(y)[:4])
                        if last_year is None or yint > last_year:
                            last_year = yint
                    except ValueError:
                        pass
            active = qualifying_count > 0
            prof = profile_by_id.get(str(aid)) or {}
            researchers.append(ResearcherCandidate(
                scopus_author_id=str(aid),
                full_name=cand.get("full_name") or prof.get("preferred_name") or "",
                indexed_name=cand.get("indexed_name") or "",
                orcid=cand.get("orcid") or prof.get("orcid"),
                current_affiliation=cand.get("affiliation_current") or prof.get("current_affiliation"),
                affiliation_history=prof.get("affiliation_history"),
                document_count=safe_int(cand.get("document_count") or prof.get("document_count")),
                subject_areas=cand.get("subject_areas") or prof.get("subject_areas"),
                active_last_5y=active,
                qualifying_publication_count_last_5y=qualifying_count,
                last_publication_year=last_year,
                raw_source_refs={"candidate": cand, "profile": prof},
            ))
        except Exception as e:
            logger.warning("Filter failed for author %s: %s", aid, e)

    active_list = [r for r in researchers if r.active_last_5y]
    result = {
        "researchers": [_candidate_to_row(r) for r in researchers],
        "active_count": len(active_list),
        "total_count": len(researchers),
    }
    write_json(checkpoint, result)
    return active_list


def _candidate_to_row(r: ResearcherCandidate) -> dict:
    return {
        "scopus_author_id": r.scopus_author_id,
        "full_name": r.full_name,
        "indexed_name": r.indexed_name,
        "orcid": r.orcid,
        "current_affiliation": r.current_affiliation,
        "document_count": r.document_count,
        "active_last_5y": r.active_last_5y,
        "qualifying_publication_count_last_5y": r.qualifying_publication_count_last_5y,
        "last_publication_year": r.last_publication_year,
    }


def _row_to_candidate(r: dict) -> ResearcherCandidate:
    return ResearcherCandidate(
        scopus_author_id=r.get("scopus_author_id", ""),
        full_name=r.get("full_name", ""),
        indexed_name=r.get("indexed_name", ""),
        orcid=r.get("orcid"),
        current_affiliation=r.get("current_affiliation"),
        affiliation_history=r.get("affiliation_history"),
        document_count=safe_int(r.get("document_count")),
        subject_areas=r.get("subject_areas"),
        active_last_5y=bool(r.get("active_last_5y")),
        qualifying_publication_count_last_5y=safe_int(r.get("qualifying_publication_count_last_5y")),
        last_publication_year=r.get("last_publication_year"),
        raw_source_refs=r.get("raw_source_refs", {}),
    )
