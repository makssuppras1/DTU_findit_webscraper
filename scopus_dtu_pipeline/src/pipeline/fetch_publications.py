"""Step 5: For each active author, retrieve publications and enrich with Abstract API."""
from __future__ import annotations

import json
import logging
from pathlib import Path

from src.api.abstract_api import enrich_from_abstract_response, get_abstract_by_eid
from src.api.scopus_client import ScopusClient
from src.api.search_api import paginated_scopus_search
from src.config import get_paths
from src.models.publication import PublicationRecord
from src.models.researcher import ResearcherCandidate
from src.utils.field_utils import get_in, safe_int
from src.utils.logging_utils import log_progress
from src.utils.output_utils import write_json

logger = logging.getLogger("scopus_pipeline")


def run(
    client: ScopusClient,
    active_researchers: list[ResearcherCandidate],
    out_dir: Path | None = None,
    checkpoint_path: Path | None = None,
    progress_interval: int = 50,
    enrich_with_abstract: bool = False,
) -> list[PublicationRecord]:
    """Fetch all publications for each active author; optionally enrich with abstract. Saves raw and checkpoint."""
    paths = get_paths()
    raw_dir = Path(out_dir or paths["raw"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    pubs_dir = raw_dir / "publications"
    pubs_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = checkpoint_path or raw_dir / "checkpoint_publications.json"

    all_publications: list[PublicationRecord] = []
    by_author: dict[str, list[dict]] = {}
    if checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            data = json.load(f)
            by_author = data.get("by_author", {})
            for aid, rows in by_author.items():
                for r in rows:
                    all_publications.append(_row_to_publication(r))
            if all_publications:
                logger.info("Resuming from checkpoint: %s authors, %s publications already fetched", len(by_author), len(all_publications))
                return all_publications

    logger.info("Fetching publications for %s active researchers (enrich_with_abstract=%s)", len(active_researchers), enrich_with_abstract)
    for i, researcher in enumerate(active_researchers):
        aid = researcher.scopus_author_id
        name = researcher.full_name or researcher.indexed_name or aid
        log_progress(logger, i + 1, len(active_researchers), "Publications")
        logger.debug("Author %s/%s: %s (id=%s)", i + 1, len(active_researchers), name, aid)
        try:
            author_pubs = _fetch_author_publications(client, aid, enrich_with_abstract)
            by_author[aid] = [p.to_row() for p in author_pubs]
            all_publications.extend(author_pubs)
            logger.info("  %s: %s publications (total so far: %s)", name[:40], len(author_pubs), len(all_publications))
            write_json(pubs_dir / f"author_{aid}_publications.json", {"author_id": aid, "publications": [p.to_row() for p in author_pubs]})
        except Exception as e:
            logger.warning("Publications failed for author %s (%s): %s", aid, name[:30], e)

    logger.info("Publications step done: %s authors, %s publications total", len(by_author), len(all_publications))
    result = {"by_author": by_author, "total_count": len(all_publications)}
    write_json(checkpoint, result)
    return all_publications


def _fetch_author_publications(client: ScopusClient, author_id: str, enrich: bool) -> list[PublicationRecord]:
    query = f"AU-ID({author_id})"
    records = []
    for entry in paginated_scopus_search(client, query):
        eid = get_in(entry, "eid") or get_in(entry, "dc:identifier") or ""
        if not eid:
            continue
        title = get_in(entry, "dc:title") or ""
        doi = get_in(entry, "prism:doi") or None
        year = get_in(entry, "prism:coverDate")
        if year and len(str(year)) >= 4:
            try:
                year = int(str(year)[:4])
            except (TypeError, ValueError):
                year = None
        else:
            year = None
        cover_date = get_in(entry, "prism:coverDate") or None
        subtype = get_in(entry, "subtypeDescription") or get_in(entry, "dc:description") or None
        venue = get_in(entry, "prism:publicationName") or None
        citedby = safe_int(get_in(entry, "citedby-count"))
        authors = get_in(entry, "dc:creator") or ""
        author_names = [a.strip() for a in str(authors).split(";") if a.strip()]
        author_ids = get_in(entry, "author-id") or []
        if isinstance(author_ids, str):
            author_ids = [author_ids]
        affil = get_in(entry, "affiliation") or []
        if isinstance(affil, dict):
            affil = [affil]
        affiliations = []
        for a in (affil or []):
            name = get_in(a, "affilname") or get_in(a, "afname") or str(a)
            if name:
                affiliations.append(str(name))
        source = get_in(entry, "prism:publicationName") or None
        abstract = None
        keywords = None
        if enrich:
            abs_resp = get_abstract_by_eid(client, str(eid))
            extra = enrich_from_abstract_response(abs_resp)
            abstract = extra.get("abstract")
            keywords = extra.get("keywords")
        records.append(PublicationRecord(
            scopus_eid=str(eid),
            doi=doi,
            title=title,
            year=year,
            cover_date=str(cover_date) if cover_date else None,
            document_type=subtype,
            venue=venue,
            citation_count=citedby,
            abstract=abstract,
            keywords=keywords,
            author_names=author_names,
            author_ids=[str(x) for x in author_ids],
            affiliations=affiliations,
            source=source,
            raw_source_refs={"search_entry": entry},
        ))
    return records


def _row_to_publication(r: dict) -> PublicationRecord:
    return PublicationRecord(
        scopus_eid=r.get("scopus_eid", ""),
        doi=r.get("doi"),
        title=r.get("title", ""),
        year=r.get("year"),
        cover_date=r.get("cover_date"),
        document_type=r.get("document_type"),
        venue=r.get("venue"),
        citation_count=safe_int(r.get("citation_count")),
        abstract=r.get("abstract"),
        keywords=(r.get("keywords") or "").split("; ") if isinstance(r.get("keywords"), str) else (r.get("keywords") or []),
        author_names=r.get("author_names", "").split("|") if isinstance(r.get("author_names"), str) else (r.get("author_names") or []),
        author_ids=r.get("author_ids", "").split("|") if isinstance(r.get("author_ids"), str) else (r.get("author_ids") or []),
        affiliations=r.get("affiliations", "").split("|") if isinstance(r.get("affiliations"), str) else (r.get("affiliations") or []),
        source=r.get("source"),
        raw_source_refs=r.get("raw_source_refs", {}),
    )
