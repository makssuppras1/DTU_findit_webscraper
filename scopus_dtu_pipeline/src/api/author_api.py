"""Scopus Author Search (candidates by affiliation) and Author Retrieval (profile)."""
from __future__ import annotations

import logging
from typing import Any

from src.api.scopus_client import ScopusClient, ScopusClientError
from src.config import get_scopus_endpoints, get_pipeline_config
from src.utils.field_utils import get_in, safe_int


def _normalize_affiliation_id(affiliation_id: str) -> str:
    """Return numeric part only; API/checkpoint may store 'AFFILIATION_ID:123' or 'AF-ID:123'."""
    s = str(affiliation_id).strip()
    for prefix in ("AF-ID:", "AFFILIATION_ID:"):
        if s.upper().startswith(prefix):
            return s.split(":", 1)[1].strip()
    return s


def search_authors_by_affiliation(
    client: ScopusClient,
    affiliation_id: str,
    start: int = 0,
    count: int | None = None,
) -> dict:
    """Return raw author search response for AF-ID(affiliation_id), one page."""
    affiliation_id = _normalize_affiliation_id(affiliation_id)
    endpoints = get_scopus_endpoints()
    url = endpoints["author_search"]
    cfg = get_pipeline_config()
    page_size = count if count is not None else cfg.get("page_size", 25)
    params = {
        "query": f"AF-ID({affiliation_id})",
        "start": start,
        "count": page_size,
    }
    return client.get(url, params=params)


def paginated_author_search(
    client: ScopusClient,
    affiliation_id: str,
    page_size: int | None = None,
):
    """Yield author search entries for the given affiliation. Stops at Scopus limit (5000 results)."""
    logger = logging.getLogger("scopus_pipeline")
    cfg = get_pipeline_config()
    size = page_size or cfg.get("page_size", 25)
    max_results = cfg.get("max_author_search_results", 5000)
    start = 0
    total = None
    while start < max_results:
        try:
            data = search_authors_by_affiliation(client, affiliation_id, start=start, count=size)
        except ScopusClientError as e:
            if e.status_code == 400 and "Exceeds the number of search results" in str(e.response_text or ""):
                logger.warning("Author Search hit result limit at start=%s; stopping (max %s)", start, max_results)
                break
            raise
        search_results = get_in(data, "search-results") or {}
        entries = get_in(search_results, "entry")
        if not entries:
            break
        if not isinstance(entries, list):
            entries = [entries]
        for e in entries:
            yield e
        total = get_in(search_results, "opensearch:totalResults")
        try:
            total = int(total) if total is not None else 0
        except (TypeError, ValueError):
            total = 0
        start += len(entries)
        if start >= total or len(entries) < size:
            break
        if start >= max_results:
            logger.warning("Author Search capped at %s results (Scopus limit); total was %s", max_results, total)
            break


def parse_candidate_from_search_entry(entry: dict) -> dict[str, Any]:
    """Build a flat dict for one candidate from author search entry."""
    author_id = get_in(entry, "dc:identifier")
    if isinstance(author_id, str) and "AUTHOR_ID:" in author_id.upper():
        author_id = author_id.split(":", 1)[1].strip()
    return {
        "scopus_author_id": author_id or "",
        "full_name": get_in(entry, "preferred-name", "ce:indexed-name") or get_in(entry, "dc:title") or "",
        "indexed_name": get_in(entry, "ce:indexed-name") or "",
        "orcid": get_in(entry, "orcid") or None,
        "affiliation_current": get_in(entry, "affiliation-current", "affiliation-name") or get_in(entry, "affiliation-name") or None,
        "document_count": safe_int(get_in(entry, "document-count")),
        "subject_areas": get_in(entry, "subject-area") or None,
        "raw_entry": entry,
    }


def get_author_profile(client: ScopusClient, author_id: str, view: str = "STANDARD") -> dict | None:
    """Fetch full author profile by Scopus author ID. Returns None on not found / error."""
    endpoints = get_scopus_endpoints()
    base = endpoints["author_retrieval"]
    url = f"{base}/{author_id}"
    params = {"view": view}
    try:
        data = client.get(url, params=params)
    except Exception:
        return None
    # Response: {"author-retrieval-response": ...} or wrapped in entry
    return get_in(data, "author-retrieval-response") or get_in(data, "author-retrieval-response", 0) or data
