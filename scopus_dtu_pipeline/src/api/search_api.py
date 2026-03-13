"""Scopus Search API: document search (e.g. AU-ID + PUBYEAR), pagination."""
from __future__ import annotations

from src.api.scopus_client import ScopusClient
from src.config import get_scopus_endpoints, get_pipeline_config
from src.utils.field_utils import get_in


def scopus_search(
    client: ScopusClient,
    query: str,
    start: int = 0,
    count: int | None = None,
    view: str = "STANDARD",
) -> dict:
    """One page of Scopus document search. query is the boolean query string."""
    endpoints = get_scopus_endpoints()
    url = endpoints["scopus_search"]
    cfg = get_pipeline_config()
    page_size = count if count is not None else cfg.get("page_size", 25)
    params = {
        "query": query,
        "start": start,
        "count": page_size,
        "view": view,
    }
    return client.get(url, params=params)


def search_author_publications_last_n_years(
    client: ScopusClient,
    author_id: str,
    years: int = 5,
    start: int = 0,
    count: int = 25,
) -> dict:
    """Search documents: AU-ID(author_id) AND PUBYEAR > (current_year - years)."""
    import datetime
    current_year = datetime.date.today().year
    threshold = current_year - years
    query = f"AU-ID({author_id}) AND PUBYEAR > {threshold}"
    return scopus_search(client, query, start=start, count=count)


def paginated_scopus_search(
    client: ScopusClient,
    query: str,
    page_size: int | None = None,
) -> list[dict]:
    """Yield all search result entries for the given query."""
    cfg = get_pipeline_config()
    size = page_size or cfg.get("page_size", 25)
    start = 0
    total = None
    while True:
        data = scopus_search(client, query, start=start, count=size)
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


def get_entries_with_doctype(
    client: ScopusClient,
    author_id: str,
    years: int,
    qualifying_doctypes: list[str],
    excluded_doctypes: list[str],
) -> list[dict]:
    """Return entries for author in last N years, filtered by document type."""
    qualifying = set(s.strip().lower() for s in qualifying_doctypes)
    excluded = set(s.strip().lower() for s in excluded_doctypes)
    result = []
    for entry in paginated_scopus_search(client, _author_pubyear_query(author_id, years)):
        dtype = (get_in(entry, "subtypeDescription") or get_in(entry, "dc:description") or "").strip().lower()
        if not dtype:
            continue
        if dtype in excluded:
            continue
        if dtype in qualifying:
            result.append(entry)
    return result


def _author_pubyear_query(author_id: str, years: int) -> str:
    import datetime
    y = datetime.date.today().year - years
    return f"AU-ID({author_id}) AND PUBYEAR > {y}"
