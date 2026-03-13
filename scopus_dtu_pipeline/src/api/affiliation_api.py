"""Scopus Affiliation Search: resolve DTU affiliation ID."""
from __future__ import annotations

from src.api.scopus_client import ScopusClient
from src.config import get_scopus_endpoints
from src.utils.field_utils import get_in


def search_affiliation(client: ScopusClient, query: str) -> list[dict]:
    """Return list of affiliation search results (entries)."""
    endpoints = get_scopus_endpoints()
    url = endpoints["affiliation_search"]
    params = {"query": query}
    data = client.get(url, params=params)
    entries = get_in(data, "search-results", "entry") or []
    return entries if isinstance(entries, list) else [entries]


def find_dtu_affiliation(client: ScopusClient, name_query: str = "Technical University of Denmark") -> dict | None:
    """Query affiliation search and return first matching DTU affiliation by affiliation-name only.
    DTU has multiple sites (e.g. Lyngby, Copenhagen); we match on affiliation-name only, not city."""
    query = f'affil("{name_query}")'
    entries = search_affiliation(client, query)
    for entry in entries:
        name = get_in(entry, "affiliation-name") or get_in(entry, "dc:title") or ""
        if "technical university of denmark" in str(name).lower() or "dtu" in str(name).lower():
            return entry
    return entries[0] if entries else None


def get_affiliation_id(affiliation_entry: dict) -> str | None:
    """Extract Scopus affiliation ID from search result entry (numeric part only)."""
    # dc:identifier can be "AF-ID:12345" or "AFFILIATION_ID:12345"
    ident = get_in(affiliation_entry, "dc:identifier")
    if not ident:
        return None
    s = str(ident).strip()
    for prefix in ("AF-ID:", "AFFILIATION_ID:"):
        if s.upper().startswith(prefix):
            return s.split(":", 1)[1].strip()
    return s
