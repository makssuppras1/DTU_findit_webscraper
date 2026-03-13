"""Scopus Abstract Retrieval: enrich publication with abstract, keywords, etc."""
from __future__ import annotations

from typing import Any

from src.api.scopus_client import ScopusClient
from src.config import get_scopus_endpoints
from src.utils.field_utils import get_in


def get_abstract_by_eid(client: ScopusClient, eid: str, view: str = "FULL") -> dict | None:
    """Retrieve abstract by EID (e.g. 2-s2.0-85123456789). Returns None on error."""
    endpoints = get_scopus_endpoints()
    # Abstract API uses path /abstract/eid/{eid}
    base = endpoints["abstract_retrieval"].replace("/scopus_id", "/eid")
    url = f"{base}/{eid}"
    params = {"view": view}
    try:
        data = client.get(url, params=params)
    except Exception:
        return None
    return get_in(data, "abstracts-retrieval-response") or data


def get_abstract_by_scopus_id(client: ScopusClient, scopus_id: str, view: str = "FULL") -> dict | None:
    """Retrieve abstract by Scopus ID (numeric)."""
    endpoints = get_scopus_endpoints()
    base = endpoints["abstract_retrieval"]
    url = f"{base}/{scopus_id}"
    params = {"view": view}
    try:
        data = client.get(url, params=params)
    except Exception:
        return None
    return get_in(data, "abstracts-retrieval-response") or data


def enrich_from_abstract_response(abstract_response: dict | None) -> dict[str, Any]:
    """Extract abstract, keywords, and other fields from abstract retrieval response."""
    if not abstract_response:
        return {"abstract": None, "keywords": None, "raw_abstract": None}
    coredata = get_in(abstract_response, "coredata") or {}
    item = get_in(abstract_response, "item") or {}
    abstract = get_in(item, "bibrecord", "head", "abstracts", "abstract", 0, "#text") or get_in(item, "abstract") or get_in(coredata, "dc:description")
    authkeywords = get_in(item, "authkeywords", "author-keyword")
    if authkeywords and not isinstance(authkeywords, list):
        authkeywords = [authkeywords]
    keywords = [str(k.get("#text", k)).strip() for k in (authkeywords or []) if k]
    return {
        "abstract": abstract,
        "keywords": keywords or None,
        "raw_abstract": abstract_response,
    }
