"""Harvest publications from ORCID and OpenAlex."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

from publication_retrieval.config import SOURCE_PRIORITY
from publication_retrieval.http_client import get_with_retry
from publication_retrieval.models import (
    AuthorInfo,
    RawPublicationRecord,
    SourceProfile,
)

log = logging.getLogger(__name__)

# Map source type strings to our canonical types
ORCID_TYPE_MAP = {
    "journal-article": "journal_article",
    "journal article": "journal_article",
    "conference-paper": "conference_paper",
    "conference paper": "conference_paper",
    "book": "book",
    "book-chapter": "book_chapter",
    "book chapter": "book_chapter",
    "dissertation": "thesis",
    "thesis": "thesis",
    "report": "technical_report",
    "other": "other",
}

OPENALEX_TYPE_MAP = {
    "article": "journal_article",
    "book": "book",
    "book-chapter": "book_chapter",
    "conference-paper": "conference_paper",
    "dissertation": "thesis",
    "paratext": "other",
}


def _normalize_doi(doi: str | None) -> str | None:
    if not doi or not str(doi).strip():
        return None
    s = str(doi).strip().lower()
    for p in ("https://doi.org/", "http://doi.org/", "doi:"):
        if s.startswith(p):
            s = s[len(p):].strip()
    return s if s else None


def _year_from_date(date_str: str | None) -> int | None:
    if not date_str:
        return None
    s = str(date_str).strip()[:4]
    if s.isdigit():
        return int(s)
    return None


def harvest_orcid(
    profile: SourceProfile,
    publication_types: frozenset[str],
    retrieved_at: str,
    raw_ref_template: str,
    max_retries: int = 3,
    backoff: float = 1.0,
) -> tuple[list[RawPublicationRecord], list[dict], Any]:
    """Fetch works from ORCID API. Returns (records, raw_payloads_for_storage, raw_response_or_none)."""
    orcid = profile.source_id
    if not orcid.startswith("https://"):
        orcid_url = f"https://orcid.org/{orcid}"
    else:
        orcid_url = orcid
    url = f"https://api.orcid.org/v3.0/{orcid.replace('https://orcid.org/', '').strip()}/works"
    r, err = get_with_retry(url, headers={"Accept": "application/vnd.orcid+json"}, max_retries=max_retries, backoff_base=backoff)
    if err or r is None:
        return [], [], None
    if r.status_code != 200:
        return [], [], None
    try:
        data = r.json()
    except json.JSONDecodeError:
        return [], [], None

    raw_payloads: list[dict] = []
    records: list[RawPublicationRecord] = []
    groups = data.get("group") or []
    for g in groups:
        summaries = g.get("work-summary") or []
        for ws in summaries:
            put_code = ws.get("put-code")
            title_obj = ws.get("title") or {}
            title = title_obj.get("title", {}).get("value") if isinstance(title_obj.get("title"), dict) else title_obj.get("value") or ""
            if not title and title_obj:
                title = str(title_obj.get("value") or title_obj.get("title") or "")
            pub_type_raw = (ws.get("type") or "").lower()
            pub_type = ORCID_TYPE_MAP.get(pub_type_raw, "other")
            if pub_type != "other" and pub_type not in publication_types:
                continue
            if pub_type == "other" and publication_types != frozenset():
                continue

            date = (ws.get("publication-date") or {})
            year = date.get("year")
            if year is None and date.get("value"):
                year = _year_from_date(date["value"])
            external_ids = ws.get("external-ids", {}).get("external-id") or []
            doi = None
            for e in external_ids:
                if (e.get("external-id-type") or "").lower() == "doi":
                    doi = _normalize_doi(e.get("external-id-value"))
                    break
            journal = (ws.get("journal-title") or {}).get("value") or ws.get("journal-title") or None
            if isinstance(journal, dict):
                journal = journal.get("value")

            raw_payloads.append(ws)
            ref = raw_ref_template.format(work_idx=len(raw_payloads)) if "{work_idx}" in raw_ref_template else raw_ref_template

            records.append(
                RawPublicationRecord(
                    ids={"orcid_put_code": str(put_code), **({"doi": doi} if doi else {})},
                    type=pub_type,
                    title=title or "Untitled",
                    year=int(year) if year is not None else None,
                    venue=None,
                    journal_name=journal,
                    volume=None,
                    issue=None,
                    pages=None,
                    authors=[AuthorInfo(name="Unknown", orcid=profile.source_id, affiliation=None)],
                    source="orcid",
                    source_id=profile.source_id,
                    retrieved_at_iso=retrieved_at,
                    raw_payload_ref=ref,
                    extraction_method="orcid_works_api",
                    author_orcid=profile.source_id,
                    raw_affiliation_strings=[],
                )
            )
    return records, raw_payloads, data


def harvest_openalex(
    profile: SourceProfile,
    publication_types: frozenset[str],
    retrieved_at: str,
    raw_ref_template: str,
    per_page: int = 200,
    max_retries: int = 3,
    backoff: float = 1.0,
) -> tuple[list[RawPublicationRecord], list[dict], list[Any]]:
    """Fetch works from OpenAlex by author ORCID. Returns (records, raw_payloads, list of page responses)."""
    orcid = profile.source_id
    if not orcid.startswith("https://"):
        orcid_filter = f"https://orcid.org/{orcid}"
    else:
        orcid_filter = orcid
    records: list[RawPublicationRecord] = []
    raw_payloads: list[dict] = []
    all_responses: list[Any] = []
    page = 1
    while True:
        url = "https://api.openalex.org/works"
        params = {
            "filter": f"authorships.author.orcid:{orcid_filter}",
            "per-page": per_page,
            "page": page,
        }
        r, err = get_with_retry(url, params=params, max_retries=max_retries, backoff_base=backoff)
        if err or r is None:
            return records, raw_payloads, all_responses
        if r.status_code != 200:
            return records, raw_payloads, all_responses
        try:
            data = r.json()
        except json.JSONDecodeError:
            return records, raw_payloads, all_responses
        all_responses.append(data)
        results = data.get("results") or []
        for w in results:
            oa_type = (w.get("type") or "article").lower()
            pub_type = OPENALEX_TYPE_MAP.get(oa_type, "journal_article" if oa_type == "article" else "other")
            if pub_type not in publication_types:
                continue

            doi_url = w.get("doi")
            doi = _normalize_doi(doi_url) if doi_url else None
            oa_id = w.get("id", "")
            if isinstance(oa_id, str) and "/" in oa_id:
                oa_id_short = oa_id.split("/")[-1]
            else:
                oa_id_short = str(oa_id)
            title = w.get("title") or w.get("display_name") or "Untitled"
            year = w.get("publication_year")
            primary = w.get("primary_location") or {}
            source_info = primary.get("source") or {}
            venue = source_info.get("display_name") or primary.get("raw_source_name")
            journal_name = venue

            authorships = w.get("authorships") or []
            authors: list[AuthorInfo] = []
            author_orcid: str | None = None
            raw_affils: list[str] = []
            for a in authorships:
                author = a.get("author") or {}
                name = author.get("display_name") or a.get("raw_author_name") or ""
                oc = author.get("orcid")
                if oc and orcid_filter and (oc == orcid_filter or oc.endswith(orcid_filter.replace("https://orcid.org/", ""))):
                    author_orcid = oc
                affils = a.get("raw_affiliation_strings") or []
                raw_affils.extend(affils)
                authors.append(AuthorInfo(name=name, orcid=oc, affiliation="; ".join(affils) if affils else None))
            if not authors:
                authors = [AuthorInfo(name="Unknown", orcid=None, affiliation=None)]

            biblio = w.get("biblio") or {}
            raw_payloads.append(w)
            ref = raw_ref_template.format(work_idx=len(raw_payloads)) if "{work_idx}" in raw_ref_template else raw_ref_template

            records.append(
                RawPublicationRecord(
                    ids={
                        "openalex_id": oa_id_short,
                        **({"doi": doi} if doi else {}),
                    },
                    type=pub_type,
                    title=title,
                    year=year,
                    venue=venue,
                    journal_name=journal_name,
                    volume=biblio.get("volume"),
                    issue=biblio.get("issue"),
                    pages=f"{biblio.get('first_page', '')}-{biblio.get('last_page', '')}".strip("-") or None,
                    authors=authors,
                    source="openalex",
                    source_id=profile.source_id,
                    retrieved_at_iso=retrieved_at,
                    raw_payload_ref=ref,
                    extraction_method="openalex_author_works",
                    author_orcid=author_orcid,
                    raw_affiliation_strings=raw_affils,
                )
            )
        meta = data.get("meta") or {}
        if page >= (meta.get("page") or 1) and len(results) < (meta.get("per_page") or per_page):
            break
        if not results:
            break
        page += 1
    return records, raw_payloads, all_responses


def harvest_source(
    profile: SourceProfile,
    publication_types: frozenset[str],
    retrieved_at: str,
    raw_ref_template: str,
    per_page_openalex: int = 200,
    max_retries: int = 3,
    backoff: float = 1.0,
) -> tuple[list[RawPublicationRecord], list[dict] | list[list[dict]], str | None]:
    """Dispatch to ORCID or OpenAlex. Returns (records, raw_payloads, error_msg). raw_payloads format depends on source."""
    if profile.source == "orcid":
        recs, payloads, _ = harvest_orcid(profile, publication_types, retrieved_at, raw_ref_template, max_retries, backoff)
        return (recs, payloads, None)
    if profile.source == "openalex":
        recs, payloads, _ = harvest_openalex(
            profile, publication_types, retrieved_at, raw_ref_template, per_page_openalex, max_retries, backoff
        )
        return (recs, payloads, None)
    if profile.source == "dtu_orbit":
        return ([], [], "dtu_orbit not implemented in MVP")
    return ([], [], f"unknown source: {profile.source}")
