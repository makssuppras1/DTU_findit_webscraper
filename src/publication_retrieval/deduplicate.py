"""Deduplicate raw publication records: exact (DOI), likely (title+year+authors), unresolved."""

import re
from publication_retrieval.config import SOURCE_PRIORITY
from publication_retrieval.models import (
    DeduplicationInfo,
    ProvenanceEntry,
    RawPublicationRecord,
    CanonicalPublication,
)

try:
    from rapidfuzz import fuzz
except ImportError:
    fuzz = None


def _normalize_doi(doi: str | None) -> str | None:
    if not doi or not str(doi).strip():
        return None
    s = str(doi).strip().lower()
    for p in ("https://doi.org/", "http://doi.org/", "doi:"):
        if s.startswith(p):
            s = s[len(p) :].strip()
    return s if s else None


def _normalize_title(s: str) -> str:
    s = re.sub(r"[^\w\s]", " ", (s or "").lower())
    return " ".join(s.split())


def _family_names_record(record: RawPublicationRecord) -> set[str]:
    out = set()
    for a in record.authors:
        name = a.name or ""
        if "," in name:
            out.add(name.split(",")[0].strip().lower())
        else:
            parts = name.split()
            if parts:
                out.add(parts[-1].lower())
    return out


def _family_names_canonical(c: CanonicalPublication) -> set[str]:
    out = set()
    for a in c.authors:
        name = a.name or ""
        if "," in name:
            out.add(name.split(",")[0].strip().lower())
        else:
            parts = name.split()
            if parts:
                out.add(parts[-1].lower())
    return out


def _author_overlap_raw(r1: RawPublicationRecord, r2: RawPublicationRecord) -> float:
    n1 = _family_names_record(r1)
    n2 = _family_names_record(r2)
    if not n1 or not n2:
        return 0.0
    return len(n1 & n2) / max(len(n1), len(n2))


def _author_overlap(r1: RawPublicationRecord, r2: RawPublicationRecord) -> float:
    return _author_overlap_raw(r1, r2)


def _title_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    na = _normalize_title(a)
    nb = _normalize_title(b)
    if not na or not nb:
        return 0.0
    if fuzz is None:
        return 1.0 if na == nb else 0.0
    return fuzz.ratio(na, nb) / 100.0


def _merge_provenance(records: list[RawPublicationRecord]) -> list[ProvenanceEntry]:
    out: list[ProvenanceEntry] = []
    seen: set[tuple[str, str, str]] = set()
    for r in records:
        key = (r.source, r.source_id, r.retrieved_at_iso)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            ProvenanceEntry(
                source=r.source,
                source_id=r.source_id,
                retrieved_at_iso=r.retrieved_at_iso,
                raw_payload_ref=r.raw_payload_ref,
                extraction_method=r.extraction_method,
                confidence="high" if r.author_orcid else "medium",
            )
        )
    return out


def _pick_best(records: list[RawPublicationRecord]) -> RawPublicationRecord:
    for src in SOURCE_PRIORITY:
        for r in records:
            if r.source == src:
                return r
    return records[0]


def _to_canonical(r: RawPublicationRecord, status: str, merged_from: list[str], provenance: list[ProvenanceEntry] | None = None) -> CanonicalPublication:
    if provenance is None:
        provenance = [
            ProvenanceEntry(
                source=r.source,
                source_id=r.source_id,
                retrieved_at_iso=r.retrieved_at_iso,
                raw_payload_ref=r.raw_payload_ref,
                extraction_method=r.extraction_method,
                confidence="high" if r.author_orcid else "medium",
            )
        ]
    return CanonicalPublication(
        ids=dict(r.ids),
        type=r.type,
        title=r.title,
        year=r.year,
        venue=r.venue,
        journal_name=r.journal_name,
        volume=r.volume,
        issue=r.issue,
        pages=r.pages,
        authors=list(r.authors),
        provenance=provenance,
        deduplication=DeduplicationInfo(status=status, merged_from=merged_from),
    )


def deduplicate(
    raw_records: list[RawPublicationRecord],
    title_similarity_threshold: float = 0.95,
    year_tolerance: int = 1,
) -> list[CanonicalPublication]:
    """Merge by DOI first; then group likely duplicates by title+year+author. Returns canonical list."""
    if not raw_records:
        return []

    by_doi: dict[str, list[RawPublicationRecord]] = {}
    for r in raw_records:
        doi = _normalize_doi(r.ids.get("doi"))
        if doi:
            by_doi.setdefault(doi, []).append(r)

    canonical: list[CanonicalPublication] = []
    used: set[int] = set()
    for group in by_doi.values():
        best = _pick_best(group)
        merged_from = [f"{r.source}:{r.raw_payload_ref or str(r.ids)}" for r in group if r is not best]
        canonical.append(_to_canonical(best, "canonical", merged_from, _merge_provenance(group)))
        for r in group:
            used.add(id(r))

    remaining = [r for r in raw_records if id(r) not in used]
    for r in remaining:
        best_idx: int | None = None
        for i, can in enumerate(canonical):
            if can.ids.get("doi"):
                continue
            sim = _title_similarity(r.title, can.title)
            if sim < title_similarity_threshold:
                continue
            ry, cy = r.year or 0, can.year or 0
            if abs(ry - cy) > year_tolerance:
                continue
            n1 = _family_names_record(r)
            n2 = _family_names_canonical(can)
            overlap = len(n1 & n2) / max(len(n1), len(n2)) if n1 and n2 else 0.0
            if overlap < 0.3:
                continue
            best_idx = i
            break
        if best_idx is not None:
            merged_from = [f"{r.source}:{r.raw_payload_ref or str(r.ids)}"]
            existing = canonical[best_idx]
            new_provenance = _merge_provenance([r]) + existing.provenance
            canonical[best_idx] = CanonicalPublication(
                ids=existing.ids,
                type=existing.type,
                title=existing.title,
                year=existing.year,
                venue=existing.venue,
                journal_name=existing.journal_name,
                volume=existing.volume,
                issue=existing.issue,
                pages=existing.pages,
                authors=existing.authors,
                provenance=new_provenance,
                deduplication=DeduplicationInfo(
                    status="likely_duplicate",
                    merged_from=existing.deduplication.merged_from + merged_from,
                ),
            )
        else:
            canonical.append(_to_canonical(r, "canonical", []))

    return canonical
