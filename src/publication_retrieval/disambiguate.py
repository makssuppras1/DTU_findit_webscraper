"""Author disambiguation: accept / reject / uncertain per publication. Precision over recall."""

import re
from publication_retrieval.models import AuthorInfo, RawPublicationRecord, ResearcherCandidate


def _normalize_name_for_match(s: str) -> str:
    s = re.sub(r"[^\w\s]", " ", (s or "").lower())
    return " ".join(s.split())


def _family_name(name: str) -> str:
    if "," in name:
        return name.split(",")[0].strip()
    parts = name.split()
    return parts[-1] if parts else name


def name_match(publication: RawPublicationRecord, professor: ResearcherCandidate) -> bool:
    """True if any author name matches professor canonical name (family name required)."""
    prof_family = _family_name(professor.canonical_name)
    prof_norm = _normalize_name_for_match(professor.canonical_name)
    for a in publication.authors:
        an = _normalize_name_for_match(a.name)
        af = _family_name(a.name)
        if prof_family and af and _normalize_name_for_match(prof_family) == _normalize_name_for_match(af):
            return True
        if prof_norm and an and (prof_norm in an or an in prof_norm):
            return True
    return False


def has_affiliation(publication: RawPublicationRecord, institution: str | None) -> bool:
    if not institution:
        return False
    inst_lower = institution.lower()
    for s in publication.raw_affiliation_strings:
        if inst_lower in (s or "").lower():
            return True
    for a in publication.authors:
        if a.affiliation and inst_lower in a.affiliation.lower():
            return True
    return False


def affiliation_conflict(publication: RawPublicationRecord, professor: ResearcherCandidate) -> bool:
    """True if publication has explicit different-institution affiliation and no match to professor."""
    if not professor.institution:
        return False
    if has_affiliation(publication, professor.institution):
        return False
    return False


def coauthor_overlap(
    publication: RawPublicationRecord,
    accepted_publications: list[RawPublicationRecord],
    min_overlap: int = 1,
) -> bool:
    names_this = {_normalize_name_for_match(a.name) for a in publication.authors}
    for acc in accepted_publications:
        names_acc = {_normalize_name_for_match(a.name) for a in acc.authors}
        if len(names_this & names_acc) >= min_overlap:
            return True
    return False


def disambiguate(
    publication: RawPublicationRecord,
    professor: ResearcherCandidate,
    accepted_publications: list[RawPublicationRecord],
    accept_threshold: str = "medium",
    strict: bool = True,
) -> tuple[str, str]:
    """Returns (decision, confidence): ('accept'|'reject'|'uncertain', 'high'|'medium'|'low')."""
    if affiliation_conflict(publication, professor):
        return ("reject", "low")
    if publication.author_orcid and professor.orcid:
        prof_orcid = professor.orcid.replace("https://orcid.org/", "").strip()
        pub_orcid = (publication.author_orcid or "").replace("https://orcid.org/", "").strip()
        if prof_orcid == pub_orcid:
            return ("accept", "high")
    if not name_match(publication, professor):
        return ("uncertain", "low")
    if has_affiliation(publication, professor.institution):
        if coauthor_overlap(publication, accepted_publications) or publication.venue or publication.journal_name:
            return ("accept", "medium")
        return ("accept", "low")
    if strict:
        return ("uncertain", "low")
    return ("accept", "low")


def accept_decision(decision: str, confidence: str, accept_threshold: str) -> bool:
    """True if (decision, confidence) passes accept_threshold (high | medium | low)."""
    if decision != "accept":
        return False
    order = ("low", "medium", "high")
    conf_idx = order.index(confidence) if confidence in order else -1
    thresh_idx = order.index(accept_threshold) if accept_threshold in order else 0
    return conf_idx >= thresh_idx
