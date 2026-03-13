"""Researcher/candidate author model."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ResearcherCandidate:
    scopus_author_id: str
    full_name: str
    indexed_name: str
    orcid: str | None
    current_affiliation: str | None
    affiliation_history: list[dict[str, Any]] | None
    document_count: int
    subject_areas: list[str] | list[dict[str, Any]] | None
    active_last_5y: bool
    qualifying_publication_count_last_5y: int
    last_publication_year: int | None
    raw_source_refs: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        return {
            "scopus_author_id": self.scopus_author_id,
            "full_name": self.full_name,
            "indexed_name": self.indexed_name,
            "orcid": self.orcid or "",
            "current_affiliation": self.current_affiliation or "",
            "document_count": self.document_count,
            "active_last_5y": self.active_last_5y,
            "qualifying_publication_count_last_5y": self.qualifying_publication_count_last_5y,
            "last_publication_year": self.last_publication_year or "",
            "subject_areas": self._subject_areas_str(),
        }

    def _subject_areas_str(self) -> str:
        if not self.subject_areas:
            return ""
        if isinstance(self.subject_areas[0], str):
            return "; ".join(self.subject_areas)
        return "; ".join(
            str(s.get("abbreviation") or s.get("code") or s) for s in self.subject_areas
        )
