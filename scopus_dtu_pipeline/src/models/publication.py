"""Publication record model."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PublicationRecord:
    scopus_eid: str
    doi: str | None
    title: str
    year: int | None
    cover_date: str | None
    document_type: str | None
    venue: str | None
    citation_count: int
    abstract: str | None
    keywords: list[str] | None
    author_names: list[str]
    author_ids: list[str]
    affiliations: list[str]
    source: str | None
    raw_source_refs: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        return {
            "scopus_eid": self.scopus_eid,
            "doi": self.doi or "",
            "title": self.title or "",
            "year": self.year or "",
            "cover_date": self.cover_date or "",
            "document_type": self.document_type or "",
            "venue": self.venue or "",
            "citation_count": self.citation_count or 0,
            "abstract": (self.abstract or "")[:5000],
            "keywords": "; ".join(self.keywords) if self.keywords else "",
            "author_names": "|".join(self.author_names) if self.author_names else "",
            "author_ids": "|".join(self.author_ids) if self.author_ids else "",
            "affiliations": "|".join(self.affiliations) if self.affiliations else "",
            "source": self.source or "",
        }
