"""Data models for retrieval pipeline: candidates, profiles, publications, output."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ResearcherCandidate:
    input_id: str
    canonical_name: str
    orcid: str | None = None
    email: str | None = None
    department: str | None = None
    institution: str | None = None
    profile_url: str | None = None
    orbit_id: str | None = None


@dataclass
class SourceProfile:
    source: str
    source_id: str
    tier: int
    discovered_at_iso: str
    url: str | None = None


@dataclass
class AuthorInfo:
    name: str
    orcid: str | None
    affiliation: str | None


@dataclass
class RawPublicationRecord:
    ids: dict[str, str]  # doi, openalex_id, etc.
    type: str
    title: str
    year: int | None
    venue: str | None
    journal_name: str | None
    volume: str | None
    issue: str | None
    pages: str | None
    authors: list[AuthorInfo]
    source: str
    source_id: str
    retrieved_at_iso: str
    raw_payload_ref: str | None
    extraction_method: str
    author_orcid: str | None  # ORCID of the author we matched (this professor)
    raw_affiliation_strings: list[str] = field(default_factory=list)


@dataclass
class ProvenanceEntry:
    source: str
    source_id: str
    retrieved_at_iso: str
    raw_payload_ref: str | None
    extraction_method: str
    confidence: str


@dataclass
class DeduplicationInfo:
    status: str  # canonical | exact_duplicate | likely_duplicate
    merged_from: list[str] = field(default_factory=list)


@dataclass
class CanonicalPublication:
    ids: dict[str, str]
    type: str
    title: str
    year: int | None
    venue: str | None
    journal_name: str | None
    volume: str | None
    issue: str | None
    pages: str | None
    authors: list[AuthorInfo]
    provenance: list[ProvenanceEntry]
    deduplication: DeduplicationInfo


@dataclass
class UnmatchedItem:
    raw_record_ref: str
    reason: str
    source: str


@dataclass
class RetrievalMeta:
    retrieval_run_id: str
    retrieved_at_iso: str
    config_scope: list[str]
    pipeline_version: str
    failed_sources: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class RetrievalResult:
    professor_identity: dict[str, Any]
    candidate_source_profiles: list[dict[str, Any]]
    publications: list[CanonicalPublication]
    unmatched_or_uncertain: list[UnmatchedItem]
    retrieval_meta: RetrievalMeta
