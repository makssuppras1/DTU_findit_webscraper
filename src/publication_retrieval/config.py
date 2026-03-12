"""Retrieval pipeline config. MVP: fixed scope; production can add YAML."""

from dataclasses import dataclass

# MVP publication types (plan: journal, conference, book, book_chapter, thesis)
DEFAULT_PUBLICATION_TYPES = frozenset({
    "journal_article",
    "conference_paper",
    "book",
    "book_chapter",
    "thesis",
})

# Source priority for metadata merge (higher index = lower priority)
SOURCE_PRIORITY = ("dtu_orbit", "orcid", "openalex", "crossref", "semantic_scholar", "dblp")


@dataclass
class RetrievalConfig:
    persons_csv_gcs_path: str = "dtu_findit/master_thesis_meta/Cleaned/dtu_orbit_persons_cleaned.csv"
    gcs_bucket: str | None = None  # None = use GCS_BUCKET env
    publication_types: frozenset[str] = DEFAULT_PUBLICATION_TYPES
    accept_threshold: str = "medium"  # high | medium | low
    strict_disambiguation: bool = True
    output_dir: str = "retrieval_output"
    raw_payloads_dir: str | None = None  # None = output_dir/raw
    per_page_openalex: int = 200
    max_retries: int = 3
    retry_backoff_base_seconds: float = 1.0
    pipeline_version: str = "0.1.0"
    limit: int | None = None
