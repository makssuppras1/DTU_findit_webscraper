"""Publication retrieval pipeline for professors. Data retrieval only; no RAG/embeddings."""

from publication_retrieval.config import RetrievalConfig
from publication_retrieval.models import (
    ResearcherCandidate,
    SourceProfile,
    RawPublicationRecord,
    CanonicalPublication,
    RetrievalResult,
)
from publication_retrieval.pipeline import run_retrieval_pipeline

__all__ = [
    "RetrievalConfig",
    "ResearcherCandidate",
    "SourceProfile",
    "RawPublicationRecord",
    "CanonicalPublication",
    "RetrievalResult",
    "run_retrieval_pipeline",
]
