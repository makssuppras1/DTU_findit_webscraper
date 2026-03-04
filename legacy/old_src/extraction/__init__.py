"""PDF text extraction: direct text + OCR fallback."""

from .hybrid_extractor import extract_pages

__all__ = ["extract_pages"]
