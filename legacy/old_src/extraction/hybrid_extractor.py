"""Orchestrate PDF text extraction with OCR fallback per page."""

import logging
from typing import Any

import fitz

from .ocr import page_to_png_bytes, run_ocr
from .pdf_text import get_page_text

log = logging.getLogger(__name__)


def _needs_ocr(text: str, min_chars: int) -> bool:
    if len(text.strip()) < min_chars:
        return True
    printable = sum(1 for c in text if c.isprintable() or c in "\n\t")
    return printable / len(text) < 0.5 if text else True


def extract_pages(config: Any, pdf_bytes: bytes) -> list[tuple[int, str, str]]:
    """
    Extract text per page. Returns list of (page_index, text, provenance).
    provenance is "pdf_text" or "ocr".
    """
    ext = config.extraction
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    out: list[tuple[int, str, str]] = []
    try:
        n = len(doc)
        if ext.max_pages and ext.max_pages > 0:
            n = min(n, ext.max_pages)
        for i in range(n):
            page = doc[i]
            text = get_page_text(page)
            prov = "pdf_text"
            if ext.ocr_fallback and _needs_ocr(text, ext.ocr_min_chars):
                img_bytes = page_to_png_bytes(page, ext.ocr_dpi)
                text = run_ocr(img_bytes, ext.ocr_lang)
                prov = "ocr"
            out.append((i, text, prov))
    finally:
        doc.close()
    return out
