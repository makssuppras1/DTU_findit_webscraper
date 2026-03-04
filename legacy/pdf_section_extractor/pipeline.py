"""Full pipeline: PDF bytes -> section-based JSON."""

import hashlib
import logging

from .extractor import PageText, extract_pages
from .sections import Section, extract_sections

log = logging.getLogger(__name__)


def doc_id_from_blob_name(blob_name: str) -> str:
    """Stable deterministic id for resume/output naming."""
    return hashlib.sha1(blob_name.encode("utf-8")).hexdigest()


def _section_to_dict(s: Section) -> dict:
    return {
        "canonical_name": s.canonical_name,
        "raw_heading": s.raw_headings[0] if s.raw_headings else "",
        "raw_headings": s.raw_headings,
        "text": s.text,
        "start_page": s.start_page,
        "end_page": s.end_page,
        "confidence": round(s.confidence, 1),
    }


def run_pipeline(pdf_bytes: bytes, source_blob: str, debug_write_pages: bool = False) -> dict:
    """Extract pages and sections; return JSON-serializable dict (section-based, no per-page by default)."""
    doc_id = doc_id_from_blob_name(source_blob)
    pages: list[PageText] = extract_pages(pdf_bytes)
    known_sections, unknown_sections = extract_sections(pages)

    ocr_page_count = sum(1 for p in pages if p.provenance == "ocr")
    total_chars = sum(p.char_count for p in pages)
    extraction_stats = {
        "page_count": len(pages),
        "ocr_page_count": ocr_page_count,
        "total_chars": total_chars,
    }

    # sections: dict keyed by canonical name; merge any duplicate canonicals (concat text, all raw_headings)
    by_canonical: dict[str, list[Section]] = {}
    for s in known_sections:
        by_canonical.setdefault(s.canonical_name, []).append(s)
    sections_dict: dict[str, dict] = {}
    for canonical, group in by_canonical.items():
        merged = Section(
            canonical_name=canonical,
            raw_headings=[h for s in group for h in s.raw_headings],
            text="\n\n".join(s.text for s in group),
            start_page=min(s.start_page for s in group),
            end_page=max(s.end_page for s in group),
            confidence=max(s.confidence for s in group),
        )
        sections_dict[canonical] = _section_to_dict(merged)

    # unknown_sections: list with same structure
    unknown_list = [_section_to_dict(s) for s in unknown_sections]

    out: dict = {
        "doc_id": doc_id,
        "source_blob": source_blob,
        "page_count": len(pages),
        "extraction_stats": extraction_stats,
        "sections": sections_dict,
        "unknown_sections": unknown_list,
    }

    if debug_write_pages:
        out["pages"] = [
            {
                "page_index": p.page_index,
                "provenance": p.provenance,
                "char_count": p.char_count,
            }
            for p in pages
        ]

    return out
