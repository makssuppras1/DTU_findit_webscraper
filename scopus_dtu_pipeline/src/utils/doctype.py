"""Normalize document type strings from Scopus."""
from __future__ import annotations

def normalize_doctype(raw: str | None) -> str:
    if not raw:
        return ""
    return raw.strip().lower()
