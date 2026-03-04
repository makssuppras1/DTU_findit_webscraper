"""Deterministic hashing for document IDs."""

import hashlib


def doc_id(blob_name: str) -> str:
    """Stable deterministic id for resume/output naming."""
    return hashlib.sha1(blob_name.encode("utf-8")).hexdigest()
