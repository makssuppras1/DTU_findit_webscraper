"""Detect section heading lines and collect hits with canonical names."""

import re
from dataclasses import dataclass
from typing import Any

from .normalization import build_alias_map, normalize_heading


@dataclass
class HeadingHit:
    page: int
    raw: str
    canonical: str | None
    confidence: float


def is_heading_line(line: str) -> bool:
    s = line.strip()
    if not s or len(s) < 3 or len(s) > 200:
        return False
    if re.match(r"^\s*\d+(?:\.\d+)*\.?\s+.+$", s):
        return True
    if re.match(r"^\s*chapter\s+[\w.]+\s*", s, re.I):
        return True
    if re.match(r"^[A-Z0-9\s\-]{3,}$", s):
        return True
    return False


def detect_hits(config: Any, page_texts: list[str]) -> list[HeadingHit]:
    """Scan page texts for heading lines; return list of HeadingHit (one per page, first match)."""
    exact, fuzzy_list = build_alias_map(config.sectioning)
    sec = config.sectioning
    hits: list[HeadingHit] = []
    for page_idx, text in enumerate(page_texts):
        for line in text.splitlines():
            line = line.strip()
            if not is_heading_line(line):
                continue
            can, conf = normalize_heading(line, exact, fuzzy_list, sec.fuzzy_match, sec.fuzzy_threshold)
            hits.append(HeadingHit(page=page_idx, raw=line, canonical=can, confidence=conf))
            break
    return hits
