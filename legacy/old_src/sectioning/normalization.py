"""Heading-to-canonical mapping and fuzzy normalization."""

import re
from typing import Any

from rapidfuzz import fuzz


def build_alias_map(sectioning_config: Any) -> tuple[dict[str, str], list[tuple[str, str]]]:
    """Build exact map (lower -> canonical) and list (variant, canonical) for fuzzy match."""
    exact: dict[str, str] = {}
    fuzzy_list: list[tuple[str, str]] = []
    for canonical, aliases in sectioning_config.heading_aliases.items():
        for a in aliases:
            key = a.strip().lower()
            exact[key] = canonical
            fuzzy_list.append((key, canonical))
    return exact, fuzzy_list


def strip_numbered(line: str) -> str:
    m = re.match(r"^\s*(\d+(?:\.\d+)*\.?)\s+(.+)$", line.strip())
    return m.group(2).strip() if m else line.strip()


def strip_chapter(line: str) -> str:
    m = re.match(r"^\s*chapter\s+[\w.]+\s*[.:]?\s*(.*)$", line.strip(), re.I)
    return (m.group(1) or "").strip() if m else line.strip()


def normalize_heading(
    raw: str,
    exact: dict[str, str],
    fuzzy_list: list[tuple[str, str]],
    fuzzy: bool,
    threshold: int,
) -> tuple[str | None, float]:
    """Return (canonical_name or None, confidence 0-100)."""
    s = raw.strip()
    if not s:
        return None, 0.0
    t = strip_numbered(s)
    t = strip_chapter(t) if t == s else t
    lower = t.lower()
    if lower in exact:
        return exact[lower], 100.0
    if not fuzzy:
        return None, 0.0
    best = 0.0
    best_can: str | None = None
    for variant, canonical in fuzzy_list:
        r = fuzz.ratio(lower, variant)
        if r > best and r >= threshold:
            best = r
            best_can = canonical
    return best_can, float(best) if best_can else 0.0
