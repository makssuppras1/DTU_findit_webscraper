"""Heading detection, normalization, and section assembly."""

import re
from dataclasses import dataclass

from rapidfuzz import fuzz

CONFIDENCE_THRESHOLD = 70.0


def _build_alias_map(sectioning_config) -> tuple[dict[str, str], list[tuple[str, str]]]:
    exact: dict[str, str] = {}
    fuzzy_list: list[tuple[str, str]] = []
    for canonical, aliases in sectioning_config.heading_aliases.items():
        for a in aliases:
            key = a.strip().lower()
            exact[key] = canonical
            fuzzy_list.append((key, canonical))
    return exact, fuzzy_list


def _strip_numbered(line: str) -> str:
    m = re.match(r"^\s*(\d+(?:\.\d+)*\.?)\s+(.+)$", line.strip())
    return m.group(2).strip() if m else line.strip()


def _strip_chapter(line: str) -> str:
    m = re.match(r"^\s*chapter\s+[\w.]+\s*[.:]?\s*(.*)$", line.strip(), re.I)
    return (m.group(1) or "").strip() if m else line.strip()


def _normalize_heading(raw: str, exact: dict, fuzzy_list: list, fuzzy: bool, threshold: int) -> tuple[str | None, float]:
    s = raw.strip()
    if not s:
        return None, 0.0
    t = _strip_numbered(s)
    t = _strip_chapter(t) if t == s else t
    lower = t.lower()
    if lower in exact:
        return exact[lower], 100.0
    if not fuzzy:
        return None, 0.0
    best, best_can = 0.0, None
    for variant, canonical in fuzzy_list:
        r = fuzz.ratio(lower, variant)
        if r > best and r >= threshold:
            best, best_can = r, canonical
    return best_can, float(best) if best_can else 0.0


def _is_heading_line(line: str) -> bool:
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


@dataclass
class _HeadingHit:
    page: int
    raw: str
    canonical: str | None
    confidence: float


def detect_hits(config, page_texts: list[str]) -> list[_HeadingHit]:
    exact, fuzzy_list = _build_alias_map(config.sectioning)
    sec = config.sectioning
    hits: list[_HeadingHit] = []
    for page_idx, text in enumerate(page_texts):
        for line in text.splitlines():
            line = line.strip()
            if not _is_heading_line(line):
                continue
            can, conf = _normalize_heading(line, exact, fuzzy_list, sec.fuzzy_match, sec.fuzzy_threshold)
            hits.append(_HeadingHit(page=page_idx, raw=line, canonical=can, confidence=conf))
            break
    return hits


def _repeated_lines(page_texts: list[str]) -> set[str]:
    n = len(page_texts)
    thresh = max(2, n // 2)
    cnt: dict[str, int] = {}
    for text in page_texts:
        seen: set[str] = set()
        for ln in text.splitlines():
            s = ln.strip()
            if len(s) < 4:
                continue
            if s not in seen:
                seen.add(s)
                cnt[s] = cnt.get(s, 0) + 1
    return {k for k, v in cnt.items() if v >= thresh}


def _normalize_ws(text: str) -> str:
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


def preclean(page_texts: list[str]) -> list[str]:
    repeated = _repeated_lines(page_texts)
    if not repeated:
        return [_normalize_ws(t) for t in page_texts]
    return [_normalize_ws("\n".join(ln for ln in t.splitlines() if ln.strip() not in repeated)) for t in page_texts]


def assemble_sections(page_texts: list[str], hits: list[_HeadingHit]) -> tuple[dict[str, dict], list[dict]]:
    sections: dict[str, dict] = {}
    unknown: list[dict] = []

    def text_between(start: int, end: int, drop: str | None) -> str:
        parts = []
        for i in range(start, end + 1):
            if i >= len(page_texts):
                break
            t = page_texts[i]
            if drop and i == start:
                lines = [ln for ln in t.splitlines() if ln.strip() != drop]
                t = "\n".join(lines)
            parts.append(t)
        return _normalize_ws("\n\n".join(parts))

    if not hits:
        return sections, unknown

    segments: list[tuple[int, int, str | None, list[str], float]] = []
    for i, h in enumerate(hits):
        start, end = h.page, len(page_texts) - 1
        if i + 1 < len(hits):
            end = hits[i + 1].page - 1
        end = max(start, end)
        if segments and segments[-1][2] == h.canonical and h.canonical:
            prev_start, prev_end, _, prev_raw, prev_conf = segments[-1]
            segments[-1] = (prev_start, end, h.canonical, prev_raw + [h.raw], max(prev_conf, h.confidence))
        else:
            segments.append((start, end, h.canonical, [h.raw], h.confidence))

    for start, end, canonical, raw_headings, confidence in segments:
        drop = raw_headings[0] if raw_headings else None
        text = text_between(start, end, drop)
        if canonical and confidence >= CONFIDENCE_THRESHOLD:
            if canonical not in sections:
                sections[canonical] = {"text": text, "start_page": start, "end_page": end, "headings": raw_headings}
            else:
                prev = sections[canonical]
                prev["text"] = prev["text"] + "\n\n" + text
                prev["end_page"] = end
                prev["headings"] = prev["headings"] + raw_headings
        else:
            unknown.append({"heading": raw_headings[0] if raw_headings else "", "text": text, "start_page": start, "end_page": end})
    return sections, unknown
