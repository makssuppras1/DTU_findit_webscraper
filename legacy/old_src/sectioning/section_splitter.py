"""Preclean page text and assemble sections from heading hits."""

import re
from typing import Any

from .heading_detection import HeadingHit

CONFIDENCE_THRESHOLD = 70.0


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


def normalize_ws(text: str) -> str:
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


def preclean(page_texts: list[str]) -> list[str]:
    """Remove header/footer (repeated lines) and normalize whitespace."""
    repeated = _repeated_lines(page_texts)
    if not repeated:
        return [normalize_ws(t) for t in page_texts]
    out = []
    for t in page_texts:
        lines = [ln for ln in t.splitlines() if ln.strip() not in repeated]
        out.append(normalize_ws("\n".join(lines)))
    return out


def assemble_sections(
    page_texts: list[str],
    hits: list[HeadingHit],
) -> tuple[dict[str, dict], list[dict]]:
    """
    Build sections from heading hits. Returns (sections_dict, unknown_sections_list).
    sections_dict: canonical -> {text, start_page, end_page, headings}
    unknown_sections: [{heading, text, start_page, end_page}]
    """
    sections: dict[str, dict] = {}
    unknown: list[dict] = []

    def text_between(start: int, end: int, drop_heading_on_start: str | None) -> str:
        parts = []
        for i in range(start, end + 1):
            if i >= len(page_texts):
                break
            t = page_texts[i]
            if drop_heading_on_start and i == start:
                lines = [ln for ln in t.splitlines() if ln.strip() != drop_heading_on_start]
                t = "\n".join(lines)
            parts.append(t)
        return normalize_ws("\n\n".join(parts))

    if not hits:
        return sections, unknown

    segments: list[tuple[int, int, str | None, list[str], float]] = []
    for i, h in enumerate(hits):
        start = h.page
        end = len(page_texts) - 1
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
