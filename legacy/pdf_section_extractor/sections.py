"""Section detection: header/footer removal, heading detection, normalization, assembly."""

import re
from dataclasses import dataclass

from rapidfuzz import fuzz

from .extractor import PageText

# Canonical section names
ABSTRACT = "abstract"
INTRODUCTION = "introduction"
BACKGROUND = "background"
RELATED_WORK = "related_work"
LITERATURE_REVIEW = "literature_review"
METHODOLOGY = "methodology"
IMPLEMENTATION = "implementation"
RESULTS = "results"
DISCUSSION = "discussion"
CONCLUSION = "conclusion"
FUTURE_WORK = "future_work"
REFERENCES = "references"

SECTION_ALIASES: list[tuple[list[str], str]] = [
    (["abstract", "summary", "resumé", "resume"], ABSTRACT),
    (["introduction", "intro", "overview"], INTRODUCTION),
    (["background"], BACKGROUND),
    (["related work", "related works", "state of the art"], RELATED_WORK),
    (["literature review", "literature survey"], LITERATURE_REVIEW),
    (
        [
            "methodology",
            "methods",
            "method",
            "materials and methods",
            "approach",
            "approaches",
            "design",
        ],
        METHODOLOGY,
    ),
    (["implementation"], IMPLEMENTATION),
    (
        [
            "results",
            "evaluation",
            "experiments",
            "experimental results",
            "findings",
            "analysis",
        ],
        RESULTS,
    ),
    (["discussion"], DISCUSSION),
    (
        [
            "conclusion",
            "conclusions",
            "concluding remarks",
            "summary and conclusion",
        ],
        CONCLUSION,
    ),
    (["future work", "future directions"], FUTURE_WORK),
    (["references", "bibliography", "works cited"], REFERENCES),
]

NORMALIZE_MAP: dict[str, str] = {}
FUZZY_VARIANTS: list[tuple[str, str]] = []
for variants, norm in SECTION_ALIASES:
    for v in variants:
        NORMALIZE_MAP[v.strip().lower()] = norm
    for v in variants:
        FUZZY_VARIANTS.append((v, norm))

FUZZ_THRESHOLD = 85
CONFIDENCE_THRESHOLD = 70.0  # above => sections; below => unknown_sections


def _strip_heading_prefix(s: str) -> str:
    s = s.strip()
    m = RE_NUMBERED.match(s)
    if m:
        return s[m.end(1) :].strip()
    m = RE_CHAPTER.match(s)
    if m:
        return (m.group(2) or "").strip() or s
    return s


def normalize_section_name(raw_heading: str) -> tuple[str | None, float]:
    """Return (canonical_name or None, confidence 0-100). None => unknown_sections."""
    stripped = raw_heading.strip()
    if not stripped:
        return None, 0.0
    to_match = _strip_heading_prefix(stripped).lower() or stripped.lower()
    if to_match in NORMALIZE_MAP:
        return NORMALIZE_MAP[to_match], 100.0
    best_ratio = 0.0
    best_norm: str | None = None
    for variant, norm in FUZZY_VARIANTS:
        r = fuzz.ratio(to_match, variant)
        if r > best_ratio:
            best_ratio = r
            best_norm = norm
    if best_ratio >= FUZZ_THRESHOLD:
        return best_norm, float(best_ratio)
    return None, float(best_ratio)


# Backward compatibility
def _normalize_heading(raw: str) -> tuple[str | None, str]:
    canonical, _ = normalize_section_name(raw)
    return canonical, raw.strip()


# Patterns for section headings
RE_NUMBERED = re.compile(r"^\s*(\d+(?:\.\d+)*\.?)\s+(.+)$")
RE_CHAPTER = re.compile(r"^\s*chapter\s+(\d+|[IVXLCDM]+)\s*[.:]?\s*(.*)$", re.I)
RE_ALL_CAPS = re.compile(r"^[A-Z0-9\s\-]{3,}$")

# Reject very short lines as headings (false positives)
MIN_HEADING_LEN = 3
MAX_HEADING_LEN = 200


def _is_heading_line(line: str) -> bool:
    s = line.strip()
    if not s or len(s) < MIN_HEADING_LEN or len(s) > MAX_HEADING_LEN:
        return False
    if RE_NUMBERED.match(s):
        return True
    if RE_CHAPTER.match(s):
        return True
    if RE_ALL_CAPS.match(s):
        return True
    return False


def _normalize_whitespace(text: str) -> str:
    """Collapse spaces per line, strip; preserve line breaks for heading detection."""
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


def _detect_repeated_lines(page_texts: list[str]) -> set[str]:
    if not page_texts:
        return set()
    n = len(page_texts)
    threshold = max(2, n // 2)
    line_counts: dict[str, int] = {}
    for text in page_texts:
        seen_this_page: set[str] = set()
        for line in text.splitlines():
            s = line.strip()
            if len(s) < 4:
                continue
            if s not in seen_this_page:
                seen_this_page.add(s)
                line_counts[s] = line_counts.get(s, 0) + 1
    return {line for line, count in line_counts.items() if count >= threshold}


def remove_headers_footers(page_texts: list[PageText]) -> list[PageText]:
    texts = [p.text for p in page_texts]
    repeated = _detect_repeated_lines(texts)
    if not repeated:
        return list(page_texts)
    out: list[PageText] = []
    for p in page_texts:
        lines = p.text.splitlines()
        kept = [ln for ln in lines if ln.strip() not in repeated]
        new_text = "\n".join(kept)
        out.append(
            PageText(
                page_index=p.page_index,
                text=new_text,
                provenance=p.provenance,
                char_count=len(new_text),
                printable_ratio=p.printable_ratio,
            )
        )
    return out


@dataclass
class HeadingHit:
    page_index: int
    raw_heading: str
    canonical: str | None
    confidence: float


@dataclass
class Section:
    canonical_name: str
    raw_headings: list[str]
    text: str
    start_page: int
    end_page: int
    confidence: float


def _preclean_pages(pages: list[PageText]) -> list[str]:
    """Header/footer removal + whitespace normalization. Returns list of page text strings."""
    cleaned = remove_headers_footers(pages)
    return [_normalize_whitespace(p.text) for p in cleaned]


def _detect_heading_hits(page_texts: list[str]) -> list[HeadingHit]:
    """Scan pages in order; return list of heading hits (page_index, raw_heading, canonical, confidence). One per page (first match) to keep segment boundaries page-aligned."""
    hits: list[HeadingHit] = []
    for page_idx, text in enumerate(page_texts):
        for line in text.splitlines():
            line = line.strip()
            if _is_heading_line(line):
                canonical, confidence = normalize_section_name(line)
                hits.append(
                    HeadingHit(
                        page_index=page_idx,
                        raw_heading=line,
                        canonical=canonical,
                        confidence=confidence,
                    )
                )
                break
    return hits


def _extract_abstract_heuristic(page_texts: list[str], first_heading_page: int) -> Section | None:
    """If content before first heading contains 'abstract' and has substance, return an abstract section."""
    if first_heading_page <= 0:
        return None
    pre_text = " ".join(page_texts[:first_heading_page]).lower()
    if "abstract" not in pre_text:
        return None
    # Rebuild text with normalized whitespace
    block = _normalize_whitespace("\n\n".join(page_texts[:first_heading_page]))
    if len(block) < 30:
        return None
    return Section(
        canonical_name=ABSTRACT,
        raw_headings=["Abstract"],
        text=block,
        start_page=0,
        end_page=first_heading_page - 1,
        confidence=80.0,
    )


def assemble_sections(
    page_texts: list[str],
    heading_hits: list[HeadingHit],
    add_abstract_heuristic: bool = True,
) -> tuple[list[Section], list[Section]]:
    """
    Build sections from page text and heading hits.
    Sections span from one heading to the next (or end). Multi-page sections get concatenated text.
    Multiple headings mapping to the same canonical are merged (concat text, all raw_headings, span range).
    Returns (sections with canonical names, unknown_sections).
    """
    known: list[Section] = []
    unknown: list[Section] = []
    if not page_texts:
        return known, unknown

    first_heading_page = heading_hits[0].page_index if heading_hits else len(page_texts)

    if add_abstract_heuristic:
        abstract_sec = _extract_abstract_heuristic(page_texts, first_heading_page)
        if abstract_sec:
            known.append(abstract_sec)

    def text_between(start_page: int, end_page: int, exclude_heading_line_on_start: bool = False) -> str:
        parts = []
        for i in range(start_page, end_page + 1):
            if i >= len(page_texts):
                break
            t = page_texts[i]
            if exclude_heading_line_on_start and i == start_page and heading_hits:
                for h in heading_hits:
                    if h.page_index == start_page:
                        lines = t.splitlines()
                        rest = [ln for ln in lines if ln.strip() != h.raw_heading]
                        t = "\n".join(rest)
                        break
            parts.append(t)
        return _normalize_whitespace("\n\n".join(parts))

    # Build (start_page, end_page, canonical, raw_headings, confidence) for each segment
    segments: list[tuple[int, int, str | None, list[str], float]] = []
    for i, h in enumerate(heading_hits):
        start = h.page_index
        end = len(page_texts) - 1
        if i + 1 < len(heading_hits):
            end = heading_hits[i + 1].page_index - 1
        end = max(start, end)
        canonical = h.canonical
        raw_headings = [h.raw_heading]
        confidence = h.confidence
        # Merge with previous segment if same canonical
        if segments and segments[-1][2] == canonical and canonical:
            prev_start, prev_end, _, prev_raw, prev_conf = segments[-1]
            segments[-1] = (
                prev_start,
                end,
                canonical,
                prev_raw + raw_headings,
                max(prev_conf, confidence),
            )
        else:
            segments.append((start, end, canonical, raw_headings, confidence))

    for start_page, end_page, canonical, raw_headings, confidence in segments:
        text = text_between(start_page, end_page, exclude_heading_line_on_start=True)
        sec = Section(
            canonical_name=canonical or "_unknown",
            raw_headings=raw_headings,
            text=text,
            start_page=start_page,
            end_page=end_page,
            confidence=confidence,
        )
        if canonical and confidence >= CONFIDENCE_THRESHOLD:
            known.append(sec)
        else:
            sec.canonical_name = raw_headings[0] if raw_headings else "_unknown"
            unknown.append(sec)

    return known, unknown


def extract_sections(pages: list[PageText]) -> tuple[list[Section], list[Section]]:
    """Pre-clean pages, detect heading hits, assemble sections. Returns (known, unknown)."""
    page_texts = _preclean_pages(pages)
    heading_hits = _detect_heading_hits(page_texts)
    return assemble_sections(page_texts, heading_hits)
