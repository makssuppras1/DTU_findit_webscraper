"""Load and validate YAML config. No hardcoded bucket/prefix; defaults only in load_config."""

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class GcsConfig:
    bucket: str
    prefix: str


@dataclass
class OutputConfig:
    target: str  # "local" | "gcs"
    local_dir: str
    gcs_bucket: str
    gcs_prefix: str


@dataclass
class RuntimeConfig:
    workers: int
    limit: int
    resume: bool
    log_file: str


@dataclass
class ExtractionConfig:
    prefer_pdf_text: bool
    ocr_fallback: bool
    ocr_dpi: int
    ocr_lang: str
    ocr_min_chars: int
    max_pages: int


@dataclass
class SectioningConfig:
    canonical_sections: list[str]
    heading_aliases: dict[str, list[str]]
    fuzzy_match: bool
    fuzzy_threshold: int


@dataclass
class Config:
    gcs: GcsConfig
    output: OutputConfig
    runtime: RuntimeConfig
    extraction: ExtractionConfig
    sectioning: SectioningConfig


def load_config(path: str | Path) -> Config:
    """Load config from YAML file. Uses defaults only when key is missing."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    g = data.get("gcs") or {}
    o = data.get("output") or {}
    r = data.get("runtime") or {}
    e = data.get("extraction") or {}
    s = data.get("sectioning") or {}
    return Config(
        gcs=GcsConfig(
            bucket=g.get("bucket", "thesis_archive_bucket"),
            prefix=g.get("prefix", "dtu_findit/master_thesis/"),
        ),
        output=OutputConfig(
            target=o.get("target", "local"),
            local_dir=o.get("local_dir", "data/sections_json"),
            gcs_bucket=o.get("gcs_bucket", "thesis_archive_bucket"),
            gcs_prefix=o.get("gcs_prefix", "dtu_findit/master_thesis_sections_json/"),
        ),
        runtime=RuntimeConfig(
            workers=r.get("workers", 1),
            limit=r.get("limit", 0),
            resume=r.get("resume", True),
            log_file=r.get("log_file", "logs/extract.log"),
        ),
        extraction=ExtractionConfig(
            prefer_pdf_text=e.get("prefer_pdf_text", True),
            ocr_fallback=e.get("ocr_fallback", True),
            ocr_dpi=e.get("ocr_dpi", 300),
            ocr_lang=e.get("ocr_lang", "eng"),
            ocr_min_chars=e.get("ocr_min_chars", 50),
            max_pages=e.get("max_pages", 0),
        ),
        sectioning=SectioningConfig(
            canonical_sections=s.get("canonical_sections", []),
            heading_aliases=s.get("heading_aliases", {}),
            fuzzy_match=s.get("fuzzy_match", True),
            fuzzy_threshold=s.get("fuzzy_threshold", 85),
        ),
    )
