"""Load settings from .env and config/settings.yaml."""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

# Project root: directory containing config/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"


def _load_yaml() -> dict:
    settings_path = CONFIG_DIR / "settings.yaml"
    if not settings_path.exists():
        return {}
    with open(settings_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_env(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key, default)


def get_scopus_api_key() -> str:
    key = get_env("SCOPUS_API_KEY")
    if not key:
        raise ValueError("SCOPUS_API_KEY must be set in .env")
    return key


def get_scopus_inst_token() -> str | None:
    return get_env("SCOPUS_INST_TOKEN")


def get_settings() -> dict:
    return _load_yaml()


def get_scopus_base_url() -> str:
    s = get_settings()
    return s.get("scopus", {}).get("base_url", "https://api.elsevier.com/content")


def get_scopus_endpoints() -> dict[str, str]:
    s = get_settings()
    base = get_scopus_base_url()
    sc = s.get("scopus", {})
    return {
        "affiliation_search": base.rstrip("/") + sc.get("affiliation_search", "/search/affiliation"),
        "author_search": base.rstrip("/") + sc.get("author_search", "/search/author"),
        "author_retrieval": base.rstrip("/") + sc.get("author_retrieval", "/author/author_id"),
        "scopus_search": base.rstrip("/") + sc.get("scopus_search", "/search/scopus"),
        "abstract_retrieval": base.rstrip("/") + sc.get("abstract_retrieval", "/abstract/scopus_id"),
    }


def get_retry_config() -> dict:
    s = get_settings()
    r = s.get("retry", {})
    return {
        "max_attempts": r.get("max_attempts", 5),
        "base_delay": r.get("base_delay_seconds", 2),
        "max_delay": r.get("max_delay_seconds", 60),
    }


def get_rate_limit_rps() -> float:
    s = get_settings()
    return float(s.get("rate_limit", {}).get("requests_per_second", 2))


def get_cache_config() -> dict:
    s = get_settings()
    c = s.get("cache", {})
    cache_dir = c.get("dir", "data/raw/cache")
    path = Path(cache_dir)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return {
        "enabled": c.get("enabled", True),
        "dir": str(path),
        "ttl_hours": c.get("ttl_hours", 168),
    }


def get_pipeline_config() -> dict:
    s = get_settings()
    p = s.get("pipeline", {})
    return {
        "active_years": p.get("active_years", 5),
        "page_size": p.get("page_size", 25),
        "qualifying_doctypes": p.get("qualifying_doctypes", ["article", "conference paper", "review", "book chapter"]),
        "excluded_doctypes": p.get("excluded_doctypes", ["editorial", "note", "erratum", "letter"]),
    }


def get_paths() -> dict[str, Path]:
    s = get_settings()
    paths = s.get("paths", {})
    raw = Path(paths.get("raw", "data/raw"))
    processed = Path(paths.get("processed", "data/processed"))
    logs = Path(paths.get("logs", "data/logs"))
    if not raw.is_absolute():
        raw = PROJECT_ROOT / raw
    if not processed.is_absolute():
        processed = PROJECT_ROOT / processed
    if not logs.is_absolute():
        logs = PROJECT_ROOT / logs
    return {"raw": raw, "processed": processed, "logs": logs}
