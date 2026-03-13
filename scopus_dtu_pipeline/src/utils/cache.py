"""Simple file-based cache for API responses."""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path


def _cache_key(method: str, url: str, params: dict | None) -> str:
    raw = f"{method}:{url}:{json.dumps(params or {}, sort_keys=True)}"
    return hashlib.sha256(raw.encode()).hexdigest()


def get_cached(
    cache_dir: str,
    method: str,
    url: str,
    params: dict | None,
    ttl_hours: float,
) -> dict | list | None:
    path = Path(cache_dir)
    path.mkdir(parents=True, exist_ok=True)
    key = _cache_key(method, url, params)
    filepath = path / f"{key}.json"
    if not filepath.exists():
        return None
    if ttl_hours > 0 and (time.time() - filepath.stat().st_mtime) > (ttl_hours * 3600):
        return None
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def set_cached(cache_dir: str, method: str, url: str, params: dict | None, data: dict | list) -> None:
    path = Path(cache_dir)
    path.mkdir(parents=True, exist_ok=True)
    key = _cache_key(method, url, params)
    filepath = path / f"{key}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
