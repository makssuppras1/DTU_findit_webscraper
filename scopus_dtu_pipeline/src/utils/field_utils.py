"""Safe extraction of nested fields from API responses."""
from __future__ import annotations

from typing import Any


def get_in(data: dict | list | None, *keys: str | int, default: Any = None) -> Any:
    """Get nested value: get_in(d, 'a', 'b', 0) -> d['a']['b'][0] or default."""
    if data is None:
        return default
    cur: Any = data
    for k in keys:
        try:
            if isinstance(cur, dict):
                cur = cur.get(k)
            else:
                cur = cur[k] if isinstance(cur, (list, tuple)) and -len(cur) <= k < len(cur) else None
        except (KeyError, IndexError, TypeError):
            return default
        if cur is None:
            return default
    return cur


def safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    s = str(val).strip()
    return s if s else default
