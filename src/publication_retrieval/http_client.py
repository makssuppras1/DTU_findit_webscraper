"""HTTP client with retry and backoff for API calls."""

import time
import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "DTU-Publication-Retrieval/1.0 (mailto:research@dtu.dk)",
}


def get_with_retry(
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    max_retries: int = 3,
    backoff_base: float = 1.0,
) -> tuple[requests.Response | None, str | None]:
    """GET with exponential backoff. Returns (response, None) on success, (None, error_msg) on failure."""
    h = {**DEFAULT_HEADERS, **(headers or {})}
    last_err: str | None = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=h, timeout=30)
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else backoff_base * (2**attempt)
                log.warning("Rate limited; waiting %s s", wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500 and attempt < max_retries - 1:
                time.sleep(backoff_base * (2**attempt))
                continue
            return (r, None)
        except requests.RequestException as e:
            last_err = str(e)
            if attempt < max_retries - 1:
                time.sleep(backoff_base * (2**attempt))
    return (None, last_err or "max retries exceeded")
