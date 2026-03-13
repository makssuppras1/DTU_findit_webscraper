"""Reusable Scopus API client with retry, rate limit, and optional caching."""
from __future__ import annotations

import time
from typing import Any

import requests

from src.config import (
    get_cache_config,
    get_retry_config,
    get_scopus_api_key,
    get_scopus_inst_token,
    get_rate_limit_rps,
)
from src.utils.cache import get_cached, set_cached
from src.utils.retry import with_retry

# Header required by Elsevier
API_KEY_HEADER = "X-ELS-APIKey"
INST_TOKEN_HEADER = "X-ELS-Insttoken"
ACCEPT_HEADER = "Accept"
ACCEPT_JSON = "application/json"
# Browser-like headers so Cloudflare is less likely to block (403 with HTML challenge)
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
BROWSER_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://dev.elsevier.com/",
    "Accept-Encoding": "gzip, deflate, br",
}


class ScopusClientError(Exception):
    """Raised when the API returns an error or unexpected status."""

    def __init__(self, message: str, status_code: int | None = None, response_text: str | None = None):
        self.status_code = status_code
        self.response_text = response_text
        super().__init__(message)


class ScopusClient:
    def __init__(
        self,
        api_key: str | None = None,
        inst_token: str | None = None,
        use_cache: bool = True,
        rate_limit_rps: float | None = None,
    ):
        self.api_key = api_key or get_scopus_api_key()
        self.inst_token = inst_token or get_scopus_inst_token()
        cache_cfg = get_cache_config()
        self.use_cache = use_cache and cache_cfg.get("enabled", True)
        self.cache_dir = cache_cfg.get("dir", "data/raw/cache")
        self.cache_ttl_hours = cache_cfg.get("ttl_hours", 168)
        self.rate_limit_rps = rate_limit_rps if rate_limit_rps is not None else get_rate_limit_rps()
        self._last_request_time: float = 0.0
        self._retry_cfg = get_retry_config()

    def _headers(self) -> dict[str, str]:
        h = {
            API_KEY_HEADER: self.api_key,
            ACCEPT_HEADER: ACCEPT_JSON,
            "User-Agent": USER_AGENT,
            **BROWSER_HEADERS,
        }
        if self.inst_token:
            h[INST_TOKEN_HEADER] = self.inst_token
        return h

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_time
        min_interval = 1.0 / self.rate_limit_rps
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

    def get(self, url: str, params: dict[str, Any] | None = None, use_cache: bool | None = None) -> dict | list:
        """GET request with retry, rate limit, and optional cache."""
        params = dict(params or {})
        params.setdefault("apiKey", self.api_key)
        if self.inst_token and "insttoken" not in params:
            params["insttoken"] = self.inst_token
        cache = use_cache if use_cache is not None else self.use_cache

        if cache:
            cached = get_cached(self.cache_dir, "GET", url, params, self.cache_ttl_hours)
            if cached is not None:
                return cached

        def _do_request() -> dict | list:
            self._rate_limit()
            resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
            if resp.status_code == 429:
                # Back off before retry to avoid key lock; then raise so with_retry will retry later
                time.sleep(60)
                raise ScopusClientError("Rate limit exceeded (429)", status_code=429, response_text=resp.text)
            if resp.status_code == 401:
                raise ScopusClientError("Invalid API key (401)", status_code=401, response_text=resp.text)
            if resp.status_code == 403:
                msg = f"Forbidden (403). Response: {resp.text[:800]}" if resp.text else "Forbidden (403)"
                raise ScopusClientError(msg, status_code=403, response_text=resp.text)
            if resp.status_code != 200:
                raise ScopusClientError(
                    f"HTTP {resp.status_code}: {resp.text[:500]}",
                    status_code=resp.status_code,
                    response_text=resp.text,
                )
            out = resp.json()
            if cache:
                set_cached(self.cache_dir, "GET", url, params, out)
            return out

        return with_retry(
            _do_request,
            max_attempts=self._retry_cfg["max_attempts"],
            base_delay=self._retry_cfg["base_delay"],
            max_delay=self._retry_cfg["max_delay"],
            retryable_exceptions=(requests.RequestException, ScopusClientError),
        )
