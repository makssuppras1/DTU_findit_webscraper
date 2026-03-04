"""Structured logging setup (no side effects at import)."""

import logging
from pathlib import Path


def setup_logging(log_file: str | None = None, level: int = logging.INFO) -> None:
    """Configure root logger; optionally add file handler."""
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logging.getLogger().addHandler(fh)
