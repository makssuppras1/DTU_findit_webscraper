"""Structured logging setup."""
from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logging(
    log_dir: Path | str,
    name: str = "scopus_pipeline",
    level: int = logging.INFO,
    progress_interval: int = 50,
) -> logging.Logger:
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    run_log = log_path / "pipeline_run.log"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    fh = logging.FileHandler(run_log, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def log_progress(logger: logging.Logger, current: int, total: int, message: str = "Processed") -> None:
    if total <= 0 or current % max(1, getattr(logger, "_progress_interval", 50)) == 0 or current == total:
        logger.info("%s %s / %s", message, current, total)
