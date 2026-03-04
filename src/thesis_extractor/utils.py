"""Hashing and logging."""

import hashlib
import logging
from pathlib import Path


def doc_id(blob_name: str) -> str:
    return hashlib.sha1(blob_name.encode("utf-8")).hexdigest()


def setup_logging(log_file: str | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logging.getLogger().addHandler(fh)
