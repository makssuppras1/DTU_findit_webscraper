"""Write JSON, CSV, and Parquet outputs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def write_json(path: Path | str, data: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def write_csv(path: Path | str, rows: list[dict], columns: list[str] | None = None) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if columns:
        df = df[[c for c in columns if c in df.columns]]
    df.to_csv(path, index=False, encoding="utf-8")


def write_parquet(path: Path | str, rows: list[dict], columns: list[str] | None = None) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if columns:
        df = df[[c for c in columns if c in df.columns]]
    df.to_parquet(path, index=False)


def write_researcher_table(path: Path | str, researchers: list[Any]) -> None:
    """Write list of ResearcherCandidate to CSV (using to_row)."""
    rows = [r.to_row() if hasattr(r, "to_row") else r for r in researchers]
    write_csv(path, rows)


def write_publication_table(path: Path | str, publications: list[Any]) -> None:
    """Write list of PublicationRecord to CSV (using to_row)."""
    rows = [p.to_row() if hasattr(p, "to_row") else p for p in publications]
    write_csv(path, rows)
