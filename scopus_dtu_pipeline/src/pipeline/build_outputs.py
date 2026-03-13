"""Step 6: Write clean researcher and publication tables plus raw audit files."""
from __future__ import annotations

from pathlib import Path

from src.config import get_paths
from src.models.publication import PublicationRecord
from src.models.researcher import ResearcherCandidate
from src.utils.output_utils import write_csv, write_json, write_researcher_table, write_publication_table


def run(
    researchers: list[ResearcherCandidate],
    publications: list[PublicationRecord],
    out_dir: Path | None = None,
) -> None:
    """Write processed CSV/parquet and summary JSON to processed/."""
    paths = get_paths()
    processed_dir = Path(out_dir or paths["processed"])
    processed_dir.mkdir(parents=True, exist_ok=True)

    write_researcher_table(processed_dir / "researchers.csv", researchers)
    write_publication_table(processed_dir / "publications.csv", publications)
    write_json(processed_dir / "summary.json", {
        "researcher_count": len(researchers),
        "publication_count": len(publications),
    })
