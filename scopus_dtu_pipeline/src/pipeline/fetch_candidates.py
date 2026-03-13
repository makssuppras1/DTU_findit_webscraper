"""Step 2: Retrieve candidate authors by DTU affiliation ID, with pagination and checkpoint."""
from __future__ import annotations

import json
from pathlib import Path

from src.api.author_api import paginated_author_search, parse_candidate_from_search_entry
from src.api.scopus_client import ScopusClient
from src.config import get_paths
from src.utils.output_utils import write_json


def run(
    client: ScopusClient,
    affiliation_id: str,
    out_dir: Path | None = None,
    checkpoint_path: Path | None = None,
) -> list[dict]:
    """Fetch all candidate authors for affiliation_id. Saves checkpoint and raw list. Returns list of candidate dicts."""
    paths = get_paths()
    raw_dir = Path(out_dir or paths["raw"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = checkpoint_path or raw_dir / "checkpoint_candidates.json"

    candidates = []
    if checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            data = json.load(f)
            candidates = data.get("candidates", [])
            if candidates:
                write_json(raw_dir / "candidates_raw.json", data)
                return candidates

    for entry in paginated_author_search(client, affiliation_id):
        try:
            c = parse_candidate_from_search_entry(entry)
            if c.get("scopus_author_id"):
                candidates.append(c)
        except Exception:
            continue

    result = {"affiliation_id": affiliation_id, "candidates": candidates, "count": len(candidates)}
    write_json(raw_dir / "candidates_raw.json", result)
    write_json(checkpoint, result)
    return candidates
