"""Step 1: Resolve DTU affiliation ID and save metadata."""
from __future__ import annotations

import json
from pathlib import Path

from src.api.affiliation_api import find_dtu_affiliation, get_affiliation_id
from src.api.scopus_client import ScopusClient
from src.config import get_paths
from src.utils.output_utils import write_json


def run(
    client: ScopusClient,
    name_query: str = "Technical University of Denmark",
    out_dir: Path | None = None,
    checkpoint_path: Path | None = None,
) -> dict | None:
    """Discover DTU affiliation, save raw metadata and checkpoint. Returns affiliation info dict or None."""
    paths = get_paths()
    raw_dir = out_dir or paths["raw"]
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = checkpoint_path or raw_dir / "checkpoint_affiliation.json"

    if checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            data = json.load(f)
            return data

    entry = find_dtu_affiliation(client, name_query)
    if not entry:
        return None

    aff_id = get_affiliation_id(entry)
    result = {
        "affiliation_id": aff_id,
        "name_query": name_query,
        "raw_entry": entry,
    }
    write_json(raw_dir / "dtu_affiliation_metadata.json", result)
    write_json(checkpoint, result)
    return result
