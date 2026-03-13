"""Step 3: For each candidate, call Author Retrieval API and save raw responses."""
from __future__ import annotations

import json
import logging
from pathlib import Path

from src.api.author_api import get_author_profile
from src.api.scopus_client import ScopusClient
from src.config import get_paths
from src.utils.field_utils import get_in
from src.utils.logging_utils import log_progress
from src.utils.output_utils import write_json as write_json_file

logger = logging.getLogger("scopus_pipeline")


def run(
    client: ScopusClient,
    candidates: list[dict],
    out_dir: Path | None = None,
    checkpoint_path: Path | None = None,
    progress_interval: int = 50,
) -> list[dict]:
    """Fetch author profile for each candidate. Saves raw JSON per author and checkpoint. Returns list of profile dicts."""
    paths = get_paths()
    raw_dir = Path(out_dir or paths["raw"])
    profiles_dir = raw_dir / "author_profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = checkpoint_path or raw_dir / "checkpoint_author_profiles.json"

    processed: list[dict] = []
    failed_ids: list[str] = []
    if checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            data = json.load(f)
            processed = data.get("profiles", [])
            failed_ids = data.get("failed_ids", [])
            already = {p.get("scopus_author_id") for p in processed if p.get("scopus_author_id")}
    else:
        already = set()

    for i, cand in enumerate(candidates):
        aid = cand.get("scopus_author_id") or cand.get("author_id")
        if not aid or str(aid) in already:
            continue
        log_progress(logger, len(processed) + len(failed_ids) + 1, len(candidates), "Author profiles")
        try:
            profile = get_author_profile(client, str(aid))
            if not profile:
                failed_ids.append(str(aid))
                logger.warning("No profile for author %s", aid)
                continue
            raw_path = profiles_dir / f"author_{aid}.json"
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump(profile, f, indent=2, ensure_ascii=False)
            preferred = get_in(profile, "author-profile", "preferred-name") or {}
            aff_history = get_in(profile, "author-profile", "affiliation-history", "affiliation") or []
            if aff_history and not isinstance(aff_history, list):
                aff_history = [aff_history]
            current_aff = get_in(profile, "author-profile", "affiliation-current", "affiliation", "afdispname") or None
            processed.append({
                "scopus_author_id": aid,
                "preferred_name": get_in(preferred, "ce:indexed-name") or get_in(preferred, "given-name", "surname"),
                "orcid": get_in(profile, "author-profile", "orcid") or None,
                "affiliation_history": aff_history,
                "current_affiliation": current_aff,
                "document_count": get_in(profile, "author-profile", "document-count") or 0,
                "subject_areas": get_in(profile, "author-profile", "subject-area") or None,
                "h_index": get_in(profile, "author-profile", "h-index") or None,
                "raw_profile": profile,
            })
            already.add(str(aid))
        except Exception as e:
            logger.warning("Failed author %s: %s", aid, e)
            failed_ids.append(str(aid))

    result = {"profiles": processed, "failed_ids": failed_ids, "count": len(processed)}
    write_json_file(checkpoint, result)
    return processed
