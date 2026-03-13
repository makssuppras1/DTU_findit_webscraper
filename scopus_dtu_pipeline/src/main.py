"""Orchestrate full pipeline: affiliation -> candidates -> profiles -> filter -> publications -> outputs."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src.api.scopus_client import ScopusClient
from src.config import get_paths, get_settings
from src.pipeline.build_outputs import run as build_outputs
from src.pipeline.discover_affiliation import run as discover_affiliation
from src.pipeline.fetch_author_profiles import run as fetch_author_profiles
from src.pipeline.fetch_candidates import run as fetch_candidates
from src.pipeline.fetch_publications import run as fetch_publications
from src.pipeline.filter_active_authors import run as filter_active_authors
from src.utils.logging_utils import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Scopus DTU pipeline: discover affiliation, fetch candidates, filter active, fetch publications.")
    parser.add_argument("--no-cache", action="store_true", help="Disable response cache")
    parser.add_argument("--from-checkpoint", type=str, default=None, help="Start from step: affiliation, candidates, profiles, filter, publications, outputs")
    parser.add_argument("--steps", type=str, default=None, help="Comma-separated steps to run (default: all)")
    args = parser.parse_args()

    paths = get_paths()
    paths["raw"].mkdir(parents=True, exist_ok=True)
    paths["processed"].mkdir(parents=True, exist_ok=True)
    paths["logs"].mkdir(parents=True, exist_ok=True)

    settings = get_settings()
    progress_interval = settings.get("logging", {}).get("progress_interval", 50)
    logger = setup_logging(paths["logs"], progress_interval=progress_interval)

    client = ScopusClient(use_cache=not args.no_cache)

    steps = (args.steps or "affiliation,candidates,profiles,filter,publications,outputs").strip().split(",")
    steps = [s.strip().lower() for s in steps if s.strip()]

    # Load or run up to each step
    aff = None
    if "affiliation" in steps:
        aff = discover_affiliation(client)
        if not aff:
            logger.error("Could not resolve DTU affiliation")
            sys.exit(1)
        logger.info("DTU affiliation ID: %s", aff.get("affiliation_id"))
    else:
        cp = paths["raw"] / "checkpoint_affiliation.json"
        if cp.exists():
            import json
            with open(cp, encoding="utf-8") as f:
                aff = json.load(f)
        if not aff:
            logger.error("Run affiliation step first or provide checkpoint")
            sys.exit(1)

    affiliation_id = aff.get("affiliation_id")
    if not affiliation_id:
        logger.error("Missing affiliation_id")
        sys.exit(1)

    candidates = []
    if "candidates" in steps:
        candidates = fetch_candidates(client, affiliation_id)
        logger.info("Candidates: %s", len(candidates))
    else:
        cp = paths["raw"] / "checkpoint_candidates.json"
        if cp.exists():
            import json
            with open(cp, encoding="utf-8") as f:
                data = json.load(f)
                candidates = data.get("candidates", [])
        if not candidates and "profiles" in steps:
            logger.error("No candidates; run candidates step first")
            sys.exit(1)

    profiles = []
    if "profiles" in steps:
        profiles = fetch_author_profiles(client, candidates, progress_interval=progress_interval)
        logger.info("Profiles: %s", len(profiles))
    else:
        cp = paths["raw"] / "checkpoint_author_profiles.json"
        if cp.exists():
            import json
            with open(cp, encoding="utf-8") as f:
                data = json.load(f)
                profiles = data.get("profiles", [])

    active = []
    if "filter" in steps:
        active = filter_active_authors(client, candidates, profiles, progress_interval=progress_interval)
        logger.info("Active researchers (last 5y): %s", len(active))
    else:
        cp = paths["raw"] / "checkpoint_active_researchers.json"
        if cp.exists():
            import json
            with open(cp, encoding="utf-8") as f:
                data = json.load(f)
                rows = [r for r in data.get("researchers", []) if r.get("active_last_5y")]
                from src.pipeline.filter_active_authors import _row_to_candidate
                active = [_row_to_candidate(r) for r in rows]
        if not active and "publications" in steps:
            logger.warning("No active researchers; skipping publications")

    publications = []
    if "publications" in steps and active:
        publications = fetch_publications(client, active, progress_interval=progress_interval)
        logger.info("Publications: %s", len(publications))
    else:
        cp = paths["raw"] / "checkpoint_publications.json"
        if cp.exists():
            import json
            with open(cp, encoding="utf-8") as f:
                data = json.load(f)
                from src.pipeline.fetch_publications import _row_to_publication
                for aid, rows in data.get("by_author", {}).items():
                    for r in rows:
                        publications.append(_row_to_publication(r))

    if "outputs" in steps:
        build_outputs(active, publications)
        logger.info("Outputs written to %s", paths["processed"])

    logger.info("Done. Researchers: %s, Publications: %s", len(active), len(publications))


if __name__ == "__main__":
    main()
