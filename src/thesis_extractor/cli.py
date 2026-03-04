"""Minimal CLI: run --config path [--limit N]."""

import argparse

from .config import load_config
from .pipeline import run_pipeline


def main() -> None:
    p = argparse.ArgumentParser(description="Thesis section extractor: GCS PDFs → section JSON")
    sub = p.add_subparsers(dest="cmd", required=True)
    run_p = sub.add_parser("run")
    run_p.add_argument("--config", "-c", default="config.yaml", help="Path to config YAML")
    run_p.add_argument("--limit", "-n", type=int, default=None, help="Override config limit (0 = no limit)")
    args = p.parse_args()
    if args.cmd != "run":
        p.error("Use: run --config config.yaml [--limit N]")
    config = load_config(args.config)
    if getattr(args, "limit", None) is not None:
        config.runtime.limit = args.limit
    run_pipeline(config)


if __name__ == "__main__":
    main()
