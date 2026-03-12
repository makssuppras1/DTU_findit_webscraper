"""Run publication retrieval CLI. Usage: uv run python run_publication_retrieval.py [--limit 1] [args]"""

import sys
from pathlib import Path

# Ensure src is on path when run from project root
src = Path(__file__).resolve().parent / "src"
if src.exists() and str(src) not in sys.path:
    sys.path.insert(0, str(src))

from publication_retrieval.cli import main

if __name__ == "__main__":
    main()
