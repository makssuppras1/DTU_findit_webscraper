"""Deprecated. Use: python -m thesis_extractor run --config config.yaml"""

import sys
from pathlib import Path

_legacy = Path(__file__).resolve().parent
if str(_legacy) not in sys.path:
    sys.path.insert(0, str(_legacy))

from pdf_section_extractor.run_gcs import main

if __name__ == "__main__":
    main()
