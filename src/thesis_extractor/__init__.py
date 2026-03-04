"""Thesis section extractor: GCS PDFs → section-based JSON. One command: python -m thesis_extractor run --config config.yaml"""

from .pipeline import run_pipeline
from .cli import main

__all__ = ["run_pipeline", "main"]
