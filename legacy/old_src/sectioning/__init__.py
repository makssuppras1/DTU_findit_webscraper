"""Section detection, normalization, and assembly."""

from .heading_detection import HeadingHit, detect_hits
from .normalization import build_alias_map, normalize_heading
from .section_splitter import assemble_sections, preclean

__all__ = [
    "HeadingHit",
    "detect_hits",
    "build_alias_map",
    "normalize_heading",
    "assemble_sections",
    "preclean",
]
