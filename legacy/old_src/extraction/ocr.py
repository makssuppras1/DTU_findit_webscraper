"""Tesseract OCR fallback for pages with poor or no embedded text."""

import logging
from io import BytesIO

import fitz
import pytesseract
from PIL import Image

log = logging.getLogger(__name__)


def page_to_png_bytes(page: fitz.Page, dpi: int = 300) -> bytes:
    """Render page to PNG bytes for OCR."""
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    return pix.tobytes("png")


def run_ocr(image_bytes: bytes, lang: str = "eng") -> str:
    """Run Tesseract on image bytes. Returns extracted text."""
    try:
        img = Image.open(BytesIO(image_bytes))
        return pytesseract.image_to_string(img, config=f"--oem 1 --psm 6 -l {lang}")
    except Exception as e:
        log.warning("OCR failed: %s", e)
        return ""
