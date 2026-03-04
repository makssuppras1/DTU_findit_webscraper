"""PDF text extraction with OCR fallback per page."""

import logging
from io import BytesIO

import fitz
import pytesseract
from PIL import Image

log = logging.getLogger(__name__)


def get_page_text(page: fitz.Page) -> str:
    return page.get_text("text") or ""


def page_to_png_bytes(page: fitz.Page, dpi: int = 300) -> bytes:
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    return pix.tobytes("png")


def run_ocr(image_bytes: bytes, lang: str = "eng") -> str:
    try:
        img = Image.open(BytesIO(image_bytes))
        return pytesseract.image_to_string(img, config=f"--oem 1 --psm 6 -l {lang}")
    except Exception as e:
        log.warning("OCR failed: %s", e)
        return ""


def _needs_ocr(text: str, min_chars: int) -> bool:
    if len(text.strip()) < min_chars:
        return True
    printable = sum(1 for c in text if c.isprintable() or c in "\n\t")
    return printable / len(text) < 0.5 if text else True


def extract_pages(config, pdf_bytes: bytes) -> list[tuple[int, str, str]]:
    """Return list of (page_index, text, provenance)."""
    ext = config.extraction
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    out: list[tuple[int, str, str]] = []
    try:
        n = len(doc)
        if ext.max_pages and ext.max_pages > 0:
            n = min(n, ext.max_pages)
        for i in range(n):
            page = doc[i]
            text = get_page_text(page)
            prov = "pdf_text"
            if ext.ocr_fallback and _needs_ocr(text, ext.ocr_min_chars):
                img_bytes = page_to_png_bytes(page, ext.ocr_dpi)
                text = run_ocr(img_bytes, ext.ocr_lang)
                prov = "ocr"
            out.append((i, text, prov))
    finally:
        doc.close()
    return out
