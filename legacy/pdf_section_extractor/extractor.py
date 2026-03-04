"""Extract text from PDF bytes: PyMuPDF first, OCR fallback per page."""

import logging
from io import BytesIO
from dataclasses import dataclass

import fitz
import pytesseract
from PIL import Image

log = logging.getLogger(__name__)

# Heuristics: use OCR if extracted text is too short or mostly non-printable
MIN_CHARS_FOR_PDF_TEXT = 50
MIN_PRINTABLE_RATIO = 0.5
OCR_DPI = 300


@dataclass
class PageText:
    page_index: int  # 0-based
    text: str
    provenance: str  # "pdf_text" | "ocr"
    char_count: int
    printable_ratio: float


def _printable_ratio(s: str) -> float:
    if not s:
        return 0.0
    printable = sum(1 for c in s if c.isprintable() or c in "\n\t")
    return printable / len(s)


def _page_text_from_fitz(page: fitz.Page) -> str:
    return page.get_text("text") or ""


def _needs_ocr(text: str) -> bool:
    """True if page should use OCR fallback."""
    if len(text.strip()) < MIN_CHARS_FOR_PDF_TEXT:
        return True
    return _printable_ratio(text) < MIN_PRINTABLE_RATIO


def _page_to_image(page: fitz.Page, dpi: int = OCR_DPI) -> bytes:
    """Render page to PNG bytes for OCR."""
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    return pix.tobytes("png")


def _ocr_page(image_bytes: bytes) -> str:
    """Run Tesseract on image bytes. Uses --oem 1 (LSTM) and --psm 6 (block of text)."""
    try:
        img = Image.open(BytesIO(image_bytes))
        return pytesseract.image_to_string(img, config="--oem 1 --psm 6")
    except Exception as e:
        log.warning("OCR failed: %s", e)
        return ""


def extract_pages(pdf_bytes: bytes) -> list[PageText]:
    """Extract text per page; use OCR for pages that fail heuristics."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    out: list[PageText] = []
    try:
        for i in range(len(doc)):
            page = doc[i]
            text = _page_text_from_fitz(page)
            char_count = len(text)
            printable_ratio = _printable_ratio(text)
            if _needs_ocr(text):
                image_bytes = _page_to_image(page)
                text = _ocr_page(image_bytes)
                provenance = "ocr"
                char_count = len(text)
                printable_ratio = _printable_ratio(text)
            else:
                provenance = "pdf_text"
            out.append(
                PageText(
                    page_index=i,
                    text=text,
                    provenance=provenance,
                    char_count=char_count,
                    printable_ratio=printable_ratio,
                )
            )
    finally:
        doc.close()
    return out
