"""Direct PDF text extraction via PyMuPDF."""

import fitz


def get_page_text(page: fitz.Page) -> str:
    """Extract text from a single page."""
    return page.get_text("text") or ""
