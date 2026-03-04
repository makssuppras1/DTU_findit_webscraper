"""Process N docs and print section coverage, OCR rate, avg chars, sections per doc; optionally one example JSON."""

import argparse
import json
import sys
from pathlib import Path

# Path bootstrap (prefer src over project root so "storage" is src/storage)
_SCRIPT_DIR = Path(__file__).resolve().parent
_SRC = _SCRIPT_DIR.parent
_ROOT = _SRC.parent
if _SRC.name == "src" and _SRC.exists():
    _src_str = str(_SRC)
    sys.path = [p for p in sys.path if p and Path(p).resolve() != _ROOT]
    if _src_str not in sys.path:
        sys.path.insert(0, _src_str)

from storage.gcs_io import list_pdfs, open_pdf_bytes
from pdf_section_extractor.pipeline import doc_id_from_blob_name, run_pipeline


def _truncate_text(obj, max_chars: int = 200):
    """In-place truncate 'text' and 'raw_headings' in section dicts for display."""
    if isinstance(obj, dict):
        if "text" in obj and isinstance(obj["text"], str) and len(obj["text"]) > max_chars:
            obj["text"] = obj["text"][:max_chars] + "..."
        for v in obj.values():
            _truncate_text(v, max_chars)
    elif isinstance(obj, list):
        for i in obj:
            _truncate_text(i, max_chars)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="thesis_archive_bucket")
    parser.add_argument("--prefix", default="dtu_findit/master_thesis/")
    parser.add_argument("-n", "--limit", type=int, default=5)
    parser.add_argument("--example-json", action="store_true", help="Print one example JSON (text truncated)")
    args = parser.parse_args()

    blobs = list(list_pdfs(args.bucket, args.prefix))[: args.limit]
    if not blobs:
        print("No PDFs found.")
        return

    section_counts: dict[str, int] = {}
    section_chars: dict[str, list[int]] = {}
    ocr_pages = 0
    total_pages = 0
    sections_per_doc: list[int] = []
    example_data = None

    for blob in blobs:
        try:
            pdf_bytes = open_pdf_bytes(blob.bucket, blob.name)
            data = run_pipeline(pdf_bytes, blob.name)
        except Exception as e:
            print(f"Skip {blob.name}: {e}")
            continue

        total_pages += data["page_count"]
        ocr_pages += data["extraction_stats"]["ocr_page_count"]
        sections_dict = data["sections"]
        unknown_list = data.get("unknown_sections", [])
        n_sections = len(sections_dict) + len(unknown_list)
        sections_per_doc.append(n_sections)

        for canonical, sec in sections_dict.items():
            section_counts[canonical] = section_counts.get(canonical, 0) + 1
            section_chars.setdefault(canonical, []).append(len(sec.get("text", "")))
        if unknown_list:
            section_counts["_unknown"] = section_counts.get("_unknown", 0) + 1
            for sec in unknown_list:
                section_chars.setdefault("_unknown", []).append(len(sec.get("text", "")))

        if example_data is None:
            example_data = data

    n_docs = len(blobs)
    print(f"Processed {n_docs} docs, {total_pages} pages total.")
    print(f"OCR page rate: {ocr_pages}/{total_pages} = {100 * ocr_pages / total_pages:.1f}%" if total_pages else "N/A")
    print(f"Sections per doc: min={min(sections_per_doc)}, max={max(sections_per_doc)}, avg={sum(sections_per_doc) / len(sections_per_doc):.1f}")

    print("\nSection coverage (% docs with this section):")
    for name in sorted(section_counts):
        pct = 100 * section_counts[name] / n_docs
        print(f"  {name}: {section_counts[name]}/{n_docs} ({pct:.0f}%)")

    print("\nAvg chars per section (when present):")
    for name in sorted(section_chars):
        vals = section_chars[name]
        print(f"  {name}: {sum(vals) / len(vals):.0f}")

    if args.example_json and example_data:
        print("\n--- Example JSON (text truncated to 200 chars) ---")
        out = dict(example_data)
        if "sections" in out:
            for sec in out["sections"].values():
                if len(sec.get("text", "")) > 200:
                    sec["text"] = sec["text"][:200] + "..."
        for sec in out.get("unknown_sections", []):
            if len(sec.get("text", "")) > 200:
                sec["text"] = sec["text"][:200] + "..."
        print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
