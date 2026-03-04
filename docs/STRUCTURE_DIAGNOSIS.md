# Step 1 — Structural diagnosis

## Repo tree (relevant parts)

```
repo_root/
├── config.py                 # Env-based config (GCS_PREFIX, GCS_BUCKET) for scraper
├── storage.py                # GCS + progress for scraper (get_bucket, upload_to_gcs, progress.json)
├── main.py                   # Scraper CLI
├── scraper.py                # DTU Findit scraping (Selenium)
├── run_pdf_gcs.py            # Launcher → pdf_section_extractor
├── run_extractor.py          # Launcher → thesis_extractor
├── config.yaml               # YAML config for thesis extractor
├── src/
│   ├── storage/
│   │   ├── gcs_io.py         # list_pdfs, open_pdf_bytes, gcs_exists, upload_json
│   │   └── __init__.py
│   ├── pdf_section_extractor/
│   │   ├── extractor.py      # PyMuPDF + OCR per page, PageText
│   │   ├── sections.py       # Heading detection, normalization, assembly
│   │   ├── pipeline.py       # run_pipeline(pdf_bytes, blob) → JSON
│   │   ├── run_gcs.py        # CLI (--bucket, --prefix, --output, …)
│   │   ├── sample_report.py
│   │   └── __init__.py
│   └── thesis_extractor/
│       ├── config.py         # load_config(path) → Config dataclass from YAML
│       ├── pipeline.py       # Monolithic: GCS + extraction + sectioning + write
│       ├── cli.py            # run --config
│       ├── __main__.py
│       └── __init__.py
├── tests/
│   └── test_pdf_sections.py
└── ...
```

## Where things live

| Concern | Location | Notes |
|--------|----------|--------|
| **GCS (scraper)** | `storage.py` (root) | get_bucket(), upload_to_gcs(), progress, blob_exists by record_id |
| **GCS (extraction)** | `src/storage/gcs_io.py` | list_pdfs, open_pdf_bytes, gcs_exists, upload_json |
| **GCS (inline)** | `src/thesis_extractor/pipeline.py` | _gcs_client, _list_pdfs, _download_pdf, _gcs_blob_exists, _upload_json_gcs (duplicate) |
| **PDF text** | `pdf_section_extractor/extractor.py`, `thesis_extractor/pipeline.py` | Duplicated: get_text via PyMuPDF |
| **OCR** | Same two places | Tesseract fallback duplicated |
| **Sectioning** | `pdf_section_extractor/sections.py`, `thesis_extractor/pipeline.py` | Aliases, heading detection, assembly in both |
| **Scraping** | `main.py`, `scraper.py` | DTU Findit only; separate from extraction |
| **CLI** | `main.py`, `run_pdf_gcs.py`, `run_extractor.py`, `run_gcs.py`, `thesis_extractor/cli.py` | Multiple entrypoints |
| **Config** | `config.py` (root), `thesis_extractor/config.py`, `config.yaml`, defaults in run_gcs.py | Scattered |

## Architectural problems

1. **Mixed concerns**  
   `thesis_extractor/pipeline.py` does GCS I/O, PDF extraction, OCR, section detection, normalization, assembly, and batch orchestration in one file (~380 lines).

2. **Duplicate logic**  
   - GCS: `src/storage/gcs_io.py` exists but thesis_extractor reimplements list/download/exists/upload.  
   - Extraction: PyMuPDF + OCR logic in both `pdf_section_extractor/extractor.py` and `thesis_extractor/pipeline.py`.  
   - Sectioning: heading patterns, alias map, assembly in both `pdf_section_extractor/sections.py` and thesis_extractor inline.

3. **Hardcoded values**  
   Defaults like `thesis_archive_bucket`, `dtu_findit/master_thesis/` in `thesis_extractor/config.py` and in `run_gcs.py` / `sample_report.py`.

4. **Two pipelines**  
   - `pdf_section_extractor`: different output schema (normalized/unknown_sections with different field names), uses `storage.gcs_io`, has its own run_gcs CLI.  
   - `thesis_extractor`: YAML config, section-based JSON (source.bucket/blob, stats.runtime_sec), inline GCS.  
   No shared extraction/sectioning between them.

5. **Path hacks**  
   `run_pdf_gcs.py`, `run_extractor.py`, and `thesis_extractor/pipeline.py` mutate `sys.path` at import/run time so `storage` and packages resolve.

6. **No shared utilities**  
   Document ID hashing is inline in thesis_extractor; no shared logging or hashing module.

7. **Config split**  
   Scraper uses root `config.py` (env); thesis extractor uses YAML + `thesis_extractor/config.py`. No single place for extraction/sectioning constants.

## Non-goals (unchanged)

- **Scraper (main.py, storage.py, scraper.py)**: Leave as-is; no refactor of scraping or scraper-specific GCS/progress.
- **pdf_section_extractor**: Can remain for legacy CLI; refactor focuses on thesis_extractor path and new modular layout.

---

# Step 2–5 — Final structure (after refactor)

```
repo_root/
├── config.yaml
├── run_extractor.py              # Launcher: adds src to path, runs thesis_extractor.cli
├── scripts/
│   └── run_gcs_pipeline.py       # CLI: --config → pipeline.runner.run_pipeline
├── src/
│   ├── storage/
│   │   ├── __init__.py
│   │   └── gcs_io.py             # list_pdfs, open_pdf_bytes, gcs_exists, upload_json
│   ├── extraction/
│   │   ├── __init__.py
│   │   ├── pdf_text.py           # get_page_text (PyMuPDF)
│   │   ├── ocr.py                # page_to_png_bytes, run_ocr (Tesseract)
│   │   └── hybrid_extractor.py   # extract_pages(config, pdf_bytes)
│   ├── sectioning/
│   │   ├── __init__.py
│   │   ├── heading_detection.py  # HeadingHit, is_heading_line, detect_hits
│   │   ├── normalization.py     # build_alias_map, normalize_heading
│   │   └── section_splitter.py  # preclean, assemble_sections
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── config.py             # Config dataclasses, load_config(path)
│   │   └── runner.py             # run_pipeline(config) — uses storage, extraction, sectioning, utils
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── hashing.py            # doc_id(blob_name)
│   │   └── logging_utils.py      # setup_logging (optional file handler)
│   └── (thesis_extractor removed; use pipeline + run_extractor.py)
├── docs/
│   └── STRUCTURE_DIAGNOSIS.md
├── tests/
└── ... (main.py, storage.py, scraper.py, pdf_section_extractor unchanged)
```

- **CLI:** `python run_extractor.py run --config config.yaml` or `python scripts/run_gcs_pipeline.py --config config.yaml`. Both use the same pipeline.
- **No circular imports:** pipeline.runner → storage, extraction, sectioning, utils; extraction/sectioning do not import pipeline.
- **Config:** All extraction/sectioning/runtime/gcs/output in config.yaml; pipeline.config.load_config() only; defaults only when key missing.
- **GCS:** Single place for list/download/exists/upload: src/storage/gcs_io.py.
