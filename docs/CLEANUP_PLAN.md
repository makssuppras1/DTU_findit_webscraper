# Cleanup plan — Step 1 inventory

## Repo tree (relevant)

```
repo_root/
├── config.yaml
├── config.py              # env (scraper)
├── run_extractor.py      # entrypoint A
├── run_pdf_gcs.py        # entrypoint B (legacy)
├── scripts/
│   └── run_gcs_pipeline.py  # entrypoint C
├── main.py               # scraper CLI
├── storage.py            # scraper GCS
├── scraper.py, metadata.py, merge_thesis_meta.py
├── src/
│   ├── storage/gcs_io.py
│   ├── extraction/       # pdf_text, ocr, hybrid_extractor
│   ├── sectioning/       # heading_detection, normalization, section_splitter
│   ├── pipeline/         # config, runner
│   ├── utils/            # hashing, logging_utils
│   └── pdf_section_extractor/  # legacy pipeline + run_gcs, sample_report
├── tests/
└── test_*.py
```

## Entrypoints

| File | Purpose |
|------|--------|
| run_extractor.py | Extraction (config-driven) |
| scripts/run_gcs_pipeline.py | Same extraction, different script |
| run_pdf_gcs.py | Launcher → pdf_section_extractor |
| main.py | Scraper (DTU Findit → GCS) |
| pdf_section_extractor/run_gcs.py | Legacy extraction CLI |
| pdf_section_extractor/sample_report.py | Report script |

## Duplicates / overlaps

- **Extraction pipelines:** `src/pipeline/runner.py` (config-driven) and `src/pdf_section_extractor/pipeline.py` (different schema).
- **GCS:** `src/storage/gcs_io.py` (extractor) vs root `storage.py` (scraper only) — separate concerns, keep both but scraper stays at root.
- **Config:** `config.yaml` (extractor) and root `config.py` (env for scraper) — keep both; extractor uses only YAML.

## Technical debt

- Hardcoded defaults in `pipeline/config.py` (bucket/prefix fallbacks).
- Two extraction entrypoints (run_extractor.py, scripts/run_gcs_pipeline.py).
- pdf_section_extractor and run_pdf_gcs unused for primary flow.

## Keep / merge / delete

| Item | Action |
|------|--------|
| config.yaml | KEEP (canonical extractor config) |
| config.example.yaml | ADD (optional, from config.yaml) |
| run_extractor.py | DELETE (use python -m thesis_extractor) |
| scripts/run_gcs_pipeline.py | DELETE |
| run_pdf_gcs.py | MOVE to legacy/ |
| src/pdf_section_extractor/ | MOVE to legacy/ |
| src/pipeline/, extraction/, sectioning/, utils/, storage/ | MERGE into src/thesis_extractor/ |
| main.py, storage.py, scraper.py, config.py | KEEP (scraper; isolate from extractor) |
| tests/test_pdf_sections.py | KEEP; update imports to thesis_extractor |

Target: one package `src/thesis_extractor/`, one command `python -m thesis_extractor run --config config.yaml`.
