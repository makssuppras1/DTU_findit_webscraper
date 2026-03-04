# Legacy / deprecated code

Deprecated in favor of the single-command extractor:

```bash
python -m thesis_extractor run --config config.yaml
```

- **run_pdf_gcs.py** — Old launcher for the GCS PDF pipeline. Use `thesis_extractor` instead.
- **pdf_section_extractor/** — Previous section extractor (different schema). Logic merged into `src/thesis_extractor/`.
- **old_src/** — Former `src/pipeline`, `storage`, `extraction`, `sectioning`, `utils`. Merged into `src/thesis_extractor/`.

Do not use for new work. Kept for reference and git history.
