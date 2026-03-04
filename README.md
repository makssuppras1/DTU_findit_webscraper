# DTU Findit Master Thesis PDF Pipeline

Enumerates all Master's thesis records on DTU Findit, downloads PDFs via the site's Download button, and uploads them to Google Cloud Storage. Resumable, rate-limited, and suitable for ~20,000 PDFs.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GCS_BUCKET` | Yes | GCS bucket name for storing PDFs |
| `GCS_PREFIX` | No | Object key prefix (default: `dtu_findit/master_thesis`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes* | Path to GCP service account JSON key |
| `MAX_RECORDS` | No | Limit records (e.g. `10` for PoC); unset = all |
| `CHROME_PROFILE_DIR` | No | Path to persist browser session (e.g. `./chrome_profile`); log in once, reuse on next runs |
| `DTU_USERNAME` | No | DTU login (for auto-fill); **avoid storing in scripts** |
| `DTU_PASSWORD` | No | DTU password (for auto-fill); **security risk** – use only in env, never commit |

\* Or use `gcloud auth application-default login` for local runs.

### Using a .env file

Copy the example and edit:

```bash
cp .env.example .env
# Edit .env with your values
```

The script loads `.env` automatically. `.env` is gitignored; never commit it.

### Avoiding manual login

**Option 1 – Session persistence (recommended, fastest):** Use a Chrome profile so the browser remembers your session. Log in once; subsequent runs skip the entire login flow:
```bash
export CHROME_PROFILE_DIR=./chrome_profile
uv run python main.py
```
Log in manually the first time. Future runs reuse the saved session. Add `chrome_profile/` to `.gitignore` (already done).

**Test login only:** Run `uv run python test_login.py` to test the login flow in isolation. Browser stays open until you press Enter.

**Option 2 – Auto-fill credentials:** Set `DTU_USERNAME` and `DTU_PASSWORD` in `.env`. The script will:
1. Click "I am not a robot"
2. Click "Allow selected" on the cookie pop-up
3. Click "Log in as DTU user"
4. Fill username/password and submit

This may not work if DTU uses 2FA (MitID) or changes the form. **Do not store passwords in code or config files.**

`DTU_USERNAME` is usually your DTU email or student/employee ID (e.g. `s123456`).

### GCS not working?

1. **"Could not automatically determine credentials"**  
   Set `GOOGLE_APPLICATION_CREDENTIALS` to the path of your service account JSON:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your-service-account.json
   ```
   Or run `gcloud auth application-default login` (uses your user account).

2. **"403 Forbidden" / "Permission denied"**  
   Your service account needs `Storage Object Admin` or `Storage Object Creator` on the bucket. In GCP Console: Bucket → Permissions → Add principal → add your service account.

3. **"404 Not Found"**  
   Check the bucket name (e.g. `thesis_archive_bucket`) and that it exists in your project.

## How to Run

1. Create a GCS bucket and ensure you have write access.
2. Set credentials:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   export GCS_BUCKET=your-bucket-name
   # For PoC, limit to 10 PDFs:
   export MAX_RECORDS=10
   ```
3. Run the script:
   ```bash
   uv run python main.py
   ```
4. Complete verification ("I am not a robot") and DTU login in the opened browser.
5. Press Enter when the thesis results list is visible.
6. The script will paginate through all records, download PDFs, and upload to GCS.

## How to Resume

Re-run the same command. The script:

- Loads `progress.json` (processed record IDs)
- Checks GCS for existing blobs before downloading
- Skips records already uploaded
- Continues from where it left off

To start fresh, delete `progress.json`.

## GCS Output Layout

```
gs://<bucket>/<GCS_PREFIX>/<record_id>_<thesis_title>.pdf
```

Example: `gs://my-bucket/dtu_findit/master_thesis/697c04de6e5c71c3975b93f4_Towards Early Detection of Children.pdf`

The thesis title is sanitized for filenames (special chars removed, max 120 chars). The record_id prefix ensures uniqueness.

## DTU Findit Selectors

Downloads are triggered from the **results page** (no navigation to detail pages). For each result row, the script finds the Download link within that row and clicks it. If the click opens a new tab, the script switches to it, waits for the PDF download, closes the tab, and continues.

**Download link** (within each result row): `find_download_in_element()` looks for links with text "Download", `href` containing `.pdf` or `fulltext-gateway`, or class containing "download".

**Result row**: Ancestor of the catalog link with class containing `document`, `result`, `item`, or `row`.

Record IDs are extracted from URLs: `/en/catalog/<record_id>`.

Pagination uses the `start` query parameter: `start=0`, `start=20`, etc.

---

## Thesis section extractor

Reads PDFs from GCS, extracts text (PDF first, OCR fallback when needed), detects section headings from config-driven aliases, and writes one section-based JSON per PDF.

### Install

From repo root:

```bash
uv sync
uv pip install -e .
```

### Run (single command)

```bash
python -m thesis_extractor run --config config.yaml
```

Optional: `--limit N` to process only the first N PDFs (overrides `config.yaml`).

### Config

Copy `config.example.yaml` to `config.yaml` and edit. Keys:

- **gcs**: `bucket`, `prefix` (where to list PDFs)
- **output**: `target` (`local` or `gcs`), `local_dir` or `gcs_bucket`/`gcs_prefix`
- **runtime**: `workers`, `limit` (0 = no limit), `resume`, `log_file`
- **extraction**: `ocr_min_chars`, `ocr_dpi`, `ocr_lang`, `max_pages`, etc.
- **sectioning**: `canonical_sections`, `heading_aliases`, `fuzzy_match`, `fuzzy_threshold`

### Dependencies

- **GCS:** `GOOGLE_APPLICATION_CREDENTIALS` or `gcloud auth application-default login`
- **Tesseract:** Required for OCR fallback — install system-wide (e.g. `brew install tesseract` on macOS)

### Output

One JSON per PDF: `doc_id`, `source`, `page_count`, `stats` (ocr_pages, runtime_sec), `sections` (canonical → text, start_page, end_page, headings), `unknown_sections`. Resume: existing output files are skipped.

### Repo layout

- `config.yaml` — extractor config (use `config.example.yaml` as template)
- `src/thesis_extractor/` — extractor package (config, cli, pipeline, gcs_io, extract, sectioning, utils)
- Scraper (DTU Findit → GCS): `main.py`, `storage.py`, `scraper.py`, root `config.py`
- Deprecated code: `legacy/` (see `legacy/README.md`)

### Tests

```bash
uv pip install -e .
python -m unittest tests.test_thesis_extractor -v
```
