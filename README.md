# DTU Findit Master Thesis PDF Pipeline

Enumerates all Master's thesis records on DTU Findit, downloads PDFs via the site's Download button, and uploads them to Google Cloud Storage. Resumable, rate-limited, and suitable for ~20,000 PDFs.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GCS_BUCKET` | Yes | GCS bucket name for storing PDFs |
| `GCS_PREFIX` | No | Object key prefix (default: `dtu_findit/master_thesis`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes* | Path to GCP service account JSON key |
| `MAX_RECORDS` | No | Limit records (e.g. `10` for PoC); unset = all |

\* Or use `gcloud auth application-default login` for local runs.

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
gs://<bucket>/<GCS_PREFIX>/<record_id>.pdf
```

Example: `gs://my-bucket/dtu_findit/master_thesis/697c04de6e5c71c3975b93f4.pdf`

## DTU Findit Selectors

The Download button is located using these strategies (in order). Update `find_download_button()` in `main.py` if the UI changes:

1. Link with exact text "Download"
2. Link with partial text "download"
3. Link with text containing "download" (case-insensitive)
4. Link with `href` containing `.pdf`
5. Link with `href` containing `fulltext-gateway` (DTU fulltext gateway)
6. Link with class containing "download"
7. Button with text containing "download"

Record IDs are extracted from URLs: `/en/catalog/<record_id>` (e.g. `697c04de6e5c71c3975b93f4`).

Pagination uses the `start` query parameter (Blacklight/Solr style): `start=0`, `start=20`, etc.
