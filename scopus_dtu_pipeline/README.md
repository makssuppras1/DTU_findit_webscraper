# Scopus DTU Pipeline

Retrieval pipeline for DTU-affiliated researchers and their publication metadata using the Scopus APIs. Produces a candidate set of research-active scientists and their publications for downstream use (e.g. supervisor-eligibility filtering).

## Setup

1. Install dependencies with uv (from repo root or from `scopus_dtu_pipeline/`):

   ```bash
   cd scopus_dtu_pipeline
   uv sync
   ```

2. Copy `.env.example` to `.env` and set your Scopus API key:

   ```bash
   cp .env.example .env
   # Edit .env: set SCOPUS_API_KEY=your_key
   ```

   Get an API key from [Elsevier Developer Portal](https://dev.elsevier.com/).

3. **Avoid key lock / 429:** Keep cache enabled (default) so repeat runs don’t re-hit the API. In `config/settings.yaml`, `rate_limit.requests_per_second` is set to 1; leave it at 1 or lower unless your key has a higher quota. On 429 the client waits 60s then retries with backoff.

## Usage

All commands are run from the `scopus_dtu_pipeline/` directory (or with `PYTHONPATH` set to it).

**Full pipeline (all steps):**
```bash
uv run python -m src.main
```

**Specific steps only:**
```bash
uv run python -m src.main --steps affiliation,candidates
uv run python -m src.main --steps profiles,filter,publications,outputs
```

**Resume after interrupt (re-run from a step; earlier checkpoints are loaded):**
```bash
uv run python -m src.main --steps filter,publications,outputs
```

**Disable response cache:**
```bash
uv run python -m src.main --no-cache
```

Pipeline steps (in order):

1. **affiliation** – Resolve DTU affiliation ID via Affiliation Search; save to `data/raw/dtu_affiliation_metadata.json` and checkpoint.
2. **candidates** – Author Search by AF-ID; save raw candidates and checkpoint.
3. **profiles** – Author Retrieval for each candidate; save raw JSON per author and checkpoint.
4. **filter** – For each candidate, Scopus Search for publications in last 5 years; keep only document types article, conference paper, review, book chapter; set `active_last_5y` and counts; checkpoint active list.
5. **publications** – For each active researcher, fetch all publications via Scopus Search and optionally enrich with Abstract Retrieval; checkpoint.
6. **outputs** – Write `data/processed/researchers.csv`, `data/processed/publications.csv`, and `data/processed/summary.json`.

Checkpoints are under `data/raw/`. If you re-run, completed steps are skipped when a checkpoint exists. **To start over from active filtering:** remove the filter and publications checkpoints, then run from the filter step:

```bash
rm -f data/raw/checkpoint_active_researchers.json data/raw/checkpoint_publications.json
uv run python -m src.main --steps filter,publications,outputs
```

Main will load affiliation, candidates, and profiles from their existing checkpoints, re-run the active-researcher filter, then fetch publications and build outputs.

## Outputs

- **data/raw/** – Raw API payloads, per-author profiles, per-author publications, checkpoints.
- **data/processed/** – `researchers.csv` (one row per active researcher), `publications.csv` (one row per publication), `summary.json`.
- **data/logs/** – `pipeline_run.log` and console logging.

## Where to add supervisor-eligibility filtering

- **After filter step:** The list `active` (researchers with ≥1 qualifying publication in the last 5 years) is the input to publication fetch. A future “supervisor eligibility” layer can sit here: take `active` and apply extra rules (e.g. minimum h-index, department/group, exclusion list, manual tags) before or after writing outputs.
- **In build_outputs:** Add a separate table or columns (e.g. `eligible_supervisor: bool`, `eligibility_notes`) that you fill from a separate module so the retrieval pipeline stays retrieval-only and eligibility logic stays pluggable.

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) for install/run. Dependencies: `requests`, `pandas`, `python-dotenv`, `pyyaml` (see `pyproject.toml`).

## Troubleshooting

**403 Forbidden with Cloudflare “Attention Required!” HTML**  
The API is behind Cloudflare. Requests from some networks (e.g. corporate or VPN) are blocked before reaching Elsevier. Fixes:

1. **Run from another network** – Home Wi‑Fi or mobile hotspot instead of office/VPN.
2. **Contact Elsevier** – [Support](https://dev.elsevier.com/support.html): ask if there is a different API host or IP allowlist for server-to-server use.
3. The client already sends a browser-like User-Agent and the API key in the URL (`apiKey` query param); if 403 persists, the block is almost certainly network/IP-based.
