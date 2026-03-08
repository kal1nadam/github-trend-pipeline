# GitHub Trend Pipeline

![CI](https://github.com/kal1nadam/github-trend-pipeline/actions/workflows/ci.yml/badge.svg)

A production-ready analytics pipeline that ingests public GitHub event data, models it in
**BigQuery** using a layered warehouse architecture, computes statistical trend signals, and
exposes the results through a **FastAPI** REST service.

---

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Pipeline steps](#pipeline-steps)
- [Repository structure](#repository-structure)
- [Quick start](#quick-start)
- [Environment variables](#environment-variables)
- [Running locally](#running-locally)
- [Running with Docker](#running-with-docker)
- [API reference](#api-reference)
- [Development](#development)
- [BigQuery costs](#bigquery-costs)

---

## Overview

The pipeline runs daily and produces five mart tables consumed by the REST API:

| Table | Description |
|-------|-------------|
| `mart_github.trending_repos_daily` | Z-score and trend signals per repo/day |
| `mart_github.trending_repos_enriched` | Trending repos with language + license metadata |
| `mart_github.trending_languages_daily` | Aggregated trend metrics per language/day |
| `mart_github.alerts_daily` | Repo and language alerts with severity levels |
| `mart_github.daily_summary` | One-row daily digest with top repos and languages |

**Data sources:**
- `githubarchive.day` — GitHub public event stream
- `bigquery-public-data.github_repos` — Repository metadata (languages, licenses)

---

## Architecture

```
githubarchive.day.YYYYMMDD
        │
        │  extract.py
        ▼
raw_github.events
        │
        │  SQL: 10_staging
        ▼
stg_github.stg_github_events
        │
        │  SQL: 20_models
        ▼
stg_github.daily_repo_activity ──► stg_github.repo_dim
        │                               (+ BQ public metadata)
        │  SQL: 30_marts
        ▼
mart_github.trending_repos_daily
        │
        ▼
mart_github.trending_repos_enriched ──► mart_github.trending_languages_daily
        │                                         │
        │  compute.py                             │
        ▼                                         ▼
mart_github.alerts_daily          mart_github.daily_summary
        │
        ▼
FastAPI  /trending/repos  /trending/languages  /alerts  /summary
```

See [`docs/architecture.mmd`](docs/architecture.mmd) for the full Mermaid diagram.
See [`docs/data_model.md`](docs/data_model.md) for complete table schemas.

---

## Pipeline steps

| Step | Entry point | Output |
|------|-------------|--------|
| **Extract** | `pipeline/extract.py` | `raw_github.events` |
| **Staging** | `sql/10_staging/` | `stg_github.stg_github_events` |
| **Models** | `sql/20_models/` | `stg_github.daily_repo_activity`, `stg_github.repo_dim` |
| **Marts** | `sql/30_marts/` | `mart_github.trending_repos_*`, `mart_github.trending_languages_daily` |
| **Compute** | `pipeline/compute.py` | `mart_github.alerts_daily`, `mart_github.daily_summary` |
| **Serve** | `pipeline/serve.py` | FastAPI REST endpoints |

---

## Repository structure

```
.
├── pipeline/               # Python application code
│   ├── config.py           # Settings from environment variables
│   ├── bq.py               # BigQuery client + SQL templating
│   ├── extract.py          # Ingest one day from GitHub Archive
│   ├── transform.py        # Run staging + mart SQL files
│   ├── compute.py          # Compute alerts + daily summary
│   ├── serve.py            # FastAPI application
│   ├── api_models.py       # Pydantic response models
│   └── run_daily.py        # Orchestrate extract → transform → compute
│
├── sql/
│   ├── 00_setup/           # Dataset + table DDL
│   ├── 10_staging/         # Staging SQL
│   ├── 20_models/          # Aggregation + dimension SQL
│   └── 30_marts/           # Trend scoring + enrichment SQL
│
├── tests/                  # Unit tests (pytest) — no GCP credentials needed
│   ├── test_config.py
│   ├── test_bq.py
│   ├── test_extract.py
│   ├── test_api_models.py
│   ├── test_serve.py
│   ├── test_placeholder.py # transform.py tests
│   ├── test_compute.py
│   └── test_run_daily.py
│
├── docs/
│   ├── architecture.mmd    # Mermaid architecture diagram
│   └── data_model.md       # Table schemas and field descriptions
│
├── .github/workflows/ci.yml  # Lint + format + test CI (runs on every push)
├── Dockerfile.job          # Batch pipeline image
├── Dockerfile.api          # FastAPI service image
└── pyproject.toml          # Poetry project + tool config
```

---

## Quick start

### Prerequisites

- Python 3.11+
- [Poetry](https://python-poetry.org/)
- A Google Cloud project with BigQuery enabled
- `gcloud` CLI (for local authentication)

### 1. Authenticate with GCP

```bash
gcloud auth application-default login
```

Or use a service account key:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### 2. Install dependencies

```bash
poetry install
```

### 3. Configure environment

Copy `.env.docker` and fill in your values:

```bash
cp .env.docker .env
# edit .env — set GCP_PROJECT_ID at minimum
```

### 4. Initialize BigQuery datasets and tables

```bash
make setup
```

### 5. Run the full pipeline for a date

```bash
make run DATE=2025-01-01
```

---

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GCP_PROJECT_ID` | **yes** | — | GCP project that owns the BigQuery datasets |
| `BQ_LOCATION` | no | `US` | BigQuery dataset location |
| `RAW_DATASET` | no | `raw_github` | Dataset for raw ingested events |
| `STG_DATASET` | no | `stg_github` | Dataset for staging and model tables |
| `MART_DATASET` | no | `mart_github` | Dataset for mart and alert tables |
| `SOURCE_EVENTS_PROJECT` | no | `githubarchive` | Project hosting GitHub Archive |
| `SOURCE_EVENTS_DATASET` | no | `day` | Dataset hosting GitHub Archive daily tables |
| `LOOKBACK_DAYS` | no | `14` | Days of history used for baseline statistics |
| `MIN_EVENTS_THRESHOLD` | no | `50` | Minimum events for a repo to appear in trending |
| `ALERT_Z_THRESHOLD_LOW` | no | `3.0` | Z-score threshold to generate a low-severity alert |
| `ALERT_GROWTH_THRESHOLD_LOW` | no | `3.0` | Growth ratio threshold for low-severity alerts |
| `MAX_REPO_ALERTS` | no | `50` | Max repo alerts inserted per day |
| `MAX_LANGUAGE_ALERTS` | no | `20` | Max language alerts inserted per day |
| `GOOGLE_APPLICATION_CREDENTIALS` | no | — | Path to service account JSON (local/Docker) |

---

## Running locally

```bash
# Initialize BigQuery infrastructure
make setup

# Extract one day of events
make extract DATE=2025-01-01

# Run staging + mart SQL transformations
make transform

# Compute alerts and daily summary
make compute DATE=2025-01-01

# Full pipeline in one command (setup → extract → transform → compute)
make run DATE=2025-01-01

# Start the API server
make serve
# → http://localhost:8000/docs
```

Individual steps can also be run directly:

```bash
poetry run python -m pipeline.extract --date 2025-01-01
poetry run python -m pipeline.transform
poetry run python -m pipeline.compute --date 2025-01-01
poetry run uvicorn pipeline.serve:app --reload --port 8000
```

---

## Running with Docker

### Batch job

```bash
docker build -f Dockerfile.job -t github-trend-job .

docker run --rm \
  --env-file .env.docker \
  -v /path/to/service-account.json:/secrets/gcp-sa.json:ro \
  github-trend-job
```

### API service

```bash
docker build -f Dockerfile.api -t github-trend-api .

docker run --rm -p 8080:8080 \
  --env-file .env.docker \
  -v /path/to/service-account.json:/secrets/gcp-sa.json:ro \
  github-trend-api
```

Then open `http://localhost:8080/docs`.

> **Cloud Run:** Use the same images. Attach a service account instead of mounting a key file.
> Schedule the job image with Cloud Scheduler for daily execution.

---

## API reference

All endpoints query BigQuery marts directly. Parameterized queries prevent SQL injection.

### `GET /health`

```json
{"status": "ok", "project": "my-project", "mart_dataset": "mart_github"}
```

### `GET /trending/repos`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `date` | string | **required** | `YYYY-MM-DD` |
| `limit` | int | `50` | Max results (1–200) |
| `language` | string | — | Filter by `primary_language` |

Returns an array of `TrendingRepo` objects ordered by `trend_score DESC`.

### `GET /trending/languages`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `date` | string | **required** | `YYYY-MM-DD` |
| `limit` | int | `20` | Max results (1–200) |

Returns an array of `TrendingLanguage` objects ordered by `total_trend_score DESC`.

### `GET /alerts`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `date` | string | **required** | `YYYY-MM-DD` |
| `alert_type` | string | — | `repo` or `language` |
| `severity` | string | — | `low`, `medium`, or `high` |
| `limit` | int | `100` | Max results (1–500) |

Returns alerts ordered by severity DESC, then trend_score DESC.

### `GET /summary`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `date` | string | **required** | `YYYY-MM-DD` |

Returns the `DailySummary` for the given date, or a placeholder if none exists.

Interactive docs: `http://localhost:8000/docs`

---

## Development

### Install dev dependencies

```bash
poetry install --with dev
```

### Run tests

```bash
poetry run pytest tests/ -v
```

Tests mock the BigQuery client — no GCP credentials are needed to run the test suite.

### Lint and format

```bash
poetry run ruff check .        # style + import checks
poetry run ruff format --check . # formatting check
poetry run ruff format .       # apply formatting
```

### CI

GitHub Actions runs lint, format, and tests on every push.
See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

---

## BigQuery costs

This pipeline is designed for minimal spend:

- All SQL queries filter on date **partition columns** (`event_date`) to avoid full scans.
- Tables are **clustered** by `repo_name` or `primary_language` to prune irrelevant blocks.
- The extract step inserts only the columns needed — raw `payload` is stored as JSON for compactness.
- Job labels (`project=github-trend-pipeline`, `step=extract|transform|compute`) make it easy to
  track spending per pipeline stage in the GCP Cost Management console.
- The API uses **parameterized queries** with `LIMIT` clauses — results are never scanned in bulk.

A typical daily run (one day of GitHub Archive data) processes well under 1 GB across all steps.
