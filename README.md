# GitHub Trend Pipeline (BigQuery + Python)

A multi-stage data pipeline that reads GitHub public events from `githubarchive`, models daily repository activity in BigQuery, computes trending/anomaly signals in Python, and writes “ready-to-serve” results back to BigQuery.

## Data sources
- GitHub public events: `githubarchive.day` (event stream)
- Repository metadata: `bigquery-public-data.github_repos` (languages/licenses/etc.)

## Architecture
See: `docs/architecture.mmd`

## Outputs
- `mart_github.trending_repos_daily`
- `mart_github.alerts_daily`
- `mart_github.trending_languages_daily`

## How to run (high level)
1. Create a GCP project and enable BigQuery.
2. Authenticate locally (ADC):
   - `gcloud auth application-default login`
3. Configure `.env` from `.env.example`
4. Run:
   - `make setup`
   - `make extract DATE=YYYY-MM-DD`
   - `make transform`
   - `make compute`

## Notes
- This repo is fully reviewable from code + SQL. Reviewers do not need access to my BigQuery project.
- Cost control: queries always filter by date partitions and project only needed columns.
