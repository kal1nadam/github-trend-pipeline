# Data Model

This document describes every BigQuery table in the pipeline, organized by layer.

---

## Layer overview

| Layer | Dataset | Purpose |
|-------|---------|---------|
| Raw | `raw_github` | Ingested source events, append-only |
| Staging | `stg_github` | Cleaned events + aggregated activity |
| Model | `stg_github` | Enriched repo dimension |
| Mart | `mart_github` | Analytics-ready outputs for serving |

---

## Raw layer

### `raw_github.events`

Ingested directly from `githubarchive.day.YYYYMMDD` by `pipeline/extract.py`.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | `DATE` | Partition key — calendar day of the event (UTC) |
| `created_at` | `TIMESTAMP` | Original event timestamp from GitHub Archive |
| `type` | `STRING` | GitHub event type (e.g. `PushEvent`, `WatchEvent`) |
| `repo_name` | `STRING` | `owner/name` of the repository |
| `actor_login` | `STRING` | GitHub username of the actor |
| `payload` | `JSON` | Raw event payload (type-specific fields) |

**Partitioned by** `event_date` · **Clustered by** `repo_name`

---

## Staging layer

### `stg_github.stg_github_events`

Cleaned version of the raw events. Rows with NULL `repo_name` or `actor_login` are dropped.
Produced by `sql/10_staging/stg_github_events.sql`.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | `DATE` | Partition key |
| `created_at` | `TIMESTAMP` | Event timestamp |
| `event_type` | `STRING` | Renamed from `type` |
| `repo_name` | `STRING` | Repository identifier |
| `actor_login` | `STRING` | Actor username (non-null) |
| `payload` | `JSON` | Raw payload |

**Partitioned by** `event_date` · **Clustered by** `repo_name`

---

### `stg_github.daily_repo_activity`

Daily aggregated metrics per repository. One row per `(event_date, repo_name)`.
Produced by `sql/20_models/daily_repo_activity.sql`.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | `DATE` | Partition key |
| `repo_name` | `STRING` | Repository identifier |
| `events_total` | `INT64` | Total events of all types |
| `actors_unique` | `INT64` | Count of distinct actors |
| `pushes` | `INT64` | `PushEvent` count |
| `pull_requests` | `INT64` | `PullRequestEvent` count |
| `issues` | `INT64` | `IssuesEvent` count |
| `stars` | `INT64` | `WatchEvent` count (GitHub star proxy) |
| `forks` | `INT64` | `ForkEvent` count |

**Partitioned by** `event_date` · **Clustered by** `repo_name`

---

### `stg_github.repo_dim`

Repository dimension enriched with language and license metadata from BigQuery public datasets.
One row per `repo_name` (latest known state). Produced by `sql/20_models/repo_dim.sql`.

| Column | Type | Description |
|--------|------|-------------|
| `repo_name` | `STRING` | Repository identifier (primary key) |
| `primary_language` | `STRING` | Language with the most bytes in the repo |
| `all_languages` | `STRING` | JSON-like list of all detected languages |
| `license` | `STRING` | SPDX license identifier (NULL if unknown) |

**Sources:** `bigquery-public-data.github_repos.languages`, `bigquery-public-data.github_repos.licenses`

---

## Mart layer

### `mart_github.trending_repos_daily`

Statistical trend signals per repository per day. Compares today's activity against a rolling
baseline of the previous `LOOKBACK_DAYS` (default 14) days.
Produced by `sql/30_marts/00_trending_repos_daily.sql`.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | `DATE` | Partition key |
| `repo_name` | `STRING` | Repository identifier |
| `events_today` | `INT64` | Total events for this date |
| `actors_today` | `INT64` | Unique actors for this date |
| `stars_today` | `INT64` | Stars (WatchEvents) for this date |
| `avg_events_prev` | `FLOAT64` | Mean events over the lookback window |
| `std_events_prev` | `FLOAT64` | Std dev of events over the lookback window |
| `growth_events_ratio` | `FLOAT64` | `events_today / std_events_prev` |
| `z_events` | `FLOAT64` | `(events_today − avg) / std` — z-score for events |
| `avg_actors_prev` | `FLOAT64` | Mean actors over the lookback window |
| `std_actors_prev` | `FLOAT64` | Std dev of actors over the lookback window |
| `growth_actors_ratio` | `FLOAT64` | `actors_today / std_actors_prev` |
| `z_actors` | `FLOAT64` | Z-score for actors |
| `avg_stars_prev` | `FLOAT64` | Mean stars over the lookback window |
| `std_stars_prev` | `FLOAT64` | Std dev of stars over the lookback window |
| `growth_stars_ratio` | `FLOAT64` | `stars_today / std_stars_prev` |
| `z_stars` | `FLOAT64` | Z-score for stars |
| `trend_score` | `FLOAT64` | Weighted score: `z_events×0.6 + z_actors×0.3 + z_stars×0.1` |

Only repos with `events_today >= MIN_EVENTS_THRESHOLD` (default 50) are included.

**Partitioned by** `event_date` · **Clustered by** `repo_name`

---

### `mart_github.trending_repos_enriched`

Joins `trending_repos_daily` with `repo_dim` to add language and license.
Primary table used by the API and the compute step.
Produced by `sql/30_marts/01_trending_repos_enriched.sql`.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | `DATE` | Partition key |
| `repo_name` | `STRING` | Repository identifier |
| `primary_language` | `STRING` | Primary language (`"Unknown"` if missing) |
| `license` | `STRING` | License identifier (`"Unknown"` if missing) |
| `events_today` | `INT64` | Total events |
| `actors_today` | `INT64` | Unique actors |
| `stars_today` | `INT64` | Stars |
| `growth_events_ratio` | `FLOAT64` | Growth ratio (events) |
| `z_events` | `FLOAT64` | Z-score (events) |
| `trend_score` | `FLOAT64` | Composite trend score |
| *(all other columns from trending_repos_daily)* | | |

**Partitioned by** `event_date` · **Clustered by** `primary_language, repo_name`

---

### `mart_github.trending_languages_daily`

Aggregated trend metrics per programming language per day.
One row per `(event_date, primary_language)`.
Produced by `sql/30_marts/02_trending_languages_daily.sql`.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | `DATE` | Partition key |
| `primary_language` | `STRING` | Language name (NULL rows excluded) |
| `trending_repos_count` | `INT64` | Number of trending repos in this language |
| `events_today_total` | `INT64` | Sum of events across all repos |
| `actors_today_total` | `INT64` | Sum of unique actors across all repos |
| `stars_today_total` | `INT64` | Sum of stars across all repos |
| `avg_trend_score` | `FLOAT64` | Mean trend_score across repos |
| `total_trend_score` | `FLOAT64` | Sum of trend_score across repos |
| `top_repos` | `ARRAY<STRUCT>` | Top 5 repos by trend_score with metrics |

**Partitioned by** `event_date` · **Clustered by** `primary_language`

---

### `mart_github.alerts_daily`

Repo and language alerts generated by `pipeline/compute.py`. Rows are deleted and
re-inserted each run so re-runs are idempotent.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | `DATE` | Partition key |
| `alert_type` | `STRING` | `"repo"` or `"language"` |
| `entity` | `STRING` | Repository name or language name |
| `severity` | `STRING` | `"low"` / `"medium"` / `"high"` |
| `trend_score` | `FLOAT64` | Composite trend score at alert time |
| `z_events` | `FLOAT64` | Z-score for events (NULL for language alerts) |
| `growth_events_ratio` | `FLOAT64` | Growth ratio (NULL for language alerts) |
| `events_today` | `INT64` | Event count |
| `actors_today` | `INT64` | Unique actor count |
| `stars_today` | `INT64` | Star count |
| `primary_language` | `STRING` | Language (NULL for language-type alerts) |
| `created_at` | `TIMESTAMP` | When this alert row was created |

**Severity thresholds (repo alerts):**

| Severity | Condition |
|----------|-----------|
| `high` | `z_events ≥ 6` OR `growth_events_ratio ≥ 10` |
| `medium` | `z_events ≥ 4` OR `growth_events_ratio ≥ 5` |
| `low` | above `ALERT_Z_THRESHOLD_LOW` (default 3) |

**Severity thresholds (language alerts):**

| Severity | Condition |
|----------|-----------|
| `high` | `avg_trend_score ≥ 6` |
| `medium` | `avg_trend_score ≥ 4` |
| `low` | below medium |

**Partitioned by** `event_date` · **Clustered by** `alert_type, severity, entity`

---

### `mart_github.daily_summary`

One summary row per pipeline run date. Produced by `pipeline/compute.py`.

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | `DATE` | Partition key |
| `summary_text` | `STRING` | Human-readable summary sentence |
| `top_repos` | `ARRAY<STRING>` | Top 5 repo names by trend_score |
| `top_languages` | `ARRAY<STRING>` | Top 5 languages by total_trend_score |
| `created_at` | `TIMESTAMP` | Row creation timestamp |

**Partitioned by** `event_date`
