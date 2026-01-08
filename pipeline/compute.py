from __future__ import annotations

import argparse
from datetime import datetime, timezone, date

from google.cloud import bigquery

from pipeline.config import Settings
from pipeline.bq import BQQueryRunner

ALERTS_INSERT_SQL = """
-- Insert alerts for a given day
INSERT INTO `${MART_DATASET}.alerts_daily`
(
    event_date, alert_type, entity, severity,
    trend_score, z_events, growth_events_ratio,
    events_today, actors_today, stars_today,
    primary_language, created_at
)
SELECT
    t.event_date,
    'repo' AS alert_type,
    t.repo_name AS entity,
    CASE
        WHEN t.z_events >= 6 OR t.growth_events_ratio >= 10 THEN 'high'
        WHEN t.z_events >= 4 OR t.growth_events_ratio >= 5 THEN 'medium'
        ELSE 'low'
    END AS severity,
    t.trend_score,
    t.z_events,
    t.growth_events_ratio,
    t.events_today,
    t.actors_today,
    t.stars_today,
    t.primary_language,
    CURRENT_TIMESTAMP() AS created_at
FROM `${MART_DATASET}.trending_repos_enriched` t
WHERE t.event_date = DATE("${DATE}")
    AND (
        t.z_events >= ${ALERT_Z_THRESHOLD_LOW}
        OR t.growth_events_ratio >= ${ALERT_GROWTH_THRESHOLD_LOW}
    )
QUALIFY
    ROW_NUMBER() OVER (ORDER BY t.trend_score DESC) <= ${MAX_REPO_ALERTS};
"""

LANG_ALERTS_INSERT_SQL = """
-- Insert language alerts for a given day
INSERT INTO `${MART_DATASET}.alerts_daily`
(
    event_date, alert_type, entity, severity,
    trend_score, z_events, growth_events_ratio,
    events_today, actors_today, stars_today,
    primary_language, created_at
)
SELECT
    l.event_date,
    'language' AS alert_type,
    l.primary_language AS entity,
    CASE
        WHEN l.avg_trend_score >= 6 THEN 'high'
        WHEN l.avg_trend_score >= 4 THEN 'medium'
        ELSE 'low'
    END AS severity,
    l.avg_trend_score AS trend_score,
    NULL AS z_events,
    NULL AS growth_events_ratio,
    l.events_today_total AS events_today,
    l.actors_today_total AS actors_today,
    l.stars_today_total AS stars_today,
    l.primary_language,
    CURRENT_TIMESTAMP() AS created_at
FROM `${MART_DATASET}.trending_languages_daily` l
WHERE l.event_date = DATE("${DATE}")
    AND l.primary_language IS NOT NULL
QUALIFY
    ROW_NUMBER() OVER (ORDER BY l.total_trend_score DESC) <= ${MAX_LANGUAGE_ALERTS};
"""

SUMMARY_INSERT_SQL = """
-- Create daily summary row
INSERT INTO `${MART_DATASET}.daily_summary`
(event_date, summary_text, top_repos, top_languages, created_at)
WITH
top_repos AS (
    SELECT ARRAY_AGG(repo_name ORDER BY trend_score DESC LIMIT 5) AS repos
    FROM `${MART_DATASET}.trending_repos_enriched`
    WHERE event_date = DATE("${DATE}")
),
top_langs AS (
    SELECT ARRAY_AGG(primary_language ORDER BY total_trend_score DESC LIMIT 5) AS langs
    FROM `${MART_DATASET}.trending_languages_daily`
    WHERE event_date = DATE("${DATE}")
),
stats AS (
    SELECT
        (SELECT COUNT(DISTINCT repo_name) FROM `${MART_DATASET}.trending_repos_enriched` WHERE event_date = DATE("${DATE}")) AS repo_count,
        (SELECT COUNT(*) FROM `${MART_DATASET}.alerts_daily` WHERE event_date = DATE("${DATE}") AND alert_type = 'repo') AS repo_alerts,
        (SELECT COUNT(*) FROM `${MART_DATASET}.alerts_daily` WHERE event_date = DATE("${DATE}") AND alert_type = 'language') AS lang_alerts
)
SELECT
    DATE("${DATE}") AS event_date,
    CONCAT(
        'Daily GitHub trend summary for ', "${DATE}", ': ',
        'Trending repos analyzed: ', CAST(stats.repo_count AS STRING), '. ',
        'Repo alerts: ', CAST(stats.repo_alerts AS STRING), '. ',
        'Language alerts: ', CAST(stats.lang_alerts AS STRING), '.'
    ) AS summary_text,
    top_repos.repos AS top_repos,
    top_langs.langs AS top_languages,
    CURRENT_TIMESTAMP() AS created_at
FROM stats, top_repos, top_langs;
"""

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    args = parser.parse_args()

    settings = Settings.load()
    bq = BQQueryRunner(settings)


    # clean existing rows for this date
    cleanup_sql = f"""
    DELETE FROM `{settings.mart_dataset}.alerts_daily` WHERE event_date = DATE("{args.date}");
    DELETE FROM `{settings.mart_dataset}.daily_summary` WHERE event_date = DATE("{args.date}");
    """

    print(f"Cleaning up existing alerts for date {args.date} ...")
    bq.run(cleanup_sql, job_config=bigquery.QueryJobConfig(labels={"step": "compute_alerts_cleanup"}))

    print(f"Inserting repo alerts for date {args.date} ...")
    bq.run(ALERTS_INSERT_SQL, extra={"DATE": args.date}, job_config=bigquery.QueryJobConfig(labels={"step": "compute_repo_alerts"}))

    print(f"Inserting language alerts for date {args.date} ...")
    bq.run(LANG_ALERTS_INSERT_SQL, extra={"DATE": args.date}, job_config=bigquery.QueryJobConfig(labels={"step": "compute_language_alerts"}))

    print(f"Inserting daily summary for date {args.date} ...")
    bq.run(SUMMARY_INSERT_SQL, extra={"DATE": args.date}, job_config=bigquery.QueryJobConfig(labels={"step": "compute_daily_summary"}))
    
    print("Compute done.")


if __name__ == "__main__":
    main()