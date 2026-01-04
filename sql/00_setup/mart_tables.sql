CREATE TABLE IF NOT EXISTS `${MART_DATASET}.alerts_daily` (
    event_date DATE,
    alert_type STRING,
    entity STRING,
    severity STRING,
    trend_score FLOAT64,
    z_events FLOAT64,
    growth_events_ratio FLOAT64,
    events_today INT64,
    actors_today INT64,
    stars_today INT64
    primary_language STRING,
    created_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY alert_type, severity, entity;

CREATE TABLE IF NOT EXISTS `${MART_DATASET}.daily_summary` (
    event_date DATE,
    summary_text STRING,
    top_repos ARRAY<STRING>,
    top_languages ARRAY<STRING>,
    created_at TIMESTAMP
)
PARTITION BY event_date;