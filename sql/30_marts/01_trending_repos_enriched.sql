CREATE OR REPLACE TABLE `${MART_DATASET}.trending_repos_enriched`
PARTITION BY event_date
CLUSTER BY primary_language, repo_name AS
SELECT
    t.event_date,
    t.repo_name,

    COALESCE(d.primary_language, 'Unknown') AS primary_language,
    COALESCE(d.license, 'Unknown') AS license,

    t.events_today,
    t.actors_today,
    t.stars_today,

    t.growth_events_ratio,
    t.z_events,
    t.trend_score

FROM `${MART_DATASET}.trending_repos_daily` t
LEFT JOIN `${STG_DATASET}.repo_dim` d
ON t.repo_name = d.repo_name;