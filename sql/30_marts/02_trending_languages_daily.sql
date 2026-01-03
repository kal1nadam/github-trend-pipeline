CREATE OR REPLACE TABLE `${MART_DATASET}.trending_languages_daily`
PARTITION BY event_date
CLUSTER BY primary_language AS
WITH base AS (
    SELECT
        event_date,
        primary_language,
        repo_name,
        events_today,
        actors_today,
        stars_today,
        trend_score
    FROM `${MART_DATASET}.trending_repos_enriched`
    WHERE primary_language IS NOT NULL
)

SELECT
    event_date,
    primary_language,

    COUNT(*) AS trending_repos_count,
    SUM(events_today) AS events_today_total,
    SUM(actors_today) AS actors_today_total,
    SUM(stars_today) AS stars_today_total,

    AVG(trend_score) AS avg_trend_score,
    SUM(trend_score) AS total_trend_score,

    ARRAY_AGG(
        STRUCT(repo_name, trend_score, events_today, actors_today, stars_today)
        ORDER BY trend_score DESC
        LIMIT 5
    ) AS top_repos
FROM base
GROUP BY event_date, primary_language;