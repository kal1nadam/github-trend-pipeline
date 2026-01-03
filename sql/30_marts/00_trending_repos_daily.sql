CREATE OR REPLACE TABLE `${MART_DATASET}.trending_repos_daily`
PARTITION BY event_date
CLUSTER BY repo_name AS
WITH base  AS (
    SELECT
        event_date,
        repo_name,
        events_total,
        actors_unique,
        pushes,
        pull_requests,
        issues,
        stars,
        forks
    FROM `${STG_DATASET}.daily_repo_activity`
),

baseline AS (
    SELECT
        b.event_date,
        b.repo_name,

        b.events_total AS events_today,
        b.actors_unique AS actors_today,
        b.stars AS stars_today,

        AVG(prev.events_total) AS avg_events_prev,
        STDDEV_SAMP(prev.events_total) AS std_events_prev,
        AVG(prev.actors_unique) AS avg_actors_prev,
        STDDEV_SAMP(prev.actors_unique) AS std_actors_prev,
        AVG(prev.stars) AS avg_stars_prev,
        STDDEV_SAMP(prev.stars) AS std_stars_prev

    FROM base b
    LEFT JOIN base prev
        ON prev.repo_name = b.repo_name
        AND prev.event_date < b.event_date
        AND prev.event_date >= DATE_SUB(b.event_date, INTERVAL ${LOOKBACK_DAYS} DAY)
    GROUP BY
        b.event_date,
        b.repo_name,
        b.events_total,
        b.actors_unique,
        b.stars
),

scored AS (
    SELECT
        event_date,
        repo_name,

        events_today,
        actors_today,
        stars_today,

        avg_events_prev,
        std_events_prev,
        avg_actors_prev,
        std_actors_prev,
        avg_stars_prev,
        std_stars_prev,

        SAFE_DIVIDE(events_today, NULLIF(std_events_prev, 0)) AS growth_events_ratio,
        SAFE_DIVIDE(actors_today, NULLIF(std_actors_prev, 0)) AS growth_actors_ratio,
        SAFE_DIVIDE(stars_today, NULLIF(std_stars_prev, 0)) AS growth_stars_ratio,

        SAFE_DIVIDE(events_today - avg_events_prev, NULLIF(std_events_prev, 0)) AS z_events,
        SAFE_DIVIDE(actors_today - avg_actors_prev, NULLIF(std_actors_prev, 0)) AS z_actors,
        SAFE_DIVIDE(stars_today - avg_stars_prev, NULLIF(std_stars_prev, 0)) AS z_stars
    FROM baseline
)

SELECT
    event_date,
    repo_name,

    events_today,
    actors_today,
    stars_today,

    avg_events_prev,
    std_events_prev,
    growth_events_ratio,
    z_events,

    avg_actors_prev,
    std_actors_prev,
    growth_actors_ratio,
    z_actors,

    avg_stars_prev,
    std_stars_prev,
    growth_stars_ratio,
    z_stars,

    (
        COALESCE(z_events, 0) * 0.6 +
        COALESCE(z_actors, 0) * 0.3 +
        COALESCE(z_stars, 0) * 0.1
    ) AS trend_score

FROM scored
WHERE events_today >= ${MIN_EVENTS_THRESHOLD}