CREATE OR REPLACE TABLE `${STG_DATASET}.daily_repo_activity`
PARTITION BY event_date
CLUSTER BY repo_name AS
SELECT
    event_date,
    repo_name,

    COUNT(*) AS events_total,
    COUNT(DISTINCT actor_login) AS actors_unique,

    COUNTIF(event_type = 'PushEvent') AS pushes,
    COUNTIF(event_type = 'PullRequestEvent') AS pull_requests,
    COUNTIF(event_type = 'IssuesEvent') AS issues,
    COUNTIF(event_type = 'WatchEvent') AS stars,
    COUNTIF(event_type = 'ForkEvent') AS forks

FROM `${STG_DATASET}.stg_github_events`
GROUP BY event_date, repo_name;