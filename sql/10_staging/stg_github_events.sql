CREATE OR REPLACE TABLE `${STG_DATASET}.stg_github_events`
PARTITION BY event_date
CLUSTER BY repo_name AS
SELECT
    event_date,
    created_at,
    type AS event_type,
    repo_name,
    actor_login,
    payload
FROM `${RAW_DATASET}.events`
WHERE repo_name IS NOT NULL
    AND actor_login IS NOT NULL;