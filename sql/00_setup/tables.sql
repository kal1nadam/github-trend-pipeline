CREATE TABLE IF NOT EXISTS `${RAW_DATASET}.events` (
    event_date DATE,
    created_at TIMESTAMP,
    type STRING,
    repo_name STRING,
    actor_login STRING,
    payload JSON
)
PARTITION BY event_date
CLUSTER BY repo_name;