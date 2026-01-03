CREATE OR REPLACE TABLE `${STG_DATASET}.repo_dim` AS
WITH active_repos AS (
    SELECT DISTINCT repo_name
    from `${STG_DATASET}.daily_repo_activity`
),

langs AS (
    SELECT
        repo_name,
        language AS languages,
    FROM `bigquery-public-data.github_repos.languages`
    WHERE repo_name IN (SELECT repo_name FROM active_repos)
),

lic AS (
    SELECT
        repo_name,
        license
    FROM `bigquery-public-data.github_repos.licenses`
    WHERE repo_name IN (SELECT repo_name FROM active_repos)
)

SELECT
    a.repo_name,
    (SELECT l.name
        FROM UNNEST(langs.languages) AS l
        ORDER BY l.bytes DESC
        LIMIT 1) AS primary_language,
    langs.languages AS all_languages,
    lic.license
FROM active_repos a 
LEFT JOIN langs ON a.repo_name = langs.repo_name
LEFT JOIN lic ON a.repo_name = lic.repo_name; 