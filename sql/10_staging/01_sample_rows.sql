SELECT
created_at,
type,
repo.name AS repo_name,
actor.login AS actor_login,
FROM `githubarchive.day.20251001`
LIMIT 50;