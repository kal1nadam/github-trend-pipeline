SELECT
type,
COUNT(*) as events,
FROM `githubarchive.day.20251001`
GROUP BY type
ORDER BY events DESC
LIMIT 50;