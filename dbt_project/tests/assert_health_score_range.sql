-- Custom singular test: Ensure health scores are strictly bounded between 0 and 100
SELECT
    repo_id,
    repo_name,
    health_score
FROM {{ ref('repo_health_score') }}
WHERE health_score < 0.0 OR health_score > 100.0
