WITH source_data AS (
    SELECT * FROM {{ source('raw', 'raw_repos') }}
)

SELECT
    repo_id::BIGINT AS repo_id,
    TRIM(name)::VARCHAR(255) AS repo_name,
    TRIM(full_name)::VARCHAR(255) AS full_name,
    TRIM(organization)::VARCHAR(255) AS organization,
    description,
    html_url,
    COALESCE(stars_count, 0)::INTEGER AS stars_count,
    COALESCE(forks_count, 0)::INTEGER AS forks_count,
    COALESCE(open_issues_count, 0)::INTEGER AS open_issues_count,
    COALESCE(TRIM(language), 'Unknown')::VARCHAR(100) AS primary_language,
    created_at::TIMESTAMP WITH TIME ZONE AS created_at,
    updated_at::TIMESTAMP WITH TIME ZONE AS updated_at,
    pushed_at::TIMESTAMP WITH TIME ZONE AS pushed_at,
    ROUND(
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at)) / 86400.0, 1
    ) AS repo_age_days,
    ROUND(
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - pushed_at)) / 86400.0, 1
    ) AS days_since_last_push,
    ingested_at::TIMESTAMP WITH TIME ZONE AS ingested_at
FROM source_data
