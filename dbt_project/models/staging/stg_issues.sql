WITH source_data AS (
    SELECT * FROM {{ source('raw', 'raw_issues') }}
)

SELECT
    issue_id::BIGINT AS issue_id,
    repo_id::BIGINT AS repo_id,
    TRIM(repo_full_name)::VARCHAR(255) AS repo_full_name,
    issue_number::INTEGER AS issue_number,
    TRIM(title) AS title,
    LOWER(TRIM(state))::VARCHAR(50) AS state,
    (LOWER(TRIM(state)) = 'closed') AS is_closed,
    COALESCE(comments_count, 0)::INTEGER AS comments_count,
    COALESCE(is_pull_request, FALSE)::BOOLEAN AS is_pull_request,
    COALESCE(TRIM(author_login), 'unknown')::VARCHAR(255) AS author_login,
    created_at::TIMESTAMP WITH TIME ZONE AS created_at,
    closed_at::TIMESTAMP WITH TIME ZONE AS closed_at,
    updated_at::TIMESTAMP WITH TIME ZONE AS updated_at,
    CASE
        WHEN closed_at IS NOT NULL AND closed_at >= created_at
        THEN ROUND(EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600.0, 2)
        ELSE NULL
    END AS duration_hours_to_close,
    CASE
        WHEN closed_at IS NOT NULL AND closed_at >= created_at
        THEN ROUND(EXTRACT(EPOCH FROM (closed_at - created_at)) / 86400.0, 2)
        ELSE NULL
    END AS duration_days_to_close,
    ingested_at::TIMESTAMP WITH TIME ZONE AS ingested_at
FROM source_data
