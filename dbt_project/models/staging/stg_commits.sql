WITH source_data AS (
    SELECT * FROM {{ source('raw', 'raw_commits') }}
)

SELECT
    TRIM(commit_sha)::VARCHAR(64) AS commit_sha,
    repo_id::BIGINT AS repo_id,
    TRIM(repo_full_name)::VARCHAR(255) AS repo_full_name,
    COALESCE(TRIM(author_name), 'Unknown Author')::VARCHAR(255) AS author_name,
    COALESCE(LOWER(TRIM(author_email)), 'unknown@example.com')::VARCHAR(255) AS author_email,
    COALESCE(TRIM(committer_name), 'Unknown Committer')::VARCHAR(255) AS committer_name,
    COALESCE(LOWER(TRIM(committer_email)), 'unknown@example.com')::VARCHAR(255) AS committer_email,
    TRIM(commit_message) AS commit_message,
    commit_date::TIMESTAMP WITH TIME ZONE AS commit_date,
    ingested_at::TIMESTAMP WITH TIME ZONE AS ingested_at
FROM source_data
