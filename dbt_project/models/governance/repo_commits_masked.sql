{{ config(
    materialized='view',
    schema='governance'
) }}

WITH staged_commits AS (
    SELECT * FROM {{ ref('stg_commits') }}
)

SELECT
    commit_sha,
    repo_id,
    repo_full_name,
    -- PII Governance: Anonymized / Masked Attributes
    {{ mask_name('author_name') }} AS author_name_masked,
    {{ mask_email('author_email') }} AS author_email_masked,
    {{ hash_pseudonym('author_email') }} AS author_pseudonym_id,
    {{ mask_name('committer_name') }} AS committer_name_masked,
    {{ mask_email('committer_email') }} AS committer_email_masked,
    commit_message,
    commit_date,
    ingested_at
FROM staged_commits
