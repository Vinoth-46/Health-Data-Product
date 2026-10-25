{{ config(
    materialized='table',
    schema='analytics'
) }}

WITH repo_metadata AS (
    SELECT
        repo_id,
        repo_name,
        full_name,
        organization,
        description,
        html_url,
        stars_count,
        forks_count,
        open_issues_count,
        primary_language,
        repo_age_days,
        days_since_last_push,
        created_at,
        pushed_at
    FROM {{ ref('stg_repos') }}
),

issue_aggregates AS (
    SELECT
        repo_id,
        COUNT(CASE WHEN NOT is_pull_request THEN 1 END) AS total_issues_tracked,
        COUNT(CASE WHEN NOT is_pull_request AND is_closed THEN 1 END) AS closed_issues_count,
        COUNT(CASE WHEN NOT is_pull_request AND NOT is_closed THEN 1 END) AS open_issues_tracked,
        COUNT(CASE WHEN is_pull_request THEN 1 END) AS pull_requests_tracked,
        ROUND(
            AVG(CASE WHEN NOT is_pull_request AND is_closed THEN duration_days_to_close END)::NUMERIC,
            2
        ) AS avg_issue_close_time_days,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY CASE WHEN NOT is_pull_request AND is_closed THEN duration_days_to_close END
            )::NUMERIC,
            2
        ) AS median_issue_close_time_days,
        ROUND(
            (
                COUNT(CASE WHEN NOT is_pull_request AND is_closed THEN 1 END)::NUMERIC * 100.0 /
                NULLIF(COUNT(CASE WHEN NOT is_pull_request THEN 1 END), 0)
            )::NUMERIC,
            1
        ) AS issue_closure_rate_pct
    FROM {{ ref('stg_issues') }}
    GROUP BY repo_id
),

commit_aggregates AS (
    SELECT
        repo_id,
        COUNT(*) AS total_commits_tracked,
        COUNT(CASE WHEN commit_date >= CURRENT_TIMESTAMP - INTERVAL '30 days' THEN 1 END) AS commits_last_30d,
        COUNT(CASE WHEN commit_date >= CURRENT_TIMESTAMP - INTERVAL '90 days' THEN 1 END) AS commits_last_90d,
        COUNT(DISTINCT author_email) AS active_contributors_count,
        MAX(commit_date) AS latest_commit_date,
        ROUND(
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(commit_date))) / 86400.0,
            1
        ) AS days_since_latest_commit
    FROM {{ ref('stg_commits') }}
    GROUP BY repo_id
),

joined_metrics AS (
    SELECT
        r.repo_id,
        r.repo_name,
        r.full_name,
        r.organization,
        r.description,
        r.html_url,
        r.primary_language,
        r.stars_count,
        r.forks_count,
        r.open_issues_count,
        r.repo_age_days,
        r.days_since_last_push,
        COALESCE(i.total_issues_tracked, 0) AS total_issues_tracked,
        COALESCE(i.closed_issues_count, 0) AS closed_issues_count,
        COALESCE(i.open_issues_tracked, 0) AS open_issues_tracked,
        COALESCE(i.pull_requests_tracked, 0) AS pull_requests_tracked,
        COALESCE(i.avg_issue_close_time_days, 5.0) AS avg_issue_close_time_days,
        COALESCE(i.median_issue_close_time_days, 4.0) AS median_issue_close_time_days,
        COALESCE(i.issue_closure_rate_pct, 70.0) AS issue_closure_rate_pct,
        COALESCE(c.total_commits_tracked, 0) AS total_commits_tracked,
        COALESCE(c.commits_last_30d, 0) AS commits_last_30d,
        COALESCE(c.commits_last_90d, 0) AS commits_last_90d,
        COALESCE(c.active_contributors_count, 1) AS active_contributors_count,
        c.latest_commit_date,
        COALESCE(c.days_since_latest_commit, 0.0) AS days_since_latest_commit
    FROM repo_metadata r
    LEFT JOIN issue_aggregates i ON r.repo_id = i.repo_id
    LEFT JOIN commit_aggregates c ON r.repo_id = c.repo_id
),

health_scoring AS (
    SELECT
        *,
        -- 1. Issue Velocity Score (0 to 100): Turnaround speed & closure efficiency
        CASE
            WHEN avg_issue_close_time_days <= 3.0 THEN 100.0
            WHEN avg_issue_close_time_days <= 7.0 THEN 85.0
            WHEN avg_issue_close_time_days <= 14.0 THEN 70.0
            WHEN avg_issue_close_time_days <= 30.0 THEN 50.0
            ELSE 35.0
        END * (0.6 + (0.4 * (LEAST(issue_closure_rate_pct, 100.0) / 100.0))) AS issue_velocity_score,

        -- 2. Commit Cadence Score (0 to 100): Active contributions and release frequency
        CASE
            WHEN commits_last_30d >= 30 THEN 100.0
            WHEN commits_last_30d >= 15 THEN 85.0
            WHEN commits_last_30d >= 8 THEN 70.0
            WHEN commits_last_30d >= 3 THEN 55.0
            ELSE 40.0
        END AS commit_cadence_score,

        -- 3. Community Engagement Score (0 to 100): Stars, forks, and contributor base
        LEAST(
            100.0,
            (LEAST(stars_count, 50000)::NUMERIC / 500.0 * 0.5) +
            (LEAST(forks_count, 20000)::NUMERIC / 200.0 * 0.3) +
            (LEAST(active_contributors_count, 50)::NUMERIC * 2.0 * 0.2)
        ) AS community_engagement_score
    FROM joined_metrics
),

calculated_totals AS (
    SELECT
        *,
        -- Composite Health Score (Weighted: 40% Issue turnaround, 35% Commits, 25% Community)
        ROUND(
            LEAST(
                100.0,
                GREATEST(
                    0.0,
                    (0.40 * issue_velocity_score) +
                    (0.35 * commit_cadence_score) +
                    (0.25 * community_engagement_score)
                )
            )::NUMERIC,
            2
        ) AS health_score
    FROM health_scoring
)

SELECT
    repo_id,
    repo_name,
    full_name,
    organization,
    description,
    html_url,
    primary_language,
    stars_count,
    forks_count,
    open_issues_count,
    repo_age_days,
    days_since_last_push,
    total_issues_tracked,
    closed_issues_count,
    avg_issue_close_time_days,
    median_issue_close_time_days,
    issue_closure_rate_pct,
    total_commits_tracked,
    commits_last_30d,
    commits_last_90d,
    active_contributors_count,
    latest_commit_date,
    days_since_latest_commit,
    health_score,
    CASE
        WHEN health_score >= 90.0 THEN 'A+'
        WHEN health_score >= 80.0 THEN 'A'
        WHEN health_score >= 70.0 THEN 'B'
        WHEN health_score >= 60.0 THEN 'C'
        ELSE 'Needs Attention'
    END AS health_grade,
    -- Window Functions: Comparative Rankings within Red Hat Ecosystem
    DENSE_RANK() OVER (ORDER BY health_score DESC) AS ecosystem_health_rank,
    ROUND(PERCENT_RANK() OVER (ORDER BY health_score)::NUMERIC, 3) AS health_percentile,
    ROUND(AVG(health_score) OVER (PARTITION BY organization)::NUMERIC, 2) AS org_avg_health_score,
    COUNT(*) OVER (PARTITION BY organization) AS org_tracked_repo_count,
    CURRENT_TIMESTAMP AS calculated_at
FROM calculated_totals
