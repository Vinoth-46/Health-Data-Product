-- ==============================================================================
-- Database & Schema Initialization Script for Health Data Product
-- Executed on container startup by PostgreSQL entrypoint
-- ==============================================================================

-- 1. Create Analytics Data Warehouse Database if not present
SELECT 'CREATE DATABASE analytics_dw'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'analytics_dw')\gexec

-- 2. Create Analytics User and configure permissions
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analytics_user') THEN
        CREATE USER analytics_user WITH ENCRYPTED PASSWORD 'analytics_password';
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE analytics_dw TO analytics_user;
GRANT ALL PRIVILEGES ON DATABASE analytics_dw TO airflow;

-- 3. Connect to analytics_dw to setup schemas and tables
\connect analytics_dw

-- Grant schema creation rights
GRANT ALL ON SCHEMA public TO analytics_user;
GRANT ALL ON SCHEMA public TO airflow;

-- Create target project schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS governance;

GRANT ALL ON SCHEMA raw TO analytics_user, airflow;
GRANT ALL ON SCHEMA staging TO analytics_user, airflow;
GRANT ALL ON SCHEMA analytics TO analytics_user, airflow;
GRANT ALL ON SCHEMA governance TO analytics_user, airflow;

-- Set default permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO analytics_user, airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO analytics_user, airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO analytics_user, airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA governance GRANT ALL ON TABLES TO analytics_user, airflow;

-- 4. Create Raw Source Tables
-- Raw Repositories
CREATE TABLE IF NOT EXISTS raw.raw_repos (
    repo_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    organization VARCHAR(255) NOT NULL,
    description TEXT,
    html_url TEXT,
    stars_count INT DEFAULT 0,
    forks_count INT DEFAULT 0,
    open_issues_count INT DEFAULT 0,
    language VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    pushed_at TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    raw_json JSONB
);

-- Raw Issues
CREATE TABLE IF NOT EXISTS raw.raw_issues (
    issue_id BIGINT PRIMARY KEY,
    repo_id BIGINT NOT NULL,
    repo_full_name VARCHAR(255) NOT NULL,
    issue_number INT NOT NULL,
    title TEXT,
    state VARCHAR(50) NOT NULL,
    comments_count INT DEFAULT 0,
    is_pull_request BOOLEAN DEFAULT FALSE,
    author_login VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    closed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    raw_json JSONB
);

-- Raw Commits
CREATE TABLE IF NOT EXISTS raw.raw_commits (
    commit_sha VARCHAR(64) PRIMARY KEY,
    repo_id BIGINT NOT NULL,
    repo_full_name VARCHAR(255) NOT NULL,
    author_name VARCHAR(255),
    author_email VARCHAR(255),
    committer_name VARCHAR(255),
    committer_email VARCHAR(255),
    commit_message TEXT,
    commit_date TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    raw_json JSONB
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_issues_repo_id ON raw.raw_issues(repo_id);
CREATE INDEX IF NOT EXISTS idx_raw_commits_repo_id ON raw.raw_commits(repo_id);
CREATE INDEX IF NOT EXISTS idx_raw_commits_date ON raw.raw_commits(commit_date);
