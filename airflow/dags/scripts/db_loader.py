"""
Database Loader for Health Data Product Raw Layer.
Performs idempotent UPSERT operations into PostgreSQL raw schema.
"""

import logging
import os
from typing import Any, Dict, List

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None
    execute_values = None

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_db_connection():
    """Establishes connection to PostgreSQL analytics data warehouse."""
    host = os.environ.get("ANALYTICS_HOST", "postgres")
    port = int(os.environ.get("ANALYTICS_PORT", "5432"))
    dbname = os.environ.get("ANALYTICS_DB", "analytics_dw")
    user = os.environ.get("ANALYTICS_USER", "analytics_user")
    password = os.environ.get("ANALYTICS_PASSWORD", "analytics_password")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10,
    )


def ensure_raw_schema_exists(conn=None) -> None:
    """Ensures raw schema and raw tables are created in the database."""
    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS governance;")

            cur.execute("""
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
            """)

            cur.execute("""
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
            """)

            cur.execute("""
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
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS analytics.repo_health_llm_summary (
                    summary_id SERIAL PRIMARY KEY,
                    repo_id BIGINT NOT NULL,
                    repo_name VARCHAR(255) NOT NULL,
                    health_score NUMERIC(5,2),
                    health_grade VARCHAR(10),
                    executive_summary TEXT NOT NULL,
                    operational_risk TEXT NOT NULL,
                    llm_provider VARCHAR(50) NOT NULL,
                    llm_model VARCHAR(100) NOT NULL,
                    generated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    raw_response JSONB
                );
            """)
            conn.commit()
            logger.info("Raw and Analytics schemas/tables verified.")
    finally:
        if should_close:
            conn.close()


def load_repos(repos: List[Dict[str, Any]], conn=None) -> int:
    """Upserts repository records into raw.raw_repos."""
    if not repos:
        return 0

    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        ensure_raw_schema_exists(conn)
        with conn.cursor() as cur:
            query = """
                INSERT INTO raw.raw_repos (
                    repo_id, name, full_name, organization, description, html_url,
                    stars_count, forks_count, open_issues_count, language,
                    created_at, updated_at, pushed_at, ingested_at, raw_json
                ) VALUES %s
                ON CONFLICT (repo_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    full_name = EXCLUDED.full_name,
                    organization = EXCLUDED.organization,
                    description = EXCLUDED.description,
                    html_url = EXCLUDED.html_url,
                    stars_count = EXCLUDED.stars_count,
                    forks_count = EXCLUDED.forks_count,
                    open_issues_count = EXCLUDED.open_issues_count,
                    language = EXCLUDED.language,
                    updated_at = EXCLUDED.updated_at,
                    pushed_at = EXCLUDED.pushed_at,
                    ingested_at = EXCLUDED.ingested_at,
                    raw_json = EXCLUDED.raw_json;
            """
            values = [
                (
                    r["repo_id"],
                    r["name"],
                    r["full_name"],
                    r["organization"],
                    r["description"],
                    r["html_url"],
                    r["stars_count"],
                    r["forks_count"],
                    r["open_issues_count"],
                    r["language"],
                    r["created_at"],
                    r["updated_at"],
                    r["pushed_at"],
                    r["ingested_at"],
                    r["raw_json"],
                )
                for r in repos
            ]
            execute_values(cur, query, values)
            conn.commit()
            logger.info(f"Successfully upserted {len(repos)} records into raw.raw_repos.")
            return len(repos)
    finally:
        if should_close:
            conn.close()


def load_issues(issues: List[Dict[str, Any]], conn=None) -> int:
    """Upserts issues and PR records into raw.raw_issues."""
    if not issues:
        return 0

    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        ensure_raw_schema_exists(conn)
        with conn.cursor() as cur:
            query = """
                INSERT INTO raw.raw_issues (
                    issue_id, repo_id, repo_full_name, issue_number, title, state,
                    comments_count, is_pull_request, author_login,
                    created_at, closed_at, updated_at, ingested_at, raw_json
                ) VALUES %s
                ON CONFLICT (issue_id) DO UPDATE SET
                    repo_id = EXCLUDED.repo_id,
                    repo_full_name = EXCLUDED.repo_full_name,
                    issue_number = EXCLUDED.issue_number,
                    title = EXCLUDED.title,
                    state = EXCLUDED.state,
                    comments_count = EXCLUDED.comments_count,
                    is_pull_request = EXCLUDED.is_pull_request,
                    author_login = EXCLUDED.author_login,
                    closed_at = EXCLUDED.closed_at,
                    updated_at = EXCLUDED.updated_at,
                    ingested_at = EXCLUDED.ingested_at,
                    raw_json = EXCLUDED.raw_json;
            """
            values = [
                (
                    i["issue_id"],
                    i["repo_id"],
                    i["repo_full_name"],
                    i["issue_number"],
                    i["title"],
                    i["state"],
                    i["comments_count"],
                    i["is_pull_request"],
                    i["author_login"],
                    i["created_at"],
                    i["closed_at"],
                    i["updated_at"],
                    i["ingested_at"],
                    i["raw_json"],
                )
                for i in issues
            ]
            execute_values(cur, query, values)
            conn.commit()
            logger.info(f"Successfully upserted {len(issues)} records into raw.raw_issues.")
            return len(issues)
    finally:
        if should_close:
            conn.close()


def load_commits(commits: List[Dict[str, Any]], conn=None) -> int:
    """Upserts commit history into raw.raw_commits."""
    if not commits:
        return 0

    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    try:
        ensure_raw_schema_exists(conn)
        with conn.cursor() as cur:
            query = """
                INSERT INTO raw.raw_commits (
                    commit_sha, repo_id, repo_full_name, author_name, author_email,
                    committer_name, committer_email, commit_message, commit_date,
                    ingested_at, raw_json
                ) VALUES %s
                ON CONFLICT (commit_sha) DO UPDATE SET
                    repo_id = EXCLUDED.repo_id,
                    repo_full_name = EXCLUDED.repo_full_name,
                    author_name = EXCLUDED.author_name,
                    author_email = EXCLUDED.author_email,
                    committer_name = EXCLUDED.committer_name,
                    committer_email = EXCLUDED.committer_email,
                    commit_message = EXCLUDED.commit_message,
                    commit_date = EXCLUDED.commit_date,
                    ingested_at = EXCLUDED.ingested_at,
                    raw_json = EXCLUDED.raw_json;
            """
            values = [
                (
                    c["commit_sha"],
                    c["repo_id"],
                    c["repo_full_name"],
                    c["author_name"],
                    c["author_email"],
                    c["committer_name"],
                    c["committer_email"],
                    c["commit_message"],
                    c["commit_date"],
                    c["ingested_at"],
                    c["raw_json"],
                )
                for c in commits
            ]
            execute_values(cur, query, values)
            conn.commit()
            logger.info(f"Successfully upserted {len(commits)} records into raw.raw_commits.")
            return len(commits)
    finally:
        if should_close:
            conn.close()
