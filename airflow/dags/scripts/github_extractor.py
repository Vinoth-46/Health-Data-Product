"""
GitHub REST API Extractor for Red Hat Ecosystem Repositories.
Handles authentication, rate limiting, exponential backoff retries, and data parsing.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import requests

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class GitHubAPIError(Exception):
    """Custom exception for GitHub API errors."""
    pass


class GitHubRateLimitError(GitHubAPIError):
    """Exception raised when GitHub rate limit is exceeded."""
    pass


class GitHubExtractor:
    """Robust extractor for GitHub REST API with retries and rate limit management."""

    BASE_URL = "https://api.github.com"

    def __init__(
        self,
        token: Optional[str] = None,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        timeout: int = 30,
    ):
        self.token = token or os.environ.get("GITHUB_TOKEN", "").strip()
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.session = requests.Session()

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "RedHat-Health-Data-Product/1.0",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
            logger.info("GitHubExtractor initialized with authenticated token.")
        else:
            logger.warning(
                "GitHubExtractor initialized without GITHUB_TOKEN. "
                "Rate limit is 60 requests/hr. Provide GITHUB_TOKEN for 5,000 requests/hr."
            )

        self.session.headers.update(headers)

    def _request_with_retry(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Any, Dict[str, str]]:
        """Makes an HTTP GET request with exponential backoff retries and rate limit handling."""
        url = f"{self.BASE_URL}{endpoint}" if endpoint.startswith("/") else endpoint
        attempt = 0

        while attempt <= self.max_retries:
            attempt += 1
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                headers = dict(response.headers)

                # Check rate limit headers
                remaining = int(headers.get("X-RateLimit-Remaining", -1))
                if remaining == 0:
                    reset_time = int(headers.get("X-RateLimit-Reset", 0))
                    current_time = int(time.time())
                    wait_seconds = max(reset_time - current_time, 1)
                    logger.warning(
                        f"GitHub rate limit exceeded. Reset in {wait_seconds}s. Remaining: {remaining}"
                    )
                    if attempt > self.max_retries:
                        raise GitHubRateLimitError(f"GitHub rate limit hit. Reset in {wait_seconds}s.")

                if response.status_code == 200:
                    return response.json(), headers

                # Retry on rate limiting (429, 403 secondary limit) or server errors (5xx)
                if response.status_code in (429, 500, 502, 503, 504) or (
                    response.status_code == 403 and "rate limit" in response.text.lower()
                ):
                    sleep_time = (self.backoff_factor ** attempt) + (attempt * 0.5)
                    logger.warning(
                        f"Request to {url} returned HTTP {response.status_code}. "
                        f"Retrying in {sleep_time:.2f}s (Attempt {attempt}/{self.max_retries})..."
                    )
                    time.sleep(sleep_time)
                    continue

                # Client error (e.g. 404, invalid token)
                response.raise_for_status()

            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt > self.max_retries:
                    logger.error(f"Network error calling {url} after {attempt} attempts: {e}")
                    raise GitHubAPIError(f"Network connection failed: {e}") from e
                sleep_time = self.backoff_factor ** attempt
                logger.warning(f"Connection error: {e}. Retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)

        raise GitHubAPIError(f"Failed to fetch {url} after {self.max_retries} attempts.")

    def fetch_repo(self, owner: str, repo: str) -> Dict[str, Any]:
        """Fetches metadata for a single repository."""
        logger.info(f"Extracting repository: {owner}/{repo}")
        endpoint = f"/repos/{owner}/{repo}"
        try:
            data, _ = self._request_with_retry(endpoint)
            return {
                "repo_id": data.get("id"),
                "name": data.get("name"),
                "full_name": data.get("full_name"),
                "organization": data.get("owner", {}).get("login"),
                "description": data.get("description"),
                "html_url": data.get("html_url"),
                "stars_count": data.get("stargazers_count", 0),
                "forks_count": data.get("forks_count", 0),
                "open_issues_count": data.get("open_issues_count", 0),
                "language": data.get("language") or "Unknown",
                "created_at": data.get("created_at"),
                "updated_at": data.get("updated_at"),
                "pushed_at": data.get("pushed_at"),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "raw_json": json.dumps(data),
            }
        except Exception as e:
            logger.error(f"Error fetching repo {owner}/{repo}: {e}")
            raise

    def fetch_issues(
        self, owner: str, repo: str, repo_id: int, per_page: int = 50, max_pages: int = 1
    ) -> List[Dict[str, Any]]:
        """Fetches issues and PRs for a repository (state=all)."""
        logger.info(f"Extracting issues for: {owner}/{repo} (up to {per_page * max_pages} records)")
        endpoint = f"/repos/{owner}/{repo}/issues"
        issues_list = []

        for page in range(1, max_pages + 1):
            params = {
                "state": "all",
                "per_page": min(per_page, 100),
                "page": page,
                "sort": "updated",
                "direction": "desc",
            }
            try:
                data, _ = self._request_with_retry(endpoint, params=params)
                if not data:
                    break

                for item in data:
                    is_pr = "pull_request" in item
                    issues_list.append({
                        "issue_id": item.get("id"),
                        "repo_id": repo_id,
                        "repo_full_name": f"{owner}/{repo}",
                        "issue_number": item.get("number"),
                        "title": item.get("title"),
                        "state": item.get("state"),
                        "comments_count": item.get("comments", 0),
                        "is_pull_request": is_pr,
                        "author_login": item.get("user", {}).get("login") if item.get("user") else "unknown",
                        "created_at": item.get("created_at"),
                        "closed_at": item.get("closed_at"),
                        "updated_at": item.get("updated_at"),
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                        "raw_json": json.dumps(item),
                    })

            except Exception as e:
                logger.error(f"Error fetching issues page {page} for {owner}/{repo}: {e}")
                if issues_list:
                    # Return whatever we gathered before error
                    return issues_list
                raise

        logger.info(f"Retrieved {len(issues_list)} issues for {owner}/{repo}")
        return issues_list

    def fetch_commits(
        self, owner: str, repo: str, repo_id: int, per_page: int = 50, max_pages: int = 1
    ) -> List[Dict[str, Any]]:
        """Fetches commit history for a repository."""
        logger.info(f"Extracting commits for: {owner}/{repo} (up to {per_page * max_pages} records)")
        endpoint = f"/repos/{owner}/{repo}/commits"
        commits_list = []

        for page in range(1, max_pages + 1):
            params = {"per_page": min(per_page, 100), "page": page}
            try:
                data, _ = self._request_with_retry(endpoint, params=params)
                if not data:
                    break

                for item in data:
                    commit_obj = item.get("commit", {})
                    author_obj = commit_obj.get("author", {})
                    committer_obj = commit_obj.get("committer", {})

                    commit_date = author_obj.get("date") or committer_obj.get("date")
                    if not commit_date:
                        commit_date = datetime.now(timezone.utc).isoformat()

                    commits_list.append({
                        "commit_sha": item.get("sha"),
                        "repo_id": repo_id,
                        "repo_full_name": f"{owner}/{repo}",
                        "author_name": author_obj.get("name") or "Unknown Author",
                        "author_email": author_obj.get("email") or "unknown@example.com",
                        "committer_name": committer_obj.get("name") or "Unknown Committer",
                        "committer_email": committer_obj.get("email") or "unknown@example.com",
                        "commit_message": commit_obj.get("message", "")[:2000],  # Truncate very long messages
                        "commit_date": commit_date,
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                        "raw_json": json.dumps(item),
                    })

            except Exception as e:
                logger.error(f"Error fetching commits page {page} for {owner}/{repo}: {e}")
                if commits_list:
                    return commits_list
                raise

        logger.info(f"Retrieved {len(commits_list)} commits for {owner}/{repo}")
        return commits_list


def generate_fallback_mock_data(target_repos: List[str]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Generates realistic synthetic data for offline or rate-limited environments."""
    logger.info("Generating deterministic fallback data for Red Hat ecosystem repos...")
    repos, issues, commits = [], [], []

    repo_configs = [
        {
            "id": 14001,
            "name": "ansible",
            "org": "ansible",
            "desc": "Ansible is a radically simple IT automation platform",
            "stars": 61200,
            "forks": 24500,
            "issues": 1200,
            "lang": "Python",
        },
        {
            "id": 14002,
            "name": "origin",
            "org": "openshift",
            "desc": "Conformance and end-to-end tests for OpenShift",
            "stars": 9400,
            "forks": 4800,
            "issues": 430,
            "lang": "Go",
        },
        {
            "id": 14003,
            "name": "vscode-yaml",
            "org": "redhat-developer",
            "desc": "Language Support for YAML Files in VS Code",
            "stars": 1250,
            "forks": 380,
            "issues": 85,
            "lang": "TypeScript",
        },
        {
            "id": 14004,
            "name": "awx",
            "org": "ansible",
            "desc": "AWX Project provides a modern web-UI and REST API for Ansible",
            "stars": 13900,
            "forks": 3500,
            "issues": 620,
            "lang": "Python",
        },
    ]

    now_iso = datetime.now(timezone.utc).isoformat()

    for idx, cfg in enumerate(repo_configs):
        full_name = f"{cfg['org']}/{cfg['name']}"
        repos.append({
            "repo_id": cfg["id"],
            "name": cfg["name"],
            "full_name": full_name,
            "organization": cfg["org"],
            "description": cfg["desc"],
            "html_url": f"https://github.com/{full_name}",
            "stars_count": cfg["stars"],
            "forks_count": cfg["forks"],
            "open_issues_count": cfg["issues"],
            "language": cfg["lang"],
            "created_at": "2018-01-15T10:00:00Z",
            "updated_at": now_iso,
            "pushed_at": now_iso,
            "ingested_at": now_iso,
            "raw_json": json.dumps({"name": cfg["name"]}),
        })

        # Add 15 mock issues per repo
        for i in range(1, 16):
            is_closed = (i % 3 != 0)
            closed_at = "2026-03-20T14:30:00Z" if is_closed else None
            issues.append({
                "issue_id": (cfg["id"] * 100) + i,
                "repo_id": cfg["id"],
                "repo_full_name": full_name,
                "issue_number": 1000 + i,
                "title": f"Optimization & feature enhancement #{i} for {cfg['name']}",
                "state": "closed" if is_closed else "open",
                "comments_count": (i * 2) % 11,
                "is_pull_request": (i % 2 == 0),
                "author_login": f"rh_developer_{i}",
                "created_at": "2026-03-15T08:00:00Z",
                "closed_at": closed_at,
                "updated_at": now_iso,
                "ingested_at": now_iso,
                "raw_json": json.dumps({"title": f"Issue {i}"}),
            })

        # Add 20 mock commits per repo
        authors = [
            ("Alice RedHat", "alice.redhat@redhat.com"),
            ("Bob OpenShift", "bob.openshift@redhat.com"),
            ("Carol Ansible", "carol.ansible@redhat.com"),
            ("Dave Developer", "dave.dev@redhat.com"),
        ]
        for c in range(1, 21):
            author_name, author_email = authors[c % len(authors)]
            commits.append({
                "commit_sha": f"sha{cfg['id']}{c:04d}abcdef01234567890abcdef12",
                "repo_id": cfg["id"],
                "repo_full_name": full_name,
                "author_name": author_name,
                "author_email": author_email,
                "committer_name": author_name,
                "committer_email": author_email,
                "commit_message": f"feat({cfg['name']}): Core stability and cluster performance improvement #{c}",
                "commit_date": f"2026-03-{min(c + 5, 28):02d}T12:00:00Z",
                "ingested_at": now_iso,
                "raw_json": json.dumps({"sha": f"sha{cfg['id']}{c}"}),
            })

    return repos, issues, commits
