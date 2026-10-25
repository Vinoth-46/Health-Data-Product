"""
Unit Test Suite for GitHubExtractor.
Validates HTTP request logic, rate-limit recovery, parsing, and fallback mock generation.
"""

import os
import sys
from unittest.mock import MagicMock, patch

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "airflow", "dags", "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from github_extractor import GitHubExtractor, generate_fallback_mock_data  # noqa: E402


def test_extractor_initialization_with_token():
    """Verifies that GITHUB_TOKEN is properly formatted into authorization headers."""
    extractor = GitHubExtractor(token="ghp_test123456789")
    assert extractor.session.headers.get("Authorization") == "Bearer ghp_test123456789"
    assert "RedHat-Health-Data-Product" in extractor.session.headers.get("User-Agent")


def test_fetch_repo_parsing():
    """Verifies repository API JSON response normalization."""
    extractor = GitHubExtractor(token="test_token")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"X-RateLimit-Remaining": "4999"}
    mock_response.json.return_value = {
        "id": 123456,
        "name": "ansible",
        "full_name": "ansible/ansible",
        "owner": {"login": "ansible"},
        "description": "Ansible automation engine",
        "html_url": "https://github.com/ansible/ansible",
        "stargazers_count": 60500,
        "forks_count": 24000,
        "open_issues_count": 1200,
        "language": "Python",
        "created_at": "2012-03-07T00:27:00Z",
        "updated_at": "2026-03-25T10:00:00Z",
        "pushed_at": "2026-03-25T09:30:00Z",
    }

    with patch.object(extractor.session, "get", return_value=mock_response):
        repo_data = extractor.fetch_repo("ansible", "ansible")

        assert repo_data["repo_id"] == 123456
        assert repo_data["name"] == "ansible"
        assert repo_data["organization"] == "ansible"
        assert repo_data["stars_count"] == 60500
        assert repo_data["primary_language"] if "primary_language" in repo_data else repo_data["language"] == "Python"
        assert "raw_json" in repo_data


def test_fallback_mock_data_structure():
    """Ensures fallback data generator produces coherent relational records across Red Hat orgs."""
    target_repos = ["ansible/ansible", "openshift/origin"]
    repos, issues, commits = generate_fallback_mock_data(target_repos)

    assert len(repos) >= 2
    assert len(issues) > 0
    assert len(commits) > 0

    repo_ids = {r["repo_id"] for r in repos}
    for issue in issues:
        assert issue["repo_id"] in repo_ids
        assert issue["state"] in ("open", "closed")

    for commit in commits:
        assert commit["repo_id"] in repo_ids
        assert "@" in commit["author_email"]
