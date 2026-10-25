"""
Unit Test Suite for LLM Health Summarizer.
Tests prompt assembly, heuristic 2-line generation, and metric parsing.
"""

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "airflow", "dags", "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from llm_summarizer import LLMHealthSummarizer  # noqa: E402


def test_heuristic_summary_format():
    """Validates that heuristic summary produces 2-line executive summary and operational risk."""
    summarizer = LLMHealthSummarizer()

    sample_metrics = {
        "repo_id": 14001,
        "repo_name": "ansible",
        "organization": "ansible",
        "health_score": 88.5,
        "health_grade": "A",
        "stars_count": 61200,
        "forks_count": 24500,
        "open_issues_count": 1200,
        "avg_issue_close_time_days": 4.2,
        "commits_last_30d": 35,
        "active_contributors_count": 18,
    }

    result = summarizer.summarize_repo(sample_metrics)

    assert "executive_summary" in result
    assert "operational_risk" in result
    assert result["provider"] in ("heuristic", "google-gemini", "anthropic")

    summary = result["executive_summary"]
    assert "ansible" in summary
    assert "88.5" in summary or "A" in summary

    # Verify that the summary has 2 sentences
    sentences = [s.strip() for s in summary.split(".") if s.strip()]
    assert len(sentences) >= 2, f"Expected 2-line summary, received: {summary}"


def test_heuristic_summary_lagging_issues():
    """Validates risk recommendation when issue turnaround is slow."""
    summarizer = LLMHealthSummarizer()

    slow_metrics = {
        "repo_id": 99999,
        "repo_name": "legacy-adapter",
        "organization": "openshift",
        "health_score": 58.0,
        "health_grade": "Needs Attention",
        "stars_count": 300,
        "forks_count": 50,
        "open_issues_count": 140,
        "avg_issue_close_time_days": 28.5,
        "commits_last_30d": 4,
        "active_contributors_count": 2,
    }

    result = summarizer.summarize_repo(slow_metrics)
    assert "triage" in result["operational_risk"].lower() or "backlog" in result["operational_risk"].lower()
