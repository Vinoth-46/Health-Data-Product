"""
LLM Health Summary Generator for GitHub Repositories.
Supports Google Gemini, Anthropic Claude, and high-fidelity heuristic fallback.
Stores executive 2-line summaries and operational risks into analytics.repo_health_llm_summary.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class LLMHealthSummarizer:
    """Manages prompt engineering, LLM API calls, and persistence of repo health summaries."""

    def __init__(
        self,
        gemini_api_key: Optional[str] = None,
        anthropic_api_key: Optional[str] = None,
        model_name: Optional[str] = None,
    ):
        self.gemini_key = gemini_api_key or os.environ.get("GEMINI_API_KEY", "").strip()
        self.anthropic_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY", "").strip()
        self.model_name = model_name or os.environ.get("LLM_MODEL", "gemini-2.5-flash")

    def _call_gemini(self, prompt: str) -> Dict[str, str]:
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{self.model_name}:generateContent"
            f"?key={self.gemini_key}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 300,
                "responseMimeType": "application/json",
            },
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        raw_text = res_json["candidates"][0]["content"]["parts"][0]["text"]
        parsed = json.loads(raw_text)
        return {
            "executive_summary": parsed.get("executive_summary", "").strip(),
            "operational_risk": parsed.get("operational_risk", "").strip(),
            "raw_response": res_json,
        }

    def _call_anthropic(self, prompt: str) -> Dict[str, str]:
        """Calls Anthropic Claude API."""
        url = "https://api.anthropic.com/v1/messages"
        payload = {
            "model": "claude-3-5-sonnet-20241022",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}],
        }
        headers = {
            "x-api-key": self.anthropic_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        raw_text = res_json["content"][0]["text"]
        parsed = json.loads(raw_text)
        return {
            "executive_summary": parsed.get("executive_summary", "").strip(),
            "operational_risk": parsed.get("operational_risk", "").strip(),
            "raw_response": res_json,
        }

    def _generate_heuristic_summary(self, metrics: Dict[str, Any]) -> Dict[str, str]:
        """Generates deterministic 2-line executive summary and operational risk based on metrics."""
        name = metrics.get("repo_name", "Repository")
        score = float(metrics.get("health_score", 75.0))
        grade = metrics.get("health_grade", "B")
        close_days = float(metrics.get("avg_issue_close_time_days", 5.0) or 5.0)
        commits_30d = int(metrics.get("commits_last_30d", 20) or 20)
        stars = int(metrics.get("stars_count", 1000) or 1000)
        contributors = int(metrics.get("active_contributors_count", 5) or 5)

        # 2-line executive summary
        line1 = (
            f"{name} demonstrates {grade}-grade operational health with a composite score of {score:.1f}/100, "
            f"backed by {stars:,} community stars and {contributors} active contributors."
        )
        if close_days <= 7.0 and commits_30d >= 15:
            line2 = (
                f"Engineering velocity is high with an efficient {close_days:.1f}-day average issue turnaround "
                f"and sustained momentum across {commits_30d} recent commits."
            )
            risk = "Low operational risk; maintain current reviewer SLAs to preserve rapid release cycles."
        elif close_days > 14.0:
            line2 = (
                f"Issue resolution turnaround is lagging at {close_days:.1f} days despite active commit cadence, "
                f"indicating potential triaging bottlenecks."
            )
            risk = "Triage backlog risk; implement automated bot triage and dedicated weekly triage shifts."
        else:
            line2 = (
                f"Project maintains moderate developer velocity with steady cadence across {commits_30d} commits "
                f"and balanced issue resolution."
            )
            risk = "Moderate dependency risk; expand contributor base to distribute code review load."

        summary = f"{line1} {line2}"
        return {
            "executive_summary": summary,
            "operational_risk": risk,
            "raw_response": {"mode": "heuristic_fallback", "metrics_evaluated": metrics},
        }

    def summarize_repo(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generates the health summary using available LLM API or smart fallback."""
        prompt = f"""
You are an expert Data & Engineering Velocity Analyst evaluating open-source repositories from the Red Hat ecosystem.
Analyze the following production metrics for repository '{metrics.get("repo_name")}':

- Health Score: {metrics.get("health_score")}/100
- Health Grade: {metrics.get("health_grade")}
- Stars: {metrics.get("stars_count")}
- Forks: {metrics.get("forks_count")}
- Open Issues: {metrics.get("open_issues_count")}
- Avg Issue Resolution Time: {metrics.get("avg_issue_close_time_days")} days
- Commits in last 30 days: {metrics.get("commits_last_30d")}
- Active Contributors: {metrics.get("active_contributors_count")}

Provide a JSON object with exactly two keys:
1. "executive_summary": Exactly two sentences summarizing engineering health, velocity, and sustainability.
2. "operational_risk": Exactly one actionable recommendation or risk mitigation for project maintainers.

Respond ONLY with valid JSON.
"""
        provider = "heuristic"
        model = "rule-augmented-v1"

        if self.gemini_key:
            try:
                logger.info(f"Calling Gemini API for {metrics.get('repo_name')}...")
                res = self._call_gemini(prompt)
                provider = "google-gemini"
                model = self.model_name
                return {**res, "provider": provider, "model": model}
            except Exception as e:
                logger.warning(f"Gemini API call failed ({e}). Falling back to heuristic summary.")

        if self.anthropic_key:
            try:
                logger.info(f"Calling Anthropic API for {metrics.get('repo_name')}...")
                res = self._call_anthropic(prompt)
                provider = "anthropic"
                model = "claude-3-5-sonnet"
                return {**res, "provider": provider, "model": model}
            except Exception as e:
                logger.warning(f"Anthropic API call failed ({e}). Falling back to heuristic summary.")

        # Fallback
        res = self._generate_heuristic_summary(metrics)
        return {**res, "provider": provider, "model": model}


def fetch_repo_health_scores(conn) -> List[Dict[str, Any]]:
    """Fetches the latest repo_health_score data product records."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                repo_id,
                repo_name,
                organization,
                health_score,
                health_grade,
                stars_count,
                forks_count,
                open_issues_count,
                avg_issue_close_time_days,
                commits_last_30d,
                active_contributors_count
            FROM analytics.repo_health_score
            ORDER BY health_score DESC;
        """)
        return [dict(row) for row in cur.fetchall()]


def store_llm_summaries(conn, summaries: List[Dict[str, Any]]) -> int:
    """Stores generated LLM summaries into analytics.repo_health_llm_summary."""
    with conn.cursor() as cur:
        for s in summaries:
            cur.execute("""
                INSERT INTO analytics.repo_health_llm_summary (
                    repo_id, repo_name, health_score, health_grade,
                    executive_summary, operational_risk, llm_provider, llm_model,
                    generated_at, raw_response
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                s["repo_id"],
                s["repo_name"],
                s["health_score"],
                s["health_grade"],
                s["executive_summary"],
                s["operational_risk"],
                s["provider"],
                s["model"],
                datetime.now(timezone.utc),
                json.dumps(s["raw_response"]),
            ))
        conn.commit()
    logger.info(f"Stored {len(summaries)} LLM health summaries into analytics.repo_health_llm_summary.")
    return len(summaries)


def run_llm_summaries():
    """Main execution entrypoint for Airflow task and standalone runner."""
    from airflow.dags.scripts.db_loader import get_db_connection, ensure_raw_schema_exists

    conn = get_db_connection()
    try:
        ensure_raw_schema_exists(conn)
        scores = fetch_repo_health_scores(conn)
        if not scores:
            logger.warning("No records found in analytics.repo_health_score. Skipping LLM summaries.")
            return

        summarizer = LLMHealthSummarizer()
        results = []
        for metric in scores:
            res = summarizer.summarize_repo(metric)
            results.append({
                "repo_id": metric["repo_id"],
                "repo_name": metric["repo_name"],
                "health_score": metric["health_score"],
                "health_grade": metric["health_grade"],
                **res,
            })

        store_llm_summaries(conn, results)
    finally:
        conn.close()


if __name__ == "__main__":
    run_llm_summaries()
