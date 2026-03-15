from __future__ import annotations

from datetime import datetime, timedelta, timezone

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class LibrariesIOCollector(BaseCollector):
    """GitHub daily new repository count (proxy for OSS activity).

    Uses the GitHub Search API to count repositories created on each
    of the last 7 days. No authentication required for low-rate usage.
    Replaces the former Libraries.io API which now requires a paid key.
    """

    meta = CollectorMeta(
        name="librariesio_new_packages",
        display_name="GitHub Daily New Repositories",
        update_frequency="daily",
        api_docs_url="https://docs.github.com/en/rest/search",
        domain="technology",
        category="developer",
    )

    SEARCH_URL = "https://api.github.com/search/repositories"

    def fetch(self) -> pd.DataFrame:
        rows = []
        today = datetime.now(timezone.utc).date()
        headers = {"Accept": "application/vnd.github.v3+json"}
        for days_ago in range(7, 0, -1):
            target = today - timedelta(days=days_ago)
            date_str = target.isoformat()
            resp = requests.get(
                self.SEARCH_URL,
                params={"q": f"created:{date_str}", "per_page": "1"},
                headers=headers,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            total = resp.json().get("total_count", 0)
            rows.append({
                "date": pd.Timestamp(date_str, tz="UTC"),
                "value": float(total),
            })
        if not rows:
            raise RuntimeError("No GitHub search data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)


# (language, collector_name, display_name)
_LANG_REPOS: list[tuple[str, str, str]] = [
    ("python", "gh_repos_python", "GitHub Daily New Python Repos"),
    ("javascript", "gh_repos_javascript", "GitHub Daily New JavaScript Repos"),
    ("typescript", "gh_repos_typescript", "GitHub Daily New TypeScript Repos"),
    ("rust", "gh_repos_rust", "GitHub Daily New Rust Repos"),
    ("go", "gh_repos_go", "GitHub Daily New Go Repos"),
    ("java", "gh_repos_java", "GitHub Daily New Java Repos"),
]


def _make_lang_repo_collector(
    language: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://docs.github.com/en/rest/search",
            domain="technology",
            category="developer",
        )

        SEARCH_URL = "https://api.github.com/search/repositories"

        def fetch(self) -> pd.DataFrame:
            rows = []
            today = datetime.now(timezone.utc).date()
            headers = {"Accept": "application/vnd.github.v3+json"}
            for days_ago in range(7, 0, -1):
                target = today - timedelta(days=days_ago)
                date_str = target.isoformat()
                resp = requests.get(
                    self.SEARCH_URL,
                    params={
                        "q": f"created:{date_str} language:{language}",
                        "per_page": "1",
                    },
                    headers=headers,
                    timeout=self.config.request_timeout,
                )
                resp.raise_for_status()
                total = resp.json().get("total_count", 0)
                rows.append({
                    "date": pd.Timestamp(date_str, tz="UTC"),
                    "value": float(total),
                })
            if not rows:
                raise RuntimeError(f"No GitHub search data for {language}")
            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"GHRepos_{name}"
    _Collector.__qualname__ = f"GHRepos_{name}"
    return _Collector


def get_lang_repo_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_lang_repo_collector(lang, name, display)
        for lang, name, display in _LANG_REPOS
    }
