from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class _GithubCommitCollector(BaseCollector):
    """Base for GitHub repo commit activity (weekly buckets from Stats API)."""

    _owner: str = ""
    _repo: str = ""

    # GitHub Stats API: returns weekly commit counts for the last year
    # No auth required for public repos (60 req/hour rate limit)
    URL_TEMPLATE = "https://api.github.com/repos/{owner}/{repo}/stats/participation"

    def fetch(self) -> pd.DataFrame:
        url = self.URL_TEMPLATE.format(owner=self._owner, repo=self._repo)
        resp = requests.get(
            url,
            headers={"Accept": "application/vnd.github.v3+json"},
            timeout=self.config.request_timeout,
        )
        # GitHub may return 202 (computing stats) on first request
        if resp.status_code == 202:
            raise RuntimeError(
                f"GitHub computing stats for {self._owner}/{self._repo}, retry later"
            )
        resp.raise_for_status()
        data = resp.json()

        # "all" = total commits per week (52 weeks), most recent last
        weekly_counts = data.get("all", [])
        if not weekly_counts:
            raise RuntimeError(f"No commit data for {self._owner}/{self._repo}")

        now = pd.Timestamp.now(tz="UTC").normalize()
        # Each entry is one week; index 51 = most recent week
        rows = []
        for i, count in enumerate(weekly_counts):
            weeks_ago = len(weekly_counts) - 1 - i
            date = now - pd.Timedelta(weeks=weeks_ago)
            rows.append({"date": date, "value": float(count)})

        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class BitcoinCommitsCollector(_GithubCommitCollector):
    _owner = "bitcoin"
    _repo = "bitcoin"
    meta = SourceMeta(
        name="github_bitcoin",
        display_name="GitHub bitcoin/bitcoin Weekly Commits",
        update_frequency="weekly",
        api_docs_url="https://docs.github.com/en/rest/metrics/statistics",
        domain="developer",
        category="developer",
    )


class EthereumCommitsCollector(_GithubCommitCollector):
    _owner = "ethereum"
    _repo = "go-ethereum"
    meta = SourceMeta(
        name="github_ethereum",
        display_name="GitHub ethereum/go-ethereum Weekly Commits",
        update_frequency="weekly",
        api_docs_url="https://docs.github.com/en/rest/metrics/statistics",
        domain="developer",
        category="developer",
    )
