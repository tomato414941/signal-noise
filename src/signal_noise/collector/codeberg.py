"""Codeberg FOSS hosting stats.

Tracks total repository count on Codeberg, the primary
community-run Gitea-based alternative to GitHub. Growth
reflects FOSS decentralization trends.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://codeberg.org/api/v1"


class CodebergRepoCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="codeberg_repos",
        display_name="Codeberg Total Repositories",
        update_frequency="daily",
        api_docs_url="https://codeberg.org/api/swagger",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_API_URL}/repos/search",
            params={"limit": "1"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.headers.get("X-Total-Count")
        if count is None:
            raise RuntimeError("No Codeberg repo count in response headers")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
