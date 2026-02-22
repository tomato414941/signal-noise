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
        domain="developer",
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
