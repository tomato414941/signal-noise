from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GitHubRepoCountCollector(BaseCollector):
    """Daily new public GitHub repositories created (sampled).

    Uses the GitHub Search API to count repositories created
    on each date. Rate-limited to 10 requests/minute without auth.
    """

    meta = CollectorMeta(
        name="github_new_repos",
        display_name="GitHub New Public Repos (daily)",
        update_frequency="daily",
        api_docs_url="https://docs.github.com/en/rest/search/search",
        domain="technology",
        category="developer",
    )

    URL = "https://api.github.com/search/repositories?q=created:{date}&per_page=1"

    def fetch(self) -> pd.DataFrame:
        headers = {"Accept": "application/vnd.github.v3+json"}
        rows = []
        end = pd.Timestamp.now(tz="UTC").normalize()
        for i in range(1, 31):
            dt = end - pd.Timedelta(days=i)
            date_str = dt.strftime("%Y-%m-%d")
            try:
                url = self.URL.format(date=date_str)
                resp = requests.get(
                    url, headers=headers, timeout=self.config.request_timeout,
                )
                if resp.status_code == 403:
                    break  # rate limited
                resp.raise_for_status()
                count = resp.json().get("total_count", 0)
                rows.append({"date": dt, "value": float(count)})
            except Exception:
                continue
        if not rows:
            raise RuntimeError("No GitHub repo count data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
