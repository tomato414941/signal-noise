from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GitLabProjectsCollector(BaseCollector):
    """GitLab public projects created recently (daily snapshot)."""

    meta = CollectorMeta(
        name="gitlab_new_projects",
        display_name="GitLab New Public Projects (daily)",
        update_frequency="daily",
        api_docs_url="https://docs.gitlab.com/ee/api/projects.html",
        domain="technology",
        category="developer",
    )

    URL = "https://gitlab.com/api/v4/projects?order_by=created_at&sort=desc&per_page=100&simple=true"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        projects = resp.json()
        if not projects:
            raise RuntimeError("No GitLab data")
        # Count projects created per day
        date_counts: dict[str, int] = {}
        for p in projects:
            created = p.get("created_at", "")[:10]
            if created:
                date_counts[created] = date_counts.get(created, 0) + 1
        rows = [
            {"date": pd.Timestamp(d, tz="UTC"), "value": float(c)}
            for d, c in date_counts.items()
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
