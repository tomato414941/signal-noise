from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class WikimediaTotalCollector(BaseCollector):
    """Total daily pageviews across all Wikimedia projects.

    Uses the Wikimedia REST API to fetch aggregate daily
    pageview counts for all projects, access types, and agents.
    """

    meta = CollectorMeta(
        name="wikimedia_pageview_total",
        display_name="Wikimedia Total Daily Pageviews",
        update_frequency="daily",
        api_docs_url="https://wikitech.wikimedia.org/wiki/Analytics/AQS/Pageviews",
        domain="technology",
        category="internet",
    )

    URL = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/aggregate"
        "/all-projects/all-access/all-agents/daily/{start}/{end}"
    )

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=365)
        url = self.URL.format(
            start=start.strftime("%Y%m%d"),
            end=end.strftime("%Y%m%d"),
        )
        headers = {"User-Agent": "signal-noise/0.1 (research project)"}
        resp = requests.get(url, headers=headers, timeout=self.config.request_timeout)
        resp.raise_for_status()
        items = resp.json().get("items", [])
        if not items:
            raise RuntimeError("No Wikimedia pageview data")
        rows = [
            {
                "date": pd.to_datetime(item["timestamp"], format="%Y%m%d%H", utc=True),
                "value": float(item["views"]),
            }
            for item in items
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
