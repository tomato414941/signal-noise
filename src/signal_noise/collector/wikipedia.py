from __future__ import annotations

from datetime import datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class WikipediaBtcCollector(BaseCollector):
    meta = SourceMeta(
        name="wikipedia_btc",
        display_name="Wikipedia Bitcoin Pageviews",
        update_frequency="daily",
        data_type="attention",
        api_docs_url="https://wikimedia.org/api/rest_v1/",
    )

    BASE_URL = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        "en.wikipedia/all-access/all-agents/Bitcoin/daily/{start}/{end}"
    )

    def fetch(self) -> pd.DataFrame:
        end = datetime.utcnow()
        start = end - timedelta(days=730)
        url = self.BASE_URL.format(
            start=start.strftime("%Y%m%d00"),
            end=end.strftime("%Y%m%d00"),
        )
        headers = {"User-Agent": "signal-noise/0.1 (https://github.com/signal-noise)"}
        resp = requests.get(url, headers=headers, timeout=self.config.request_timeout)
        resp.raise_for_status()
        items = resp.json()["items"]
        rows = [
            {"date": pd.to_datetime(item["timestamp"], format="%Y%m%d00", utc=True), "value": int(item["views"])}
            for item in items
        ]
        df = pd.DataFrame(rows)
        df = df.sort_values("date").reset_index(drop=True)
        return df
