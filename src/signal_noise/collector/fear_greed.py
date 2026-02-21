from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class FearGreedCollector(BaseCollector):
    meta = SourceMeta(
        name="fear_greed",
        display_name="Crypto Fear & Greed Index",
        update_frequency="daily",
        data_type="sentiment",
        api_docs_url="https://alternative.me/crypto/fear-and-greed-index/",
    )

    URL = "https://api.alternative.me/fng/?limit=0&format=json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()["data"]
        rows = [
            {"date": pd.to_datetime(int(d["timestamp"]), unit="s", utc=True), "value": int(d["value"])}
            for d in data
        ]
        df = pd.DataFrame(rows)
        df = df.sort_values("date").reset_index(drop=True)
        return df
