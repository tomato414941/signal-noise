from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class HashrateCollector(BaseCollector):
    meta = SourceMeta(
        name="hashrate",
        display_name="BTC Hash Rate",
        update_frequency="daily",
        api_docs_url="https://www.blockchain.com/explorer/charts/hash-rate",
        domain="financial",
        category="crypto",
    )

    URL = "https://api.blockchain.info/charts/hash-rate?timespan=2years&format=json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        values = resp.json()["values"]
        rows = [
            {"date": pd.to_datetime(v["x"], unit="s", utc=True), "value": float(v["y"])}
            for v in values
        ]
        df = pd.DataFrame(rows)
        df = df.sort_values("date").reset_index(drop=True)
        return df
