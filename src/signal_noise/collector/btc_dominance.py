from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class BtcDominanceCollector(BaseCollector):
    meta = CollectorMeta(
        name="btc_dominance",
        display_name="BTC Market Dominance",
        update_frequency="daily",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="markets",
        category="crypto",
    )

    URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=365"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        market_caps = data["market_caps"]
        rows = [
            {"date": pd.to_datetime(mc[0], unit="ms", utc=True), "value": float(mc[1])}
            for mc in market_caps
            if mc[1] is not None
        ]
        df = pd.DataFrame(rows)
        df = df.sort_values("date").reset_index(drop=True)
        return df
