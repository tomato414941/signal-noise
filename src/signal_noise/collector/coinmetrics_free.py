from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CoinMetricsCollector(BaseCollector):
    """Coin Metrics community BTC transaction count (free tier)."""

    meta = CollectorMeta(
        name="coinmetrics_btc_txcount",
        display_name="BTC Daily Transaction Count (Coin Metrics)",
        update_frequency="daily",
        api_docs_url="https://docs.coinmetrics.io/api/v4/",
        domain="markets",
        category="crypto",
    )

    URL = (
        "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
        "?assets=btc&metrics=TxCnt&frequency=1d&page_size=365"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            raise RuntimeError("No Coin Metrics data")
        rows = [
            {"date": pd.Timestamp(d["time"], tz="UTC"), "value": float(d["TxCnt"])}
            for d in data if "TxCnt" in d
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
