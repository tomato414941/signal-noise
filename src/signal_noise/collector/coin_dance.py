from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CoinDanceCollector(BaseCollector):
    """Coin Dance BTC node count (daily snapshot)."""

    meta = CollectorMeta(
        name="btc_node_count",
        display_name="Bitcoin Node Count",
        update_frequency="daily",
        api_docs_url="https://coin.dance/nodes",
        domain="markets",
        category="crypto",
    )

    URL = "https://bitnodes.io/api/v1/snapshots/?limit=30"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            raise RuntimeError("No bitnodes data")
        rows = [
            {
                "date": pd.Timestamp(r["timestamp"], tz="UTC").normalize(),
                "value": float(r["total_nodes"]),
            }
            for r in results if "total_nodes" in r
        ]
        df = pd.DataFrame(rows)
        daily = df.groupby("date")["value"].last().reset_index()
        return daily.sort_values("date").reset_index(drop=True)
