from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class LightningCapacityCollector(BaseCollector):
    """Bitcoin Lightning Network total capacity in BTC.

    Growing capacity = growing L2 adoption.
    mempool.space provides historical stats.
    """

    meta = CollectorMeta(
        name="lightning_capacity",
        display_name="Lightning Network Capacity (BTC)",
        update_frequency="daily",
        api_docs_url="https://mempool.space/docs/api",
        domain="markets",
        category="crypto",
    )

    URL = "https://mempool.space/api/v1/lightning/statistics/2y"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        rows = []
        for entry in data:
            try:
                ts = pd.to_datetime(entry["added"], unit="s", utc=True)
                # capacity in satoshis, convert to BTC
                capacity_btc = entry["total_capacity"] / 1e8
                rows.append({"date": ts.normalize(), "value": capacity_btc})
            except (KeyError, ValueError, TypeError):
                continue

        if not rows:
            raise RuntimeError("No Lightning data parsed")

        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=["date"], keep="last")
        return df.sort_values("date").reset_index(drop=True)
