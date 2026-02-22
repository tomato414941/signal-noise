from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class PolymarketVolumeCollector(BaseCollector):
    """Polymarket prediction market daily trading volume."""

    meta = CollectorMeta(
        name="polymarket_volume",
        display_name="Polymarket Daily Trading Volume",
        update_frequency="daily",
        api_docs_url="https://docs.polymarket.com/",
        domain="sentiment",
        category="sentiment",
    )

    URL = "https://gamma-api.polymarket.com/markets?closed=false&limit=100&order=volume24hr&ascending=false"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        markets = resp.json()
        if not markets:
            raise RuntimeError("No Polymarket data")
        total_volume = sum(float(m.get("volume24hr", 0)) for m in markets)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": total_volume}])
