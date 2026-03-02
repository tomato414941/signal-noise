from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class KalshiActiveMarketsCollector(BaseCollector):
    """Kalshi — count of active event contracts."""

    meta = CollectorMeta(
        name="kalshi_active_markets",
        display_name="Kalshi Active Markets Count",
        update_frequency="daily",
        api_docs_url="https://trading-api.readme.io/reference/getevents",
        domain="prediction",
        category="prediction_market",
    )

    URL = "https://api.elections.kalshi.com/trade-api/v2/events?status=open&limit=1"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            self.URL,
            timeout=self.config.request_timeout,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        count = data.get("cursor_info", {}).get("total_count", 0)
        if not count:
            events = data.get("events", [])
            count = len(events)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": count}])
