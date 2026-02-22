from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class MessariCollector(BaseCollector):
    """Messari BTC real volume (24h, daily snapshot)."""

    meta = CollectorMeta(
        name="messari_btc_volume",
        display_name="Messari BTC Real Volume (24h)",
        update_frequency="daily",
        api_docs_url="https://messari.io/api/docs",
        domain="financial",
        category="crypto",
    )

    URL = "https://data.messari.io/api/v1/assets/bitcoin/metrics"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", {})
        market = data.get("market_data", {})
        vol = float(market.get("real_volume_last_24_hours", 0))
        if vol <= 0:
            raise RuntimeError("No Messari volume data")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": vol}])
