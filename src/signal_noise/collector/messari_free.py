from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class MessariCollector(BaseCollector):
    """BTC 24h trading volume via CoinPaprika free API (no key needed)."""

    meta = CollectorMeta(
        name="messari_btc_volume",
        display_name="BTC Real Volume 24h (CoinPaprika)",
        update_frequency="daily",
        api_docs_url="https://api.coinpaprika.com/",
        domain="financial",
        category="crypto",
    )

    URL = "https://api.coinpaprika.com/v1/tickers/btc-bitcoin"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        quotes = data.get("quotes", {}).get("USD", {})
        vol = float(quotes.get("volume_24h", 0))
        if vol <= 0:
            raise RuntimeError("No CoinPaprika BTC volume data")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": vol}])
