from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class BinanceOpenInterestCollector(BaseCollector):
    """Binance Futures BTC open interest (daily snapshot)."""

    meta = CollectorMeta(
        name="binance_btc_oi",
        display_name="Binance BTC Open Interest",
        update_frequency="daily",
        api_docs_url="https://binance-docs.github.io/apidocs/futures/en/",
        domain="financial",
        category="crypto",
    )

    URL = "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1d&limit=365"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No Binance OI data")
        rows = [
            {
                "date": pd.Timestamp(d["timestamp"], unit="ms", tz="UTC"),
                "value": float(d["sumOpenInterest"]),
            }
            for d in data
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
