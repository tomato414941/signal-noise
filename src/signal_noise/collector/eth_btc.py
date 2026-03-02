from __future__ import annotations

import time

import ccxt
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class EthBtcCollector(BaseCollector):
    meta = CollectorMeta(
        name="eth_btc",
        display_name="ETH/BTC Ratio",
        update_frequency="hourly",
        api_docs_url="https://binance-docs.github.io/apidocs/spot/en/",
        domain="markets",
        category="crypto",
    )

    def __init__(self, total: int = 5000, **kwargs):
        super().__init__(**kwargs)
        self.total = total

    def fetch(self) -> pd.DataFrame:
        exchange = ccxt.binance({"enableRateLimit": True})
        since = exchange.milliseconds() - self.total * 3_600_000
        all_data: list[list] = []
        while len(all_data) < self.total:
            batch = exchange.fetch_ohlcv(
                "ETH/BTC", "1h", since=since, limit=1000
            )
            if not batch:
                break
            all_data.extend(batch)
            since = batch[-1][0] + 1
            if len(batch) < 1000:
                break
            time.sleep(0.2)

        df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["value"] = df["close"]
        df = df[["timestamp", "value"]].drop_duplicates(subset=["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df.head(self.total)
