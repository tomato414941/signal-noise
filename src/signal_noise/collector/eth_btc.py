from __future__ import annotations

import ccxt
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class EthBtcCollector(BaseCollector):
    meta = SourceMeta(
        name="eth_btc",
        display_name="ETH/BTC Ratio",
        update_frequency="hourly",
        api_docs_url="https://binance-docs.github.io/apidocs/spot/en/",
        domain="financial",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        exchange = ccxt.binance({"enableRateLimit": True})
        ohlcv = exchange.fetch_ohlcv("ETH/BTC", "1h", limit=1000)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["value"] = df["close"]
        df = df[["timestamp", "value"]].drop_duplicates(subset=["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
