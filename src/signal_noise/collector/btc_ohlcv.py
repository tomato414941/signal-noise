from __future__ import annotations

import time

import ccxt
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class BtcOhlcvCollector(BaseCollector):
    meta = SourceMeta(
        name="btc_ohlcv",
        display_name="BTC/USDT OHLCV",
        update_frequency="hourly",
        data_type="price",
        api_docs_url="https://binance-docs.github.io/apidocs/spot/en/",
    )

    def __init__(self, symbol: str = "BTC/USDT", timeframe: str = "1h", total: int = 5000, **kwargs):
        super().__init__(**kwargs)
        self.symbol = symbol
        self.timeframe = timeframe
        self.total = total

    def fetch(self) -> pd.DataFrame:
        exchange = ccxt.binance({"enableRateLimit": True})
        all_data: list[list] = []
        since = None
        while len(all_data) < self.total:
            batch = exchange.fetch_ohlcv(
                self.symbol, self.timeframe, since=since, limit=1000
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
        df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        return df.head(self.total)
