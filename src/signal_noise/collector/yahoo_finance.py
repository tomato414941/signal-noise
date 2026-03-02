from __future__ import annotations

import yfinance as yf
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class _YahooCollector(BaseCollector):
    _ticker: str = ""

    def __init__(self, total: int | None = None, **kwargs):
        super().__init__(**kwargs)
        # Map total rows to yfinance period
        # ~250 trading days/year: >500 → 5y, >1250 → 10y, >2500 → max
        if total and total > 2500:
            self._period = "max"
        elif total and total > 1250:
            self._period = "10y"
        elif total and total > 500:
            self._period = "5y"
        else:
            self._period = "2y"

    def fetch(self) -> pd.DataFrame:
        ticker = yf.Ticker(self._ticker)
        hist = ticker.history(period=self._period, interval="1d")
        if hist.empty:
            raise RuntimeError(f"No data returned for {self._ticker}")
        ts = hist.index.tz_localize("UTC") if hist.index.tz is None else hist.index.tz_convert("UTC")
        df = pd.DataFrame({
            "date": ts,
            "value": hist["Close"].values,
            "open": hist["Open"].values,
            "high": hist["High"].values,
            "low": hist["Low"].values,
            "volume": hist["Volume"].values,
        })
        df = df.sort_values("date").reset_index(drop=True)
        return df


class DXYCollector(_YahooCollector):
    _ticker = "DX-Y.NYB"
    meta = CollectorMeta(
        name="dxy",
        display_name="US Dollar Index (DXY)",
        update_frequency="daily",
        api_docs_url="https://finance.yahoo.com/quote/DX-Y.NYB/",
        domain="markets",
        category="forex",
        signal_type="ohlcv",
    )


class GoldCollector(_YahooCollector):
    _ticker = "GC=F"
    meta = CollectorMeta(
        name="gold",
        display_name="Gold Futures",
        update_frequency="daily",
        api_docs_url="https://finance.yahoo.com/quote/GC=F/",
        domain="markets",
        category="commodity",
        signal_type="ohlcv",
    )


class SP500Collector(_YahooCollector):
    _ticker = "^GSPC"
    meta = CollectorMeta(
        name="sp500",
        display_name="S&P 500",
        update_frequency="daily",
        api_docs_url="https://finance.yahoo.com/quote/%5EGSPC/",
        domain="markets",
        category="equity",
        signal_type="ohlcv",
    )
