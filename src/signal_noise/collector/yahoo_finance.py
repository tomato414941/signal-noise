from __future__ import annotations

import yfinance as yf
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class _YahooCollector(BaseCollector):
    _ticker: str = ""

    def fetch(self) -> pd.DataFrame:
        ticker = yf.Ticker(self._ticker)
        hist = ticker.history(period="2y", interval="1d")
        if hist.empty:
            raise RuntimeError(f"No data returned for {self._ticker}")
        df = pd.DataFrame({
            "date": hist.index.tz_localize("UTC") if hist.index.tz is None else hist.index.tz_convert("UTC"),
            "value": hist["Close"].values,
        })
        df = df.sort_values("date").reset_index(drop=True)
        return df


class DXYCollector(_YahooCollector):
    _ticker = "DX-Y.NYB"
    meta = SourceMeta(
        name="dxy",
        display_name="US Dollar Index (DXY)",
        update_frequency="daily",
        data_type="macro",
        api_docs_url="https://finance.yahoo.com/quote/DX-Y.NYB/",
    )


class GoldCollector(_YahooCollector):
    _ticker = "GC=F"
    meta = SourceMeta(
        name="gold",
        display_name="Gold Futures",
        update_frequency="daily",
        data_type="macro",
        api_docs_url="https://finance.yahoo.com/quote/GC=F/",
    )


class SP500Collector(_YahooCollector):
    _ticker = "^GSPC"
    meta = SourceMeta(
        name="sp500",
        display_name="S&P 500",
        update_frequency="daily",
        data_type="macro",
        api_docs_url="https://finance.yahoo.com/quote/%5EGSPC/",
    )
