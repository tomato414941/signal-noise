"""Derived ratio signals computed from existing market data.

These collectors fetch two underlying series and compute their ratio,
providing cross-asset signals that indicate relative strength or
risk appetite shifts.

No API key required (uses yfinance).
"""
from __future__ import annotations

import yfinance as yf
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


def _fetch_close(ticker: str, period: str = "2y") -> pd.Series:
    t = yf.Ticker(ticker)
    hist = t.history(period=period, interval="1d")
    if hist.empty:
        raise RuntimeError(f"No data for {ticker}")
    idx = hist.index.tz_localize("UTC") if hist.index.tz is None else hist.index.tz_convert("UTC")
    idx = idx.normalize()
    return pd.Series(hist["Close"].values, index=idx, name=ticker)


# (numerator_ticker, denominator_ticker, collector_name, display_name, domain, category)
_RATIO_SERIES: list[tuple[str, str, str, str, str, str]] = [
    ("GC=F", "SI=F", "ratio_gold_silver", "Gold/Silver Ratio", "markets", "commodity"),
    ("GC=F", "CL=F", "ratio_gold_oil", "Gold/Oil Ratio", "markets", "commodity"),
    ("BTC-USD", "GC=F", "ratio_btc_gold", "BTC/Gold Ratio", "markets", "crypto"),
    ("^IXIC", "^DJI", "ratio_nasdaq_djia", "NASDAQ/DJIA Ratio", "markets", "equity"),
    ("HG=F", "GC=F", "ratio_copper_gold", "Copper/Gold Ratio", "markets", "commodity"),
]


def _make_ratio_collector(
    num_ticker: str, den_ticker: str,
    name: str, display_name: str, domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://finance.yahoo.com/",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            num = _fetch_close(num_ticker)
            den = _fetch_close(den_ticker)
            merged = pd.DataFrame({"num": num, "den": den}).dropna()
            if merged.empty:
                raise RuntimeError(f"No overlapping data for {num_ticker}/{den_ticker}")
            merged["value"] = merged["num"] / merged["den"]
            rows = [
                {"date": idx, "value": float(row["value"])}
                for idx, row in merged.iterrows()
            ]
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Ratio_{name}"
    _Collector.__qualname__ = f"Ratio_{name}"
    return _Collector


def get_derived_ratios_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_ratio_collector(num, den, name, display, domain, cat)
        for num, den, name, display, domain, cat in _RATIO_SERIES
    }
