"""Binance Futures hourly collectors.

5 metrics × 3 symbols = 15 collectors via factory pattern.
Metrics: funding rate, open interest, global L/S ratio,
top trader L/S ratio, taker buy/sell ratio.
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

FUTURES_SYMBOLS = [
    ("BTCUSDT", "btc"),
    ("ETHUSDT", "eth"),
    ("SOLUSDT", "sol"),
]

_METRICS: list[tuple[str, str, str, str, str, str]] = [
    # (metric_key, api_path, value_field, ts_field, period, display_suffix)
    ("funding", "/fapi/v1/fundingRate", "fundingRate", "fundingTime", "", "Funding Rate"),
    ("oi", "/futures/data/openInterestHist", "sumOpenInterest", "timestamp", "1h", "Open Interest"),
    ("ls_global", "/futures/data/globalLongShortAccountRatio", "longShortRatio", "timestamp", "1h", "Global L/S Ratio"),
    ("ls_top", "/futures/data/topLongShortAccountRatio", "longShortRatio", "timestamp", "1h", "Top Trader L/S Ratio"),
    ("taker_ratio", "/futures/data/takerlongshortRatio", "buySellRatio", "timestamp", "1h", "Taker Buy/Sell Ratio"),
]

_BASE = "https://fapi.binance.com"


def _make_futures_collector(
    api_symbol: str,
    symbol_key: str,
    metric_key: str,
    api_path: str,
    value_field: str,
    ts_field: str,
    period: str,
    display_suffix: str,
) -> type[BaseCollector]:
    name = f"futures_{metric_key}_{symbol_key}"
    display = f"Binance Futures {api_symbol} {display_suffix}"

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display,
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/",
            domain="financial",
            category="crypto",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            params: dict[str, str] = {"symbol": api_symbol, "limit": "500"}
            if period:
                params["period"] = period
            url = f"{_BASE}{api_path}"
            resp = requests.get(url, params=params, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                raise RuntimeError(f"No Binance Futures data for {name}")
            rows = [
                {
                    "date": pd.Timestamp(d[ts_field], unit="ms", tz="UTC"),
                    "value": float(d[value_field]),
                }
                for d in data
            ]
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Futures_{name}"
    _Collector.__qualname__ = f"Futures_{name}"
    return _Collector


def get_binance_futures_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for api_symbol, symbol_key in FUTURES_SYMBOLS:
        for metric_key, api_path, value_field, ts_field, period, display_suffix in _METRICS:
            name = f"futures_{metric_key}_{symbol_key}"
            collectors[name] = _make_futures_collector(
                api_symbol, symbol_key, metric_key,
                api_path, value_field, ts_field, period, display_suffix,
            )
    return collectors
