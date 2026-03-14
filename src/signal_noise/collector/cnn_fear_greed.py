"""CNN Fear & Greed Index (stock market) collectors.

No API key required.  Requires User-Agent and Referer headers.
Source: https://edition.cnn.com/markets/fear-and-greed
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._utils import build_timeseries_df

_GRAPH_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/2020-07-14"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible)",
    "Referer": "https://edition.cnn.com/markets/fear-and-greed",
}

# (json_key, collector_name, display_name)
_COMPONENT_SERIES: list[tuple[str, str, str]] = [
    ("market_momentum_sp500", "cnn_fg_momentum", "CNN F&G: Market Momentum (S&P 500)"),
    ("stock_price_strength", "cnn_fg_strength", "CNN F&G: Stock Price Strength"),
    ("stock_price_breadth", "cnn_fg_breadth", "CNN F&G: Stock Price Breadth"),
    ("put_call_options", "cnn_fg_put_call", "CNN F&G: Put/Call Options"),
    ("market_volatility_vix", "cnn_fg_vix", "CNN F&G: Market Volatility (VIX)"),
    ("junk_bond_demand", "cnn_fg_junk_bond", "CNN F&G: Junk Bond Demand"),
    ("safe_haven_demand", "cnn_fg_safe_haven", "CNN F&G: Safe Haven Demand"),
]


def _fetch_graphdata(timeout: int = 60) -> dict:
    resp = requests.get(_GRAPH_URL, headers=_HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _parse_historical(data: dict) -> pd.DataFrame:
    """Parse fear_and_greed_historical into a DataFrame."""
    hist = data.get("fear_and_greed_historical", {}).get("data", [])
    if not hist:
        raise RuntimeError("CNN F&G: no historical data")
    rows = []
    for point in hist:
        try:
            dt = pd.to_datetime(point["x"], unit="ms", utc=True)
            rows.append({"date": dt, "value": float(point["y"])})
        except (KeyError, ValueError, TypeError):
            continue
    return build_timeseries_df(rows, "CNN Fear & Greed")


def _parse_component(data: dict, key: str) -> pd.DataFrame:
    """Parse a component's time series into a DataFrame."""
    component = data.get(key, {})
    series = component.get("data", [])
    if not series:
        raise RuntimeError(f"CNN F&G: no data for {key}")
    rows = []
    for point in series:
        try:
            dt = pd.to_datetime(point["x"], unit="ms", utc=True)
            rows.append({"date": dt, "value": float(point["y"])})
        except (KeyError, ValueError, TypeError):
            continue
    return build_timeseries_df(rows, f"CNN F&G {key}")


class CNNFearGreedCollector(BaseCollector):
    meta = CollectorMeta(
        name="cnn_fear_greed",
        display_name="CNN Fear & Greed Index",
        update_frequency="daily",
        api_docs_url="https://edition.cnn.com/markets/fear-and-greed",
        domain="sentiment",
        category="sentiment",
    )

    def fetch(self) -> pd.DataFrame:
        data = _fetch_graphdata(timeout=self.config.request_timeout)
        return _parse_historical(data)


def _make_component_collector(
    key: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://edition.cnn.com/markets/fear-and-greed",
            domain="sentiment",
            category="sentiment",
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_graphdata(timeout=self.config.request_timeout)
            return _parse_component(data, key)

    _Collector.__name__ = f"CNNFG_{name}"
    _Collector.__qualname__ = f"CNNFG_{name}"
    return _Collector


def get_cnn_fear_greed_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for key, name, display_name in _COMPONENT_SERIES:
        collectors[name] = _make_component_collector(key, name, display_name)
    return collectors
