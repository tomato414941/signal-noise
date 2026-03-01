"""Binance Futures hourly collectors.

Legacy: 5 metrics × 3 symbols = 15 collectors (futures_* naming).
New:    4 collector types = 11 collectors for alpha-os Layer 2 integration.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

log = logging.getLogger(__name__)

_BASE = "https://fapi.binance.com"

# ── Legacy collectors (backward-compatible) ──

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
            category="crypto_derivatives",
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


# ── New Layer 2 collectors (alpha-os integration) ──

# -- Funding Rate --

_FUNDING_SYMBOLS = [
    ("BTCUSDT", "funding_rate_btc", "BTC Funding Rate"),
    ("ETHUSDT", "funding_rate_eth", "ETH Funding Rate"),
    ("SOLUSDT", "funding_rate_sol", "SOL Funding Rate"),
]


def _make_funding_rate_collector(
    symbol: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#get-funding-rate-history",
            domain="financial",
            category="crypto_derivatives",
            signal_type="scalar",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                f"{_BASE}/fapi/v1/fundingRate",
                params={"symbol": symbol, "limit": "1000"},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return pd.DataFrame(columns=["timestamp", "value"])

            rows = []
            for item in data:
                ts = pd.Timestamp(item["fundingTime"], unit="ms", tz="UTC")
                rows.append({"timestamp": ts, "value": float(item["fundingRate"])})
            df = pd.DataFrame(rows)
            return df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    _Collector.__name__ = f"FundingRate_{symbol}"
    _Collector.__qualname__ = f"FundingRate_{symbol}"
    return _Collector


def get_funding_rate_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_funding_rate_collector(*t) for t in _FUNDING_SYMBOLS}


# -- Liquidations --

_LIQ_SYMBOLS = [
    ("BTCUSDT", "liq_ratio_btc_1h", "BTC Liquidation Ratio 1H"),
    ("ETHUSDT", "liq_ratio_eth_1h", "ETH Liquidation Ratio 1H"),
]


def _make_liquidation_collector(
    symbol: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#get-all-liquidation-orders",
            domain="financial",
            category="crypto_derivatives",
            signal_type="scalar",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                f"{_BASE}/fapi/v1/allForceOrders",
                params={"symbol": symbol, "limit": "1000"},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return pd.DataFrame(columns=["timestamp", "value"])

            # Bucket into 1h intervals: liq_long / (liq_long + liq_short)
            buckets: dict[str, dict[str, float]] = {}
            for item in data:
                ts = pd.Timestamp(item["time"], unit="ms", tz="UTC")
                bucket_key = ts.floor("h").isoformat()
                if bucket_key not in buckets:
                    buckets[bucket_key] = {"liq_long": 0.0, "liq_short": 0.0}

                notional = float(item["price"]) * float(item["executedQty"])
                if item["side"] == "SELL":
                    buckets[bucket_key]["liq_long"] += notional
                else:
                    buckets[bucket_key]["liq_short"] += notional

            rows = []
            for ts_str, vals in sorted(buckets.items()):
                total = vals["liq_long"] + vals["liq_short"]
                ratio = vals["liq_long"] / total if total > 0 else 0.5
                rows.append({"timestamp": pd.Timestamp(ts_str), "value": ratio})

            return pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)

    _Collector.__name__ = f"Liquidation_{symbol}"
    _Collector.__qualname__ = f"Liquidation_{symbol}"
    return _Collector


def get_liquidation_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_liquidation_collector(*t) for t in _LIQ_SYMBOLS}


# -- Open Interest --

_OI_SYMBOLS = [
    ("BTCUSDT", "oi_btc_1h", "BTC Open Interest 1H"),
    ("ETHUSDT", "oi_eth_1h", "ETH Open Interest 1H"),
    ("SOLUSDT", "oi_sol_1h", "SOL Open Interest 1H"),
]


def _make_open_interest_collector(
    symbol: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#open-interest",
            domain="financial",
            category="crypto_derivatives",
            signal_type="scalar",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                f"{_BASE}/fapi/v1/openInterest",
                params={"symbol": symbol},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return pd.DataFrame(columns=["timestamp", "value"])

            now = datetime.now(timezone.utc)
            ts = pd.Timestamp(now).floor("h")
            return pd.DataFrame([{"timestamp": ts, "value": float(data["openInterest"])}])

    _Collector.__name__ = f"OpenInterest_{symbol}"
    _Collector.__qualname__ = f"OpenInterest_{symbol}"
    return _Collector


def get_open_interest_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_open_interest_collector(*t) for t in _OI_SYMBOLS}


# -- Long/Short Ratio --

_LS_ENDPOINTS = [
    ("/futures/data/globalLongShortAccountRatio", "ls_ratio_global_btc", "BTC Global L/S Ratio"),
    ("/futures/data/topLongShortAccountRatio", "ls_ratio_top_btc", "BTC Top Trader L/S Account Ratio"),
    ("/futures/data/topLongShortPositionRatio", "ls_position_ratio_btc", "BTC Top Trader L/S Position Ratio"),
]


def _make_long_short_ratio_collector(
    endpoint: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#long-short-ratio",
            domain="financial",
            category="crypto_derivatives",
            signal_type="scalar",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                f"{_BASE}{endpoint}",
                params={"symbol": "BTCUSDT", "period": "1h", "limit": "500"},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return pd.DataFrame(columns=["timestamp", "value"])

            rows = []
            for item in data:
                ts = pd.Timestamp(item["timestamp"], unit="ms", tz="UTC")
                rows.append({"timestamp": ts, "value": float(item["longShortRatio"])})
            df = pd.DataFrame(rows)
            return df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    _Collector.__name__ = f"LongShortRatio_{name}"
    _Collector.__qualname__ = f"LongShortRatio_{name}"
    return _Collector


def get_long_short_ratio_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_long_short_ratio_collector(*t) for t in _LS_ENDPOINTS}
