"""Cross-exchange BTC intelligence collectors (Binance / Bybit / OKX).

All use public CCXT endpoints (no authentication required).
Shared data is cached via SharedAPICache to minimise API calls.
"""
from __future__ import annotations

import logging

import ccxt
import numpy as np
import pandas as pd

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta

log = logging.getLogger(__name__)

_xex_cache = SharedAPICache(ttl=120)

_EXCHANGES: dict[str, ccxt.Exchange] = {}


def _get_exchange(name: str) -> ccxt.Exchange:
    if name not in _EXCHANGES:
        cls = getattr(ccxt, name)
        _EXCHANGES[name] = cls({"enableRateLimit": True})
    return _EXCHANGES[name]


def _fetch_ticker(exchange_name: str) -> dict:
    def _fetch() -> dict:
        ex = _get_exchange(exchange_name)
        t = ex.fetch_ticker("BTC/USDT")
        return {"last": float(t["last"]), "quoteVolume": float(t["quoteVolume"] or 0)}

    return _xex_cache.get_or_fetch(f"ticker_{exchange_name}", _fetch)


def _fetch_hourly_returns(exchange_name: str, periods: int = 24) -> np.ndarray:
    def _fetch() -> np.ndarray:
        ex = _get_exchange(exchange_name)
        ohlcv = ex.fetch_ohlcv("BTC/USDT", "1h", limit=periods + 1)
        closes = np.array([c[4] for c in ohlcv], dtype=np.float64)
        if len(closes) < 2:
            return np.array([], dtype=np.float64)
        return np.diff(closes) / closes[:-1]

    return _xex_cache.get_or_fetch(f"returns_{exchange_name}_{periods}", _fetch)


def _compute_lead_lag(returns_a: np.ndarray, returns_b: np.ndarray) -> float:
    n = min(len(returns_a), len(returns_b))
    if n < 3:
        return 0.0
    a = returns_a[:n]
    b = returns_b[:n]
    a_prev = a[:-1]
    b_next = b[1:]
    b_prev = b[:-1]
    a_next = a[1:]

    if (
        np.ptp(a_prev) == 0.0
        or np.ptp(b_next) == 0.0
        or np.ptp(b_prev) == 0.0
        or np.ptp(a_next) == 0.0
    ):
        return 0.0

    # corr(a[t], b[t+1]) - corr(b[t], a[t+1])
    corr_ab = np.corrcoef(a_prev, b_next)[0, 1]
    corr_ba = np.corrcoef(b_prev, a_next)[0, 1]
    if np.isnan(corr_ab) or np.isnan(corr_ba):
        return 0.0
    return float(corr_ab - corr_ba)


# ── Collectors ──


class CrossExchangeSpreadBinanceBybitCollector(BaseCollector):
    meta = CollectorMeta(
        name="spread_binance_bybit_btc",
        display_name="BTC Spread Binance-Bybit (bps)",
        update_frequency="hourly",
        api_docs_url="https://docs.ccxt.com/",
        domain="markets",
        category="crypto",
        signal_type="scalar",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        binance = _fetch_ticker("binance")
        bybit = _fetch_ticker("bybit")
        spread_bps = (bybit["last"] - binance["last"]) / binance["last"] * 10_000
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [spread_bps]})


class CrossExchangeSpreadBinanceOkxCollector(BaseCollector):
    meta = CollectorMeta(
        name="spread_binance_okx_btc",
        display_name="BTC Spread Binance-OKX (bps)",
        update_frequency="hourly",
        api_docs_url="https://docs.ccxt.com/",
        domain="markets",
        category="crypto",
        signal_type="scalar",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        binance = _fetch_ticker("binance")
        okx = _fetch_ticker("okx")
        spread_bps = (okx["last"] - binance["last"]) / binance["last"] * 10_000
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [spread_bps]})


class VolumeDominanceCollector(BaseCollector):
    meta = CollectorMeta(
        name="volume_dominance_btc",
        display_name="BTC Volume Dominance (Binance share)",
        update_frequency="hourly",
        api_docs_url="https://docs.ccxt.com/",
        domain="markets",
        category="crypto",
        signal_type="scalar",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        binance = _fetch_ticker("binance")
        bybit = _fetch_ticker("bybit")
        okx = _fetch_ticker("okx")
        total = binance["quoteVolume"] + bybit["quoteVolume"] + okx["quoteVolume"]
        dominance = binance["quoteVolume"] / total if total > 0 else 0.0
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [dominance]})


class LeadLagCollector(BaseCollector):
    meta = CollectorMeta(
        name="lead_lag_btc",
        display_name="BTC Lead-Lag (Binance vs OKX)",
        update_frequency="hourly",
        api_docs_url="https://docs.ccxt.com/",
        domain="markets",
        category="crypto",
        signal_type="scalar",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        returns_b = _fetch_hourly_returns("binance", periods=24)
        returns_o = _fetch_hourly_returns("okx", periods=24)
        ll = _compute_lead_lag(returns_b, returns_o)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [ll]})
