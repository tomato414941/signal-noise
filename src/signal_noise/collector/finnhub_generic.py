from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector._auth import load_secret
from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._utils import build_timeseries_df
from signal_noise.collector._cache import SharedAPICache

_BASE_URL = "https://finnhub.io/api/v1"
_finnhub_cache = SharedAPICache(ttl=3600)

# Stocks to track — Magnificent 7 + liquid cross-sector leaders
_STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META",
    "AMD", "AVGO", "GS", "XOM", "CVX", "CAT", "DE", "UNP",
    "FDX", "DHI", "LEN",
    "NFLX", "ASML", "INTC", "BKNG", "LLY", "UNH", "JPM",
]
_STOCK_NAMES = {
    "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Alphabet",
    "AMZN": "Amazon", "NVDA": "NVIDIA", "TSLA": "Tesla", "META": "Meta",
    "AMD": "AMD", "AVGO": "Broadcom", "GS": "Goldman Sachs",
    "XOM": "Exxon Mobil", "CVX": "Chevron", "CAT": "Caterpillar",
    "DE": "Deere", "UNP": "Union Pacific", "FDX": "FedEx",
    "DHI": "D.R. Horton", "LEN": "Lennar",
    "NFLX": "Netflix", "ASML": "ASML", "INTC": "Intel",
    "BKNG": "Booking Holdings", "LLY": "Eli Lilly", "UNH": "UnitedHealth",
    "JPM": "JPMorgan Chase",
}


def _get_finnhub_key() -> str:
    return load_secret("finnhub", "FINNHUB_API_KEY",
                       signup_url="https://finnhub.io/register")


# ── Fetch helpers (shared cache per symbol) ──


def _fetch_metric(symbol: str, timeout: int = 60) -> dict:
    cache_key = f"metric|{symbol}"

    def _fetch() -> dict:
        resp = requests.get(
            f"{_BASE_URL}/stock/metric",
            params={"symbol": symbol, "metric": "all", "token": _get_finnhub_key()},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"Finnhub metric error for {symbol}: {data['error']}")
        return data

    return _finnhub_cache.get_or_fetch(cache_key, _fetch)


def _fetch_recommendation(symbol: str, timeout: int = 60) -> list[dict]:
    cache_key = f"rec|{symbol}"

    def _fetch() -> list:
        resp = requests.get(
            f"{_BASE_URL}/stock/recommendation",
            params={"symbol": symbol, "token": _get_finnhub_key()},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"Finnhub rec error for {symbol}: {data['error']}")
        return data

    return _finnhub_cache.get_or_fetch(cache_key, _fetch)


def _fetch_insider_sentiment(
    symbol: str, timeout: int = 60,
) -> list[dict]:
    cache_key = f"insider|{symbol}"

    def _fetch() -> list:
        resp = requests.get(
            f"{_BASE_URL}/stock/insider-sentiment",
            params={
                "symbol": symbol,
                "from": "2020-01-01",
                "to": "2099-12-31",
                "token": _get_finnhub_key(),
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"Finnhub insider error for {symbol}: {data['error']}")
        return data.get("data", []) if isinstance(data, dict) else []

    return _finnhub_cache.get_or_fetch(cache_key, _fetch)


def _fetch_earnings(symbol: str, timeout: int = 60) -> list[dict]:
    cache_key = f"earn|{symbol}"

    def _fetch() -> list:
        resp = requests.get(
            f"{_BASE_URL}/stock/earnings",
            params={"symbol": symbol, "token": _get_finnhub_key()},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"Finnhub earnings error for {symbol}: {data['error']}")
        return data

    return _finnhub_cache.get_or_fetch(cache_key, _fetch)


# ── Series definitions ──

# (symbol, metric_key, collector_name, display_name, frequency, domain, category)
FINNHUB_METRIC_SERIES: list[tuple[str, str, str, str, str, str, str]] = []

_METRICS = [
    ("eps", "EPS", "quarterly", "markets", "equity"),
    ("peTTM", "P/E Ratio (TTM)", "quarterly", "markets", "equity"),
    ("roeTTM", "ROE (TTM)", "quarterly", "markets", "equity"),
]

for _sym in _STOCKS:
    _sname = _STOCK_NAMES[_sym]
    _slow = _sym.lower()
    for _mkey, _mlabel, _freq, _dom, _cat in _METRICS:
        FINNHUB_METRIC_SERIES.append((
            _sym, _mkey,
            f"finnhub_{_slow}_{_mkey.lower().replace('ttm', '').rstrip('_') or _mkey.lower()}",
            f"Finnhub: {_sname} {_mlabel}",
            _freq, _dom, _cat,
        ))

# (symbol, collector_name, display_name, frequency, domain, category)
FINNHUB_REC_SERIES: list[tuple[str, str, str, str, str, str]] = [
    (sym, f"finnhub_{sym.lower()}_rec",
     f"Finnhub: {_STOCK_NAMES[sym]} Analyst Consensus",
     "monthly", "sentiment", "equity")
    for sym in _STOCKS
]

FINNHUB_INSIDER_SERIES: list[tuple[str, str, str, str, str, str]] = [
    (sym, f"finnhub_{sym.lower()}_insider",
     f"Finnhub: {_STOCK_NAMES[sym]} Insider Sentiment",
     "monthly", "sentiment", "sentiment")
    for sym in _STOCKS
]

FINNHUB_EARNINGS_SERIES: list[tuple[str, str, str, str, str, str]] = [
    (sym, f"finnhub_{sym.lower()}_earnings",
     f"Finnhub: {_STOCK_NAMES[sym]} Earnings Surprise",
     "quarterly", "markets", "equity")
    for sym in _STOCKS
]


# ── Factory: Metric series collectors ──


def _make_metric_collector(
    symbol: str,
    metric_key: str,
    name: str,
    display_name: str,
    frequency: str,
    domain: str,
    category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://finnhub.io/docs/api/company-basic-financials",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_metric(symbol, timeout=self.config.request_timeout)
            series = data.get("series", {}).get("quarterly", {}).get(metric_key, [])
            if not series:
                raise RuntimeError(
                    f"Finnhub: no quarterly {metric_key} data for {symbol}"
                )
            rows: list[dict] = []
            for point in series:
                try:
                    dt = pd.to_datetime(point["period"], utc=True)
                    val = float(point["v"])
                    rows.append({"date": dt, "value": val})
                except (KeyError, ValueError, TypeError):
                    continue
            return build_timeseries_df(rows, f"Finnhub {symbol} {metric_key}")

    _Collector.__name__ = f"Finnhub_{name}"
    _Collector.__qualname__ = f"Finnhub_{name}"
    return _Collector


# ── Factory: Recommendation collectors ──


def _make_rec_collector(
    symbol: str,
    name: str,
    display_name: str,
    frequency: str,
    domain: str,
    category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://finnhub.io/docs/api/recommendation-trends",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_recommendation(
                symbol, timeout=self.config.request_timeout,
            )
            if not data:
                raise RuntimeError(f"Finnhub: no recommendations for {symbol}")
            rows: list[dict] = []
            for rec in data:
                try:
                    dt = pd.to_datetime(rec["period"], utc=True)
                    total = (
                        rec.get("strongBuy", 0) + rec.get("buy", 0)
                        + rec.get("hold", 0)
                        + rec.get("sell", 0) + rec.get("strongSell", 0)
                    )
                    if total == 0:
                        continue
                    # Score: 1.0 (all strongSell) to 5.0 (all strongBuy)
                    score = (
                        rec.get("strongBuy", 0) * 5
                        + rec.get("buy", 0) * 4
                        + rec.get("hold", 0) * 3
                        + rec.get("sell", 0) * 2
                        + rec.get("strongSell", 0) * 1
                    ) / total
                    rows.append({"date": dt, "value": round(score, 4)})
                except (KeyError, ValueError, TypeError):
                    continue
            return build_timeseries_df(rows, f"Finnhub {symbol} recommendation")

    _Collector.__name__ = f"Finnhub_{name}"
    _Collector.__qualname__ = f"Finnhub_{name}"
    return _Collector


# ── Factory: Insider sentiment collectors ──


def _make_insider_collector(
    symbol: str,
    name: str,
    display_name: str,
    frequency: str,
    domain: str,
    category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://finnhub.io/docs/api/insider-sentiment",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_insider_sentiment(
                symbol, timeout=self.config.request_timeout,
            )
            if not data:
                raise RuntimeError(f"Finnhub: no insider sentiment for {symbol}")
            rows: list[dict] = []
            for entry in data:
                year = entry.get("year")
                month = entry.get("month")
                mspr = entry.get("mspr")
                if year is None or month is None or mspr is None:
                    continue
                try:
                    dt = pd.Timestamp(f"{year}-{month:02d}-01", tz="UTC")
                    rows.append({"date": dt, "value": float(mspr)})
                except (ValueError, TypeError):
                    continue
            return build_timeseries_df(rows, f"Finnhub {symbol} insider")

    _Collector.__name__ = f"Finnhub_{name}"
    _Collector.__qualname__ = f"Finnhub_{name}"
    return _Collector


# ── Factory: Earnings surprise collectors ──


def _make_earnings_collector(
    symbol: str,
    name: str,
    display_name: str,
    frequency: str,
    domain: str,
    category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://finnhub.io/docs/api/company-earnings",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_earnings(symbol, timeout=self.config.request_timeout)
            if not data:
                raise RuntimeError(f"Finnhub: no earnings for {symbol}")
            rows: list[dict] = []
            for earn in data:
                try:
                    dt = pd.to_datetime(earn["period"], utc=True)
                    val = float(earn["surprisePercent"])
                    rows.append({"date": dt, "value": val})
                except (KeyError, ValueError, TypeError):
                    continue
            return build_timeseries_df(rows, f"Finnhub {symbol} earnings")

    _Collector.__name__ = f"Finnhub_{name}"
    _Collector.__qualname__ = f"Finnhub_{name}"
    return _Collector


# ── Registry ──


def get_finnhub_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for t in FINNHUB_METRIC_SERIES:
        collectors[t[2]] = _make_metric_collector(*t)
    for t in FINNHUB_REC_SERIES:
        collectors[t[1]] = _make_rec_collector(*t)
    for t in FINNHUB_INSIDER_SERIES:
        collectors[t[1]] = _make_insider_collector(*t)
    for t in FINNHUB_EARNINGS_SERIES:
        collectors[t[1]] = _make_earnings_collector(*t)
    return collectors
