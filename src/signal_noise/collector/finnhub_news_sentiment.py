"""Finnhub company/market news volume and VADER sentiment collectors.

Free-tier endpoints used:
- GET /api/v1/company-news  (per-symbol, date range)
- GET /api/v1/news          (market-level general news)

No premium subscription required.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector._utils import build_timeseries_df
from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector.finnhub_generic import (
    _STOCKS,
    _STOCK_NAMES,
    _get_finnhub_key,
)

_BASE_URL = "https://finnhub.io/api/v1"
_news_cache = SharedAPICache(ttl=3600)
_LOOKBACK_DAYS = 7
_vader = SentimentIntensityAnalyzer()


def _fetch_company_news(symbol: str, timeout: int = 60) -> list[dict]:
    cache_key = f"company_news|{symbol}"

    def _fetch() -> list:
        now = datetime.now(UTC)
        resp = requests.get(
            f"{_BASE_URL}/company-news",
            params={
                "symbol": symbol,
                "from": (now - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d"),
                "to": now.strftime("%Y-%m-%d"),
                "token": _get_finnhub_key(),
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"Finnhub company-news error for {symbol}: {data['error']}")
        return data

    return _news_cache.get_or_fetch(cache_key, _fetch)


def _fetch_market_news(timeout: int = 60) -> list[dict]:
    cache_key = "market_news|general"

    def _fetch() -> list:
        resp = requests.get(
            f"{_BASE_URL}/news",
            params={"category": "general", "token": _get_finnhub_key()},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"Finnhub market-news error: {data['error']}")
        return data

    return _news_cache.get_or_fetch(cache_key, _fetch)


def _compute_news_signals(articles: list[dict]) -> tuple[int, float | None]:
    count = len(articles)
    if count == 0:
        return 0, None
    compounds = []
    for article in articles:
        headline = article.get("headline", "")
        if not headline:
            continue
        score = _vader.polarity_scores(headline)
        compounds.append(score["compound"])
    if not compounds:
        return count, None
    return count, round(sum(compounds) / len(compounds), 4)


# ── Series definitions ──

FINNHUB_NEWS_VOLUME_SERIES: list[tuple[str, str, str, str, str, str]] = [
    (sym, f"finnhub_{sym.lower()}_news_volume",
     f"Finnhub: {_STOCK_NAMES[sym]} News Volume (7d)",
     "daily", "sentiment", "attention")
    for sym in _STOCKS
]

FINNHUB_NEWS_SENTIMENT_SERIES: list[tuple[str, str, str, str, str, str]] = [
    (sym, f"finnhub_{sym.lower()}_news_sentiment",
     f"Finnhub: {_STOCK_NAMES[sym]} News Sentiment (VADER)",
     "daily", "sentiment", "sentiment")
    for sym in _STOCKS
]


# ── Factory: per-symbol news volume ──

def _make_news_volume_collector(
    symbol: str, name: str, display_name: str,
    frequency: str, domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://finnhub.io/docs/api/company-news",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            articles = _fetch_company_news(symbol, timeout=self.config.request_timeout)
            count, _ = _compute_news_signals(articles)
            now = pd.Timestamp.now(tz="UTC").normalize()
            return build_timeseries_df(
                [{"date": now, "value": count}],
                f"Finnhub {symbol} news volume",
            )

    _Collector.__name__ = f"Finnhub_{name}"
    _Collector.__qualname__ = f"Finnhub_{name}"
    return _Collector


# ── Factory: per-symbol news sentiment ──

def _make_news_sentiment_collector(
    symbol: str, name: str, display_name: str,
    frequency: str, domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://finnhub.io/docs/api/company-news",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            articles = _fetch_company_news(symbol, timeout=self.config.request_timeout)
            _, sentiment = _compute_news_signals(articles)
            if sentiment is None:
                raise RuntimeError(f"No headlines with text for {symbol}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return build_timeseries_df(
                [{"date": now, "value": sentiment}],
                f"Finnhub {symbol} news sentiment",
            )

    _Collector.__name__ = f"Finnhub_{name}"
    _Collector.__qualname__ = f"Finnhub_{name}"
    return _Collector


# ── Factory: market-level news ──

def _make_market_news_collector(
    name: str, display_name: str,
    frequency: str, domain: str, category: str,
    *, signal_type: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://finnhub.io/docs/api/market-news",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            articles = _fetch_market_news(timeout=self.config.request_timeout)
            count, sentiment = _compute_news_signals(articles)
            now = pd.Timestamp.now(tz="UTC").normalize()
            if signal_type == "volume":
                return build_timeseries_df(
                    [{"date": now, "value": count}],
                    "Finnhub market news volume",
                )
            if sentiment is None:
                raise RuntimeError("No market headlines with text")
            return build_timeseries_df(
                [{"date": now, "value": sentiment}],
                "Finnhub market news sentiment",
            )

    _Collector.__name__ = f"Finnhub_{name}"
    _Collector.__qualname__ = f"Finnhub_{name}"
    return _Collector


# ── Registry ──

def get_finnhub_news_sentiment_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for t in FINNHUB_NEWS_VOLUME_SERIES:
        collectors[t[1]] = _make_news_volume_collector(*t)
    for t in FINNHUB_NEWS_SENTIMENT_SERIES:
        collectors[t[1]] = _make_news_sentiment_collector(*t)
    collectors["finnhub_market_news_volume"] = _make_market_news_collector(
        "finnhub_market_news_volume", "Finnhub: Market News Volume",
        "daily", "sentiment", "attention", signal_type="volume",
    )
    collectors["finnhub_market_news_sentiment"] = _make_market_news_collector(
        "finnhub_market_news_sentiment", "Finnhub: Market News Sentiment (VADER)",
        "daily", "sentiment", "sentiment", signal_type="sentiment",
    )
    return collectors
