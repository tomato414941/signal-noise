"""CoinGecko trending search collectors.

Tracks the number of trending coins/NFTs/categories as a proxy for
retail crypto interest and FOMO.  No API key required.

Docs: https://docs.coingecko.com/reference/trending-search
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_cache = SharedAPICache(ttl=840)

_TRENDING_URL = "https://api.coingecko.com/api/v3/search/trending"


def _get_trending(timeout: int = 30) -> dict:
    def _fetch() -> dict:
        resp = requests.get(_TRENDING_URL, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    return _cache.get_or_fetch("trending", _fetch)


class CoinGeckoTrendingCoinsCollector(BaseCollector):
    meta = CollectorMeta(
        name="cg_trending_coins",
        display_name="CoinGecko Trending Coins Count",
        update_frequency="hourly",
        api_docs_url="https://docs.coingecko.com/reference/trending-search",
        domain="sentiment",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        data = _get_trending(timeout=self.config.request_timeout)
        count = len(data.get("coins", []))
        now = pd.Timestamp.now(tz="UTC").floor("min")
        return pd.DataFrame([{"date": now, "value": float(count)}])


class CoinGeckoTrendingTopMarketCapRankCollector(BaseCollector):
    meta = CollectorMeta(
        name="cg_trending_top_rank",
        display_name="CoinGecko Trending Top Market Cap Rank",
        update_frequency="hourly",
        api_docs_url="https://docs.coingecko.com/reference/trending-search",
        domain="sentiment",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        data = _get_trending(timeout=self.config.request_timeout)
        coins = data.get("coins", [])
        ranks = [
            c["item"]["market_cap_rank"]
            for c in coins
            if c.get("item", {}).get("market_cap_rank") is not None
        ]
        best_rank = float(min(ranks)) if ranks else 0.0
        now = pd.Timestamp.now(tz="UTC").floor("min")
        return pd.DataFrame([{"date": now, "value": best_rank}])


class CoinGeckoTrendingAvgScoreCollector(BaseCollector):
    meta = CollectorMeta(
        name="cg_trending_avg_score",
        display_name="CoinGecko Trending Avg Score",
        update_frequency="hourly",
        api_docs_url="https://docs.coingecko.com/reference/trending-search",
        domain="sentiment",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        data = _get_trending(timeout=self.config.request_timeout)
        coins = data.get("coins", [])
        scores = [c["item"]["score"] for c in coins if "score" in c.get("item", {})]
        avg = sum(scores) / len(scores) if scores else 0.0
        now = pd.Timestamp.now(tz="UTC").floor("min")
        return pd.DataFrame([{"date": now, "value": float(avg)}])


def get_coingecko_trending_collectors() -> dict[str, type[BaseCollector]]:
    return {
        "cg_trending_coins": CoinGeckoTrendingCoinsCollector,
        "cg_trending_top_rank": CoinGeckoTrendingTopMarketCapRankCollector,
        "cg_trending_avg_score": CoinGeckoTrendingAvgScoreCollector,
    }
