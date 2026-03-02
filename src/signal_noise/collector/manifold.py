from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_manifold_cache = SharedAPICache(ttl=300)


def _get_markets(timeout: int = 30) -> list[dict]:
    def _fetch() -> list[dict]:
        markets = []
        url = "https://api.manifold.markets/v0/search-markets?sort=liquidity&limit=500"
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        markets.extend(resp.json())
        return markets
    return _manifold_cache.get_or_fetch("markets", _fetch)


class ManifoldActiveMarketsCollector(BaseCollector):
    """Number of active (open) markets on Manifold Markets.

    Proxy for prediction market ecosystem activity.
    """

    meta = CollectorMeta(
        name="manifold_active_markets",
        display_name="Manifold Markets Active Markets",
        update_frequency="daily",
        api_docs_url="https://docs.manifold.markets/api",
        domain="prediction",
        category="prediction_market",
    )

    def fetch(self) -> pd.DataFrame:
        markets = _get_markets(timeout=self.config.request_timeout)
        active = sum(1 for m in markets if not m.get("isResolved", True))
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(active)}])


class ManifoldTotalLiquidityCollector(BaseCollector):
    """Total liquidity (mana) across Manifold Markets."""

    meta = CollectorMeta(
        name="manifold_total_liquidity",
        display_name="Manifold Markets Total Liquidity",
        update_frequency="daily",
        api_docs_url="https://docs.manifold.markets/api",
        domain="prediction",
        category="prediction_market",
    )

    def fetch(self) -> pd.DataFrame:
        markets = _get_markets(timeout=self.config.request_timeout)
        total = sum(m.get("totalLiquidity", 0) for m in markets)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])


class ManifoldTotalTradersCollector(BaseCollector):
    """Total unique traders across sampled Manifold Markets."""

    meta = CollectorMeta(
        name="manifold_total_traders",
        display_name="Manifold Markets Total Traders",
        update_frequency="daily",
        api_docs_url="https://docs.manifold.markets/api",
        domain="prediction",
        category="prediction_market",
    )

    def fetch(self) -> pd.DataFrame:
        markets = _get_markets(timeout=self.config.request_timeout)
        total = sum(m.get("uniqueBettorCount", 0) for m in markets)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
