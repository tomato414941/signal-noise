"""Polymarket prediction market — volume by category."""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_GAMMA_URL = "https://gamma-api.polymarket.com/markets"

_CATEGORIES = [
    ("politics", "polymarket_politics", "Polymarket Politics Volume"),
    ("crypto", "polymarket_crypto_markets", "Polymarket Crypto Volume"),
    ("sports", "polymarket_sports", "Polymarket Sports Volume"),
    ("ai", "polymarket_ai", "Polymarket AI Volume"),
    ("science", "polymarket_science", "Polymarket Science Volume"),
    ("business", "polymarket_business", "Polymarket Business Volume"),
    ("culture", "polymarket_culture", "Polymarket Culture Volume"),
]


def _make_polymarket_volume_collector(
    tag: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://docs.polymarket.com/",
            domain="sentiment",
            category="prediction_market",
        )

        def fetch(self) -> pd.DataFrame:
            params = {
                "closed": "false",
                "limit": 100,
                "order": "volume24hr",
                "ascending": "false",
                "tag": tag,
            }
            resp = requests.get(_GAMMA_URL, params=params, timeout=self.config.request_timeout)
            resp.raise_for_status()
            markets = resp.json()
            total = sum(float(m.get("volume24hr", 0)) for m in markets) if markets else 0
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": total}])

    _Collector.__name__ = f"Polymarket_{name}"
    _Collector.__qualname__ = f"Polymarket_{name}"
    return _Collector


class PolymarketTotalLiquidityCollector(BaseCollector):
    meta = CollectorMeta(
        name="polymarket_total_liquidity",
        display_name="Polymarket Total Liquidity",
        update_frequency="daily",
        api_docs_url="https://docs.polymarket.com/",
        domain="sentiment",
        category="prediction_market",
    )

    def fetch(self) -> pd.DataFrame:
        params = {"closed": "false", "limit": 100, "order": "liquidity", "ascending": "false"}
        resp = requests.get(_GAMMA_URL, params=params, timeout=self.config.request_timeout)
        resp.raise_for_status()
        markets = resp.json()
        total = sum(float(m.get("liquidity", 0)) for m in markets) if markets else 0
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": total}])


class PolymarketActiveMarketsCollector(BaseCollector):
    meta = CollectorMeta(
        name="polymarket_active_markets",
        display_name="Polymarket Active Markets Count",
        update_frequency="daily",
        api_docs_url="https://docs.polymarket.com/",
        domain="sentiment",
        category="prediction_market",
    )

    def fetch(self) -> pd.DataFrame:
        params = {"closed": "false", "limit": 100, "order": "volume24hr", "ascending": "false"}
        resp = requests.get(_GAMMA_URL, params=params, timeout=self.config.request_timeout)
        resp.raise_for_status()
        markets = resp.json()
        active = sum(1 for m in markets if float(m.get("volume24hr", 0)) > 0)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(active)}])


def get_polymarket_categories_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {
        "polymarket_total_liquidity": PolymarketTotalLiquidityCollector,
        "polymarket_active_markets": PolymarketActiveMarketsCollector,
    }
    for tag, name, display in _CATEGORIES:
        collectors[name] = _make_polymarket_volume_collector(tag, name, display)
    return collectors
