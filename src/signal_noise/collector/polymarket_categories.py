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
]


def _make_polymarket_collector(
    tag: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://docs.polymarket.com/",
            domain="prediction",
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


def get_polymarket_categories_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_polymarket_collector(tag, name, display)
        for tag, name, display in _CATEGORIES
    }
