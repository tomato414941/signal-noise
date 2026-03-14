"""GDELT DOC API tone (sentiment) collectors.

Uses the ``timelinetone`` mode to retrieve hourly average tone scores
for news articles matching a given query.  Positive values indicate
positive sentiment; negative values indicate negative sentiment.

No API key required.  Rate limit: 1 req / 5 s.
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

_TONE_SERIES: list[tuple[str, str, str]] = [
    ("bitcoin", "gdelt_tone_bitcoin", "GDELT Tone: Bitcoin"),
    ("cryptocurrency", "gdelt_tone_crypto", "GDELT Tone: Cryptocurrency"),
    ("stock market", "gdelt_tone_stock_market", "GDELT Tone: Stock Market"),
    ("inflation", "gdelt_tone_inflation", "GDELT Tone: Inflation"),
    ("recession", "gdelt_tone_recession", "GDELT Tone: Recession"),
    ("federal reserve", "gdelt_tone_fed", "GDELT Tone: Federal Reserve"),
    ("oil price", "gdelt_tone_oil", "GDELT Tone: Oil Prices"),
    ("gold price", "gdelt_tone_gold", "GDELT Tone: Gold Prices"),
    ("trade war", "gdelt_tone_tradewar", "GDELT Tone: Trade War"),
    ("artificial intelligence", "gdelt_tone_ai", "GDELT Tone: AI"),
    ("climate change", "gdelt_tone_climate", "GDELT Tone: Climate Change"),
    ("war", "gdelt_tone_war", "GDELT Tone: War"),
]


def _make_gdelt_tone_collector(
    query: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/",
            domain="sentiment",
            category="sentiment",
        )

        def fetch(self) -> pd.DataFrame:
            params = {
                "query": query,
                "mode": "timelinetone",
                "format": "json",
                "TIMESPAN": "7d",
            }
            resp = requests.get(_DOC_URL, params=params, timeout=self.config.request_timeout)
            resp.raise_for_status()
            if not resp.text.strip():
                raise RuntimeError(f"GDELT returned empty response for '{query}'")
            data = resp.json()

            timeline = data.get("timeline", [])
            rows = []
            for series in timeline:
                for point in series.get("data", []):
                    dt = point.get("date")
                    val = point.get("value")
                    if dt and val is not None:
                        rows.append({
                            "date": pd.to_datetime(dt, utc=True),
                            "value": float(val),
                        })

            if not rows:
                raise RuntimeError(f"No GDELT tone data for '{query}'")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"GDELTTone_{name}"
    _Collector.__qualname__ = f"GDELTTone_{name}"
    return _Collector


def get_gdelt_tone_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_gdelt_tone_collector(query, name, display)
        for query, name, display in _TONE_SERIES
    }
