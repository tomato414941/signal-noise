"""GDELT DOC API collectors for food-related news volume."""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._gdelt_throttle import throttle as _gdelt_throttle

_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

_FOOD_SERIES = [
    ("food crisis", "gdelt_doc_food_crisis", "GDELT News: Food Crisis"),
    ("famine", "gdelt_doc_famine", "GDELT News: Famine"),
]


def _make_gdelt_food_collector(
    query: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/",
            domain="economy",
            category="food_price",
        )

        def fetch(self) -> pd.DataFrame:
            _gdelt_throttle()
            params = {
                "query": query,
                "mode": "timelinevol",
                "format": "json",
                "TIMESPAN": "365d",
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
                raise RuntimeError(f"No GDELT doc data for '{query}'")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"GDELTFood_{name}"
    _Collector.__qualname__ = f"GDELTFood_{name}"
    return _Collector


def get_gdelt_food_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_gdelt_food_collector(query, name, display)
        for query, name, display in _FOOD_SERIES
    }
