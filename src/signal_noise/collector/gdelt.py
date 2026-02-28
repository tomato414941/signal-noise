"""GDELT (Global Database of Events, Language, and Tone) collectors.

No API key required.  Uses the GDELT Analysis Service API.
Docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/

Note: GDELT TV API database stopped updating around Oct 2024 and now
requires station: filters. TV collectors are disabled; DOC collectors
(news articles) remain active.
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

# GDELT DOC API for news article counts
_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

GDELT_DOC_SERIES: list[tuple[str, str, str]] = [
    ("inflation", "gdelt_doc_inflation", "GDELT News: Inflation"),
    ("recession", "gdelt_doc_recession", "GDELT News: Recession"),
    ("bitcoin", "gdelt_doc_bitcoin", "GDELT News: Bitcoin"),
    ("cryptocurrency", "gdelt_doc_crypto", "GDELT News: Cryptocurrency"),
    ("stock market crash", "gdelt_doc_crash", "GDELT News: Market Crash"),
    ("federal reserve", "gdelt_doc_fed", "GDELT News: Federal Reserve"),
    ("oil price", "gdelt_doc_oil", "GDELT News: Oil Prices"),
    ("trade war", "gdelt_doc_tradewar", "GDELT News: Trade War"),
    ("climate change", "gdelt_doc_climate", "GDELT News: Climate Change"),
    ("artificial intelligence", "gdelt_doc_ai", "GDELT News: AI"),
    ("housing crisis", "gdelt_doc_housing", "GDELT News: Housing Crisis"),
    ("bank failure", "gdelt_doc_bankfail", "GDELT News: Bank Failure"),
    ("gold price", "gdelt_doc_gold", "GDELT News: Gold Prices"),
    ("supply chain", "gdelt_doc_supply", "GDELT News: Supply Chain"),
    ("cyber attack", "gdelt_doc_cyber", "GDELT News: Cyber Attacks"),
]


def _make_gdelt_doc_collector(
    query: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/",
            domain="sentiment",
            category="attention",
        )

        def fetch(self) -> pd.DataFrame:
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

    _Collector.__name__ = f"GDELTDoc_{name}"
    _Collector.__qualname__ = f"GDELTDoc_{name}"
    return _Collector


def get_gdelt_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    # TV collectors disabled: database stopped updating Oct 2024
    for query, name, display in GDELT_DOC_SERIES:
        collectors[name] = _make_gdelt_doc_collector(query, name, display)
    return collectors
