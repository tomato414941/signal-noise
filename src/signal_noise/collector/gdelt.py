"""GDELT (Global Database of Events, Language, and Tone) collectors.

No API key required.  Uses the GDELT Analysis Service API.
Docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://api.gdeltproject.org/api/v2/tv/tv"

# (query, mode, collector_name, display_name)
GDELT_TV_SERIES: list[tuple[str, str, str, str]] = [
    ("inflation", "timelinevol", "gdelt_tv_inflation", "GDELT TV: Inflation"),
    ("recession", "timelinevol", "gdelt_tv_recession", "GDELT TV: Recession"),
    ("bitcoin", "timelinevol", "gdelt_tv_bitcoin", "GDELT TV: Bitcoin"),
    ("cryptocurrency", "timelinevol", "gdelt_tv_crypto", "GDELT TV: Cryptocurrency"),
    ("stock market", "timelinevol", "gdelt_tv_stocks", "GDELT TV: Stock Market"),
    ("federal reserve", "timelinevol", "gdelt_tv_fed", "GDELT TV: Federal Reserve"),
    ("interest rate", "timelinevol", "gdelt_tv_rates", "GDELT TV: Interest Rates"),
    ("oil price", "timelinevol", "gdelt_tv_oil", "GDELT TV: Oil Prices"),
    ("unemployment", "timelinevol", "gdelt_tv_unemployment", "GDELT TV: Unemployment"),
    ("trade war", "timelinevol", "gdelt_tv_tradewar", "GDELT TV: Trade War"),
    ("tariff", "timelinevol", "gdelt_tv_tariff", "GDELT TV: Tariffs"),
    ("sanctions", "timelinevol", "gdelt_tv_sanctions", "GDELT TV: Sanctions"),
    ("climate change", "timelinevol", "gdelt_tv_climate", "GDELT TV: Climate Change"),
    ("war", "timelinevol", "gdelt_tv_war", "GDELT TV: War"),
    ("cyber attack", "timelinevol", "gdelt_tv_cyber", "GDELT TV: Cyber Attacks"),
    ("artificial intelligence", "timelinevol", "gdelt_tv_ai", "GDELT TV: AI"),
    ("housing market", "timelinevol", "gdelt_tv_housing", "GDELT TV: Housing Market"),
    ("debt ceiling", "timelinevol", "gdelt_tv_debtceiling", "GDELT TV: Debt Ceiling"),
    ("bank crisis", "timelinevol", "gdelt_tv_bankcrisis", "GDELT TV: Bank Crisis"),
    ("gold price", "timelinevol", "gdelt_tv_gold", "GDELT TV: Gold Prices"),
    ("supply chain", "timelinevol", "gdelt_tv_supply", "GDELT TV: Supply Chain"),
    ("energy crisis", "timelinevol", "gdelt_tv_energy", "GDELT TV: Energy Crisis"),
    ("dollar", "timelinevol", "gdelt_tv_dollar", "GDELT TV: US Dollar"),
    ("china economy", "timelinevol", "gdelt_tv_china", "GDELT TV: China Economy"),
    ("europe economy", "timelinevol", "gdelt_tv_europe", "GDELT TV: Europe Economy"),
]

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


def _make_gdelt_tv_collector(
    query: str, mode: str, name: str, display_name: str,
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
                "mode": mode,
                "format": "json",
                "last24": "yes",
                "timezoom": "yes",
                "DATERES": "day",
            }
            resp = requests.get(_BASE_URL, params=params, timeout=self.config.request_timeout)
            resp.raise_for_status()
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
                raise RuntimeError(f"No GDELT TV data for '{query}'")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"GDELT_{name}"
    _Collector.__qualname__ = f"GDELT_{name}"
    return _Collector


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
    for query, mode, name, display in GDELT_TV_SERIES:
        collectors[name] = _make_gdelt_tv_collector(query, mode, name, display)
    for query, name, display in GDELT_DOC_SERIES:
        collectors[name] = _make_gdelt_doc_collector(query, name, display)
    return collectors
