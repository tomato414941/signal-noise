"""USDA World Agricultural Supply and Demand Estimates (WASDE).

Monthly production estimates for major commodities via USDA FAS API.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_USDA_PSD_URL = "https://apps.fas.usda.gov/OpenData/api/psd/commodity"

_COMMODITIES = [
    ("0440000", "usda_corn_production", "USDA World Corn Production", "Production"),
    ("0410000", "usda_wheat_production", "USDA World Wheat Production", "Production"),
    ("2222000", "usda_soybean_production", "USDA World Soybean Production", "Production"),
]


def _make_usda_collector(
    commodity_code: str, name: str, display_name: str, attribute: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="monthly",
            api_docs_url="https://apps.fas.usda.gov/OpenData/swagger/ui/index",
            domain="food",
            category="agriculture",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"{_USDA_PSD_URL}/{commodity_code}"
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                raise RuntimeError(f"No USDA data for commodity {commodity_code}")
            rows = []
            for entry in data:
                if entry.get("attributeDescription", "") != attribute:
                    continue
                if entry.get("countryCode") != "XX":
                    continue
                try:
                    year = int(entry.get("marketYear", 0))
                    val = float(entry.get("value", 0))
                    dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                    rows.append({"date": dt, "value": val})
                except (ValueError, TypeError):
                    continue
            if not rows:
                raise RuntimeError(f"No {attribute} data for {commodity_code}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"USDA_{name}"
    _Collector.__qualname__ = f"USDA_{name}"
    return _Collector


def get_usda_wasde_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_usda_collector(code, name, display, attr)
        for code, name, display, attr in _COMMODITIES
    }
