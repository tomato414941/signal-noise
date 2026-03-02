"""IMF PortWatch chokepoint transit collectors.

No API key required. Uses ArcGIS REST API (public).
Docs: https://portwatch.imf.org/
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_QUERY_URL = (
    "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
    "Daily_Chokepoints_Data/FeatureServer/0/query"
)

# (chokepoint_name, collector_name, display_name)
PORTWATCH_CHOKEPOINTS: list[tuple[str, str, str]] = [
    ("Suez Canal", "portwatch_suez", "PortWatch: Suez Canal"),
    ("Panama Canal", "portwatch_panama", "PortWatch: Panama Canal"),
    ("Strait of Hormuz", "portwatch_hormuz", "PortWatch: Strait of Hormuz"),
    ("Strait of Malacca", "portwatch_malacca", "PortWatch: Strait of Malacca"),
    ("Bab el-Mandeb", "portwatch_mandeb", "PortWatch: Bab el-Mandeb"),
    ("Strait of Gibraltar", "portwatch_gibraltar", "PortWatch: Strait of Gibraltar"),
]


def _make_portwatch_collector(
    chokepoint: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="weekly",
            api_docs_url="https://portwatch.imf.org/",
            domain="infrastructure",
            category="logistics",
        )

        def fetch(self) -> pd.DataFrame:
            params = {
                "where": f"chokepoint='{chokepoint}'",
                "outFields": "date,n_total",
                "orderByFields": "date ASC",
                "resultRecordCount": 2000,
                "f": "json",
            }
            resp = requests.get(
                _QUERY_URL, params=params, timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            features = resp.json().get("features", [])
            if not features:
                raise RuntimeError(
                    f"No PortWatch data for '{chokepoint}'"
                )
            rows = []
            for f in features:
                attrs = f.get("attributes", {})
                ts = attrs.get("date")
                val = attrs.get("n_total")
                if ts is not None and val is not None:
                    rows.append({
                        "date": pd.to_datetime(ts, unit="ms", utc=True),
                        "value": float(val),
                    })
            if not rows:
                raise RuntimeError(
                    f"No valid PortWatch records for '{chokepoint}'"
                )
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"PortWatch_{name}"
    _Collector.__qualname__ = f"PortWatch_{name}"
    return _Collector


def get_portwatch_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for chokepoint, name, display in PORTWATCH_CHOKEPOINTS:
        collectors[name] = _make_portwatch_collector(chokepoint, name, display)
    return collectors
