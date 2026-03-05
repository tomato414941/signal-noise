from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (field_name, collector_name, display_name)
UNHCR_SERIES: list[tuple[str, str, str]] = [
    ("forced_total", "unhcr_displaced", "UNHCR Global Forcibly Displaced Population"),
    ("refugees", "unhcr_refugees", "UNHCR Global Refugees"),
    ("idps", "unhcr_idps", "UNHCR Global Internally Displaced Persons"),
    ("asylum_seekers", "unhcr_asylum_seekers", "UNHCR Global Asylum Seekers"),
    ("stateless", "unhcr_stateless", "UNHCR Global Stateless Population"),
]

_BASE_URL = "https://api.unhcr.org/population/v1/population/"
_QUERY = "?limit=1000&yearFrom=2000&yearTo=2030"


def _extract_value(entry: dict, field: str) -> float:
    if field == "forced_total":
        keys = ("refugees", "idps", "asylum_seekers", "ooc", "oip")
        return float(sum(float(entry.get(k) or 0.0) for k in keys))
    return float(entry.get(field) or 0.0)


def _make_unhcr_collector(field: str, name: str, display_name: str) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        """UNHCR displacement metrics (annual)."""

        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url="https://api.unhcr.org/docs/",
            domain="society",
            category="displacement",
        )

        URL = f"{_BASE_URL}{_QUERY}"

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(self.URL, timeout=self.config.request_timeout)
            resp.raise_for_status()
            result = resp.json()
            items = result.get("items", [])
            if not items:
                raise RuntimeError("No UNHCR displacement data")
            yearly: dict[int, float] = {}
            for entry in items:
                try:
                    year = int(entry["year"])
                    value = _extract_value(entry, field)
                    yearly[year] = yearly.get(year, 0.0) + value
                except (KeyError, ValueError, TypeError):
                    continue
            rows = [
                {"date": pd.Timestamp(year=y, month=1, day=1, tz="UTC"), "value": v}
                for y, v in sorted(yearly.items())
                if v > 0
            ]
            if not rows:
                raise RuntimeError(f"No parseable UNHCR data for {field}")
            return pd.DataFrame(rows)

    _Collector.__name__ = f"UNHCR_{name}"
    _Collector.__qualname__ = f"UNHCR_{name}"
    return _Collector


UNHCRDisplacedCollector = _make_unhcr_collector(
    "forced_total",
    "unhcr_displaced",
    "UNHCR Global Forcibly Displaced Population",
)


def get_unhcr_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_unhcr_collector(field, name, display)
        for field, name, display in UNHCR_SERIES
    }

