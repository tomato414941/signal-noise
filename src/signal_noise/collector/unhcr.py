from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (field_name, collector_name, display_name, query params)
UNHCR_SERIES: list[tuple[str, str, str, dict[str, str] | None]] = [
    ("forced_total", "unhcr_displaced", "UNHCR Global Forcibly Displaced Population", None),
    ("refugees", "unhcr_refugees", "UNHCR Global Refugees", None),
    ("idps", "unhcr_idps", "UNHCR Global Internally Displaced Persons", None),
    ("asylum_seekers", "unhcr_asylum_seekers", "UNHCR Global Asylum Seekers", None),
    ("stateless", "unhcr_stateless", "UNHCR Global Stateless Population", None),
    ("returned_refugees", "unhcr_returned_refugees", "UNHCR Returned Refugees", None),
    ("returned_idps", "unhcr_returned_idps", "UNHCR Returned IDPs", None),
    (
        "forced_total",
        "unhcr_ukraine_displaced",
        "UNHCR Ukraine Forcibly Displaced Population",
        {"coo": "UKR", "cf_type": "ISO"},
    ),
    (
        "forced_total",
        "unhcr_syria_displaced",
        "UNHCR Syria Forcibly Displaced Population",
        {"coo": "SYR", "cf_type": "ISO"},
    ),
    (
        "forced_total",
        "unhcr_sudan_displaced",
        "UNHCR Sudan Forcibly Displaced Population",
        {"coo": "SDN", "cf_type": "ISO"},
    ),
    (
        "forced_total",
        "unhcr_afghanistan_displaced",
        "UNHCR Afghanistan Forcibly Displaced Population",
        {"coo": "AFG", "cf_type": "ISO"},
    ),
    (
        "forced_total",
        "unhcr_myanmar_displaced",
        "UNHCR Myanmar Forcibly Displaced Population",
        {"coo": "MMR", "cf_type": "ISO"},
    ),
]

_BASE_URL = "https://api.unhcr.org/population/v1/population/"
_DEFAULT_PARAMS = {
    "limit": "1000",
    "yearFrom": "2000",
    "yearTo": "2030",
}


def _to_float(value) -> float:
    if value in (None, "", "-"):
        return 0.0
    return float(value)


def _extract_value(entry: dict, field: str) -> float:
    if field == "forced_total":
        keys = ("refugees", "idps", "asylum_seekers", "ooc", "oip")
        return float(sum(_to_float(entry.get(k)) for k in keys))
    return _to_float(entry.get(field))


def _make_unhcr_collector(
    field: str,
    name: str,
    display_name: str,
    query_params: dict[str, str] | None = None,
) -> type[BaseCollector]:
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

        def fetch(self) -> pd.DataFrame:
            params = dict(_DEFAULT_PARAMS)
            params.update(query_params or {})
            resp = requests.get(_BASE_URL, params=params, timeout=self.config.request_timeout)
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
        name: _make_unhcr_collector(field, name, display, params)
        for field, name, display, params in UNHCR_SERIES
    }
