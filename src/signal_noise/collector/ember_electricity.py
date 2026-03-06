from __future__ import annotations

import csv
import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_ember_cache = SharedAPICache(ttl=3600)
_EMBER_CSV_URL = "https://files.ember-energy.org/public-downloads/yearly_full_release_long_format.csv"


def _get_ember_data(timeout: int = 30) -> list[dict]:
    """Fetch Ember yearly electricity data for World aggregate."""
    def _fetch() -> list[dict]:
        headers = {"User-Agent": "signal-noise/0.1 (research)"}
        resp = requests.get(_EMBER_CSV_URL, headers=headers, timeout=timeout)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        return [row for row in reader if row.get("Area") == "World"]
    return _ember_cache.get_or_fetch("yearly", _fetch)


# (variable, unit, collector_name, display_name)
EMBER_SIGNALS: list[tuple[str, str, str, str]] = [
    ("Coal", "%", "ember_coal_share", "Ember: Coal Share of Electricity %"),
    ("Gas", "%", "ember_gas_share", "Ember: Gas Share of Electricity %"),
    ("Nuclear", "%", "ember_nuclear_share", "Ember: Nuclear Share of Electricity %"),
    ("Wind", "%", "ember_wind_share", "Ember: Wind Share of Electricity %"),
    ("Solar", "%", "ember_solar_share", "Ember: Solar Share of Electricity %"),
    ("Hydro", "%", "ember_hydro_share", "Ember: Hydro Share of Electricity %"),
]


def _make_ember_collector(
    variable: str, unit: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url="https://ember-climate.org/data/",
            domain="economy",
            category="energy",
        )

        def fetch(self) -> pd.DataFrame:
            data = _get_ember_data(timeout=self.config.request_timeout)
            rows = []
            for row in data:
                if row.get("Variable") != variable or row.get("Unit") != unit:
                    continue
                try:
                    year = int(row["Year"])
                    val = float(row["Value"])
                    date = pd.Timestamp(year=year, month=7, day=1, tz="UTC")
                    rows.append({"date": date, "value": val})
                except (ValueError, TypeError, KeyError):
                    continue
            if not rows:
                raise RuntimeError(f"No Ember data for {variable}")
            df = pd.DataFrame(rows)
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 10)
            df = df[df["date"] >= cutoff]
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Ember_{name}"
    _Collector.__qualname__ = f"Ember_{name}"
    return _Collector


def get_ember_collectors() -> dict[str, type[BaseCollector]]:
    return {t[2]: _make_ember_collector(*t) for t in EMBER_SIGNALS}
