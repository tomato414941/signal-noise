from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector._auth import load_secret
from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_BASE = "https://api.usa.gov/crime/fbi/sapi"


def _get_key() -> str:
    return load_secret("fbi", "FBI_API_KEY",
                       signup_url="https://api.data.gov/signup/")


# (offense, name, display_name)
FBI_OFFENSES = [
    ("violent-crime", "fbi_violent_crime", "FBI: Violent Crime (US)"),
    ("property-crime", "fbi_property_crime", "FBI: Property Crime (US)"),
    ("homicide", "fbi_homicide", "FBI: Homicide (US)"),
    ("robbery", "fbi_robbery", "FBI: Robbery (US)"),
    ("burglary", "fbi_burglary", "FBI: Burglary (US)"),
    ("arson", "fbi_arson", "FBI: Arson (US)"),
]


def _make_fbi_collector(
    offense: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url="https://cde.ucr.cjis.gov/",
            requires_key=True,
            domain="society",
            category="crime",
        )

        def fetch(self) -> pd.DataFrame:
            api_key = _get_key()
            url = (
                f"{_API_BASE}/api/estimates/national"
                f"?api_key={api_key}"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, list) or not data:
                raise RuntimeError(f"No FBI data for {offense}")

            rows = []
            for entry in data:
                year = entry.get("year")
                val = entry.get(offense.replace("-", "_"))
                if year and val is not None:
                    rows.append({
                        "date": pd.to_datetime(f"{year}-01-01", utc=True),
                        "value": float(val),
                    })

            if not rows:
                raise RuntimeError(f"No FBI data for {offense}")

            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"FBI_{name}"
    _Collector.__qualname__ = f"FBI_{name}"
    return _Collector


def get_fbi_crime_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_fbi_collector(*t) for t in FBI_OFFENSES}
