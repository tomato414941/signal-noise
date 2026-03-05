from __future__ import annotations

from io import StringIO

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (location, collector_name, display_name)
OWID_EXCESS_SERIES: list[tuple[str, str, str]] = [
    ("World", "owid_excess_mortality", "OWID Excess Mortality (World, P-score)"),
    ("United States", "owid_excess_mortality_us", "OWID Excess Mortality (US, P-score)"),
    ("United Kingdom", "owid_excess_mortality_uk", "OWID Excess Mortality (UK, P-score)"),
    ("Japan", "owid_excess_mortality_jp", "OWID Excess Mortality (Japan, P-score)"),
    ("Germany", "owid_excess_mortality_de", "OWID Excess Mortality (Germany, P-score)"),
]

_URL = (
    "https://raw.githubusercontent.com/owid/covid-19-data"
    "/master/public/data/excess_mortality/excess_mortality.csv"
)


def _make_owid_excess_collector(
    location: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        """Our World in Data excess mortality p-score (weekly)."""

        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="weekly",
            api_docs_url="https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality",
            domain="society",
            category="excess_deaths",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(_URL, timeout=60)
            resp.raise_for_status()
            raw = pd.read_csv(StringIO(resp.text))
            subset = raw[raw["location"] == location].copy()
            if subset.empty:
                raise RuntimeError(f"No OWID excess mortality data for location={location}")
            df = subset[["date", "p_scores_all_ages"]].dropna().copy()
            df.columns = ["date", "value"]
            df["date"] = pd.to_datetime(df["date"], utc=True)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"OWIDExcess_{name}"
    _Collector.__qualname__ = f"OWIDExcess_{name}"
    return _Collector


OWIDExcessMortalityCollector = _make_owid_excess_collector(
    "World", "owid_excess_mortality", "OWID Excess Mortality (World, P-score)",
)


def get_owid_excess_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_owid_excess_collector(location, name, display)
        for location, name, display in OWID_EXCESS_SERIES
    }

