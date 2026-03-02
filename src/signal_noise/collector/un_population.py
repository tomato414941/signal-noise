from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class UNPopulationCollector(BaseCollector):
    """World total population from the World Bank API.

    Uses the SP.POP.TOTL indicator for the World aggregate (WLD).
    """

    meta = CollectorMeta(
        name="un_world_population",
        display_name="World Population Estimate",
        update_frequency="yearly",
        api_docs_url="https://api.worldbank.org/v2/country/WLD/indicator/SP.POP.TOTL",
        domain="economy",
        category="economic",
    )

    URL = (
        "https://api.worldbank.org/v2/country/WLD/indicator/SP.POP.TOTL"
        "?format=json&per_page=100&date=1960:2025"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        payload = resp.json()
        # World Bank returns [metadata, data_array]
        if not isinstance(payload, list) or len(payload) < 2:
            raise RuntimeError("Unexpected World Bank response format")
        entries = payload[1]
        if not entries:
            raise RuntimeError("No World Bank population data")
        rows = []
        for entry in entries:
            try:
                year = int(entry["date"])
                val = entry.get("value")
                if val is None:
                    continue
                dt = pd.Timestamp(year=year, month=7, day=1, tz="UTC")
                rows.append({"date": dt, "value": float(val)})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable World Bank population data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
