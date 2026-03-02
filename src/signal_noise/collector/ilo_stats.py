from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class ILOUnemploymentCollector(BaseCollector):
    """Global unemployment rate from the World Bank API.

    Uses the SL.UEM.TOTL.ZS indicator (modeled ILO estimate)
    for the World aggregate (WLD).
    """

    meta = CollectorMeta(
        name="ilo_unemployment_rate",
        display_name="Global Unemployment Rate (%)",
        update_frequency="yearly",
        api_docs_url="https://api.worldbank.org/v2/country/WLD/indicator/SL.UEM.TOTL.ZS",
        domain="economy",
        category="labor",
    )

    URL = (
        "https://api.worldbank.org/v2/country/WLD/indicator/SL.UEM.TOTL.ZS"
        "?format=json&per_page=100&date=2000:2025"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            raise RuntimeError("Unexpected World Bank response format")
        entries = payload[1]
        if not entries:
            raise RuntimeError("No World Bank unemployment data")
        rows = []
        for entry in entries:
            try:
                year = int(entry["date"])
                val = entry.get("value")
                if val is None:
                    continue
                dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                rows.append({"date": dt, "value": float(val)})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable World Bank unemployment data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
