from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NOAAWhaleStrandingCollector(BaseCollector):
    """NOAA Fisheries — marine mammal stranding events (monthly count)."""

    meta = CollectorMeta(
        name="noaa_whale_stranding",
        display_name="NOAA Marine Mammal Strandings",
        update_frequency="monthly",
        api_docs_url="https://www.fisheries.noaa.gov/national/marine-life-distress/national-stranding-database-public-access",
        domain="animal",
        category="wildlife",
    )

    URL = (
        "https://apps-st.fisheries.noaa.gov/ods/foss/stranding"
        "?start=2020-01-01&end=2030-12-31&format=json"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            data = data.get("data", data.get("results", []))
        if not data:
            raise RuntimeError("No NOAA stranding data")
        dates = []
        for entry in data:
            dt_str = entry.get("observation_date") or entry.get("date") or entry.get("Date")
            if dt_str:
                try:
                    dates.append(pd.to_datetime(dt_str, utc=True))
                except (ValueError, TypeError):
                    continue
        if not dates:
            raise RuntimeError("No parseable dates in NOAA stranding data")
        series = pd.Series(dates)
        monthly = series.dt.to_period("M").value_counts().sort_index()
        rows = [
            {"date": pd.Timestamp(p.to_timestamp(), tz="UTC"), "value": v}
            for p, v in monthly.items()
        ]
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
