from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NYCTaxiTripsCollector(BaseCollector):
    """NYC TLC — monthly yellow taxi trip count."""

    meta = CollectorMeta(
        name="nyc_taxi_trips",
        display_name="NYC Yellow Taxi Monthly Trips",
        update_frequency="monthly",
        api_docs_url="https://data.cityofnewyork.us/Transportation/2024-Yellow-Taxi-Trip-Data/wn6i-ogsj",
        domain="urban",
        category="transportation",
    )

    URL = "https://data.cityofnewyork.us/resource/m6nq-qud6.json"

    def fetch(self) -> pd.DataFrame:
        params = {
            "$select": "date_trunc_ym(pickup_datetime) as month, count(*) as cnt",
            "$group": "date_trunc_ym(pickup_datetime)",
            "$order": "month ASC",
            "$limit": 5000,
        }
        resp = requests.get(self.URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No NYC taxi data")
        rows = []
        for entry in data:
            try:
                rows.append({
                    "date": pd.to_datetime(entry["month"], utc=True),
                    "value": int(entry["cnt"]),
                })
            except (KeyError, ValueError, TypeError):
                continue
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
