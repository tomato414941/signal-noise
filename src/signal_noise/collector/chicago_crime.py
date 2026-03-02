from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class ChicagoCrimeCollector(BaseCollector):
    """Chicago Data Portal — daily reported crime count."""

    meta = CollectorMeta(
        name="chicago_crime_count",
        display_name="Chicago Daily Crime Count",
        update_frequency="daily",
        api_docs_url="https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2",
        domain="society",
        category="city_stats",
    )

    URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

    def fetch(self) -> pd.DataFrame:
        since = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")
        params = {
            "$where": f"date > '{since}'",
            "$select": "date_trunc_ymd(date) as day, count(*) as cnt",
            "$group": "date_trunc_ymd(date)",
            "$order": "day ASC",
            "$limit": 10000,
        }
        resp = requests.get(self.URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No Chicago crime data")
        rows = []
        for entry in data:
            try:
                rows.append({
                    "date": pd.to_datetime(entry["day"], utc=True),
                    "value": int(entry["cnt"]),
                })
            except (KeyError, ValueError, TypeError):
                continue
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
