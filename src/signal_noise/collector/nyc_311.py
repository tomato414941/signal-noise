from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NYC311ComplaintsCollector(BaseCollector):
    """NYC Open Data — daily 311 service request count."""

    meta = CollectorMeta(
        name="nyc_311_complaints",
        display_name="NYC 311 Daily Complaints",
        update_frequency="daily",
        api_docs_url="https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9",
        domain="urban",
        category="city_stats",
    )

    URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

    def fetch(self) -> pd.DataFrame:
        since = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")
        params = {
            "$where": f"created_date > '{since}'",
            "$select": "date_trunc_ymd(created_date) as day, count(*) as cnt",
            "$group": "date_trunc_ymd(created_date)",
            "$order": "day ASC",
            "$limit": 10000,
        }
        resp = requests.get(self.URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No NYC 311 data")
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
