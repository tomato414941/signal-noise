from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class EarthquakeCountCollector(BaseCollector):
    """Daily count of M4.5+ earthquakes worldwide (USGS)."""

    meta = SourceMeta(
        name="earthquake_count",
        display_name="Daily M4.5+ Earthquake Count",
        update_frequency="daily",
        data_type="seismic",
        api_docs_url="https://earthquake.usgs.gov/fdsnws/event/1/",
        domain="geophysical",
        category="seismic",
    )

    URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=365 * 2)
        resp = requests.get(
            self.URL,
            params={
                "format": "geojson",
                "starttime": start.strftime("%Y-%m-%d"),
                "endtime": end.strftime("%Y-%m-%d"),
                "minmagnitude": 4.5,
            },
            timeout=60,
        )
        resp.raise_for_status()
        features = resp.json()["features"]

        events = []
        for f in features:
            ts = pd.to_datetime(f["properties"]["time"], unit="ms", utc=True)
            events.append(ts.normalize())

        if not events:
            return pd.DataFrame(columns=["date", "value"])

        sr = pd.Series(events, name="date")
        daily = sr.value_counts().reset_index()
        daily.columns = ["date", "value"]
        daily["value"] = daily["value"].astype(float)
        return daily.sort_values("date").reset_index(drop=True)
