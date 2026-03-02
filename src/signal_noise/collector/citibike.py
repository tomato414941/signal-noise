from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CitibikeStationStatusCollector(BaseCollector):
    """Citi Bike NYC — current total available bikes (snapshot)."""

    meta = CollectorMeta(
        name="citibike_available",
        display_name="Citi Bike NYC Available Bikes",
        update_frequency="daily",
        api_docs_url="https://gbfs.citibikenyc.com/gbfs/gbfs.json",
        domain="technology",
        category="transportation",
    )

    URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("stations", [])
        if not data:
            raise RuntimeError("No Citi Bike station data")
        total_bikes = sum(
            s.get("num_bikes_available", 0) for s in data
        )
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": total_bikes}])
