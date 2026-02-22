from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SolarWindCollector(BaseCollector):
    """NOAA SWPC real-time solar wind speed from DSCOVR/ACE satellite."""

    meta = CollectorMeta(
        name="solar_wind_speed",
        display_name="Solar Wind Speed (km/s)",
        update_frequency="hourly",
        api_docs_url="https://www.swpc.noaa.gov/products/real-time-solar-wind",
        domain="geophysical",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        # First row is header: ["time_tag", "density", "speed", "temperature"]
        rows = []
        for entry in data[1:]:
            try:
                ts = pd.to_datetime(entry[0], utc=True)
                speed = float(entry[2])
                if speed > 0:
                    rows.append({"timestamp": ts, "value": speed})
            except (ValueError, TypeError, IndexError):
                continue
        if not rows:
            return pd.DataFrame(columns=["timestamp", "value"])
        df = pd.DataFrame(rows)
        df = df.set_index("timestamp").resample("1h").mean().dropna().reset_index()
        return df
