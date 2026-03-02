from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SolarXrayCollector(BaseCollector):
    """GOES satellite X-ray flux (1-8 Angstrom), hourly average.

    Higher flux = stronger solar activity.  7-day rolling window from SWPC;
    data accumulates across collection runs via parquet append.
    """

    meta = CollectorMeta(
        name="solar_xray",
        display_name="GOES Solar X-ray Flux (1-8A)",
        update_frequency="hourly",
        api_docs_url="https://www.swpc.noaa.gov/products/goes-x-ray-flux",
        domain="environment",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/json/goes/primary/xrays-7-day.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()

        rows = []
        for entry in data:
            try:
                ts = pd.to_datetime(entry["time_tag"], utc=True)
                flux = float(entry["flux"])
                if flux > 0:
                    rows.append({"timestamp": ts, "value": flux})
            except (KeyError, ValueError, TypeError):
                continue

        if not rows:
            return pd.DataFrame(columns=["timestamp", "value"])

        df = pd.DataFrame(rows)
        # Resample to hourly mean (raw data is per-minute)
        df = df.set_index("timestamp").resample("1h").mean().dropna().reset_index()
        return df
