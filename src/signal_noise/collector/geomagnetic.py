from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class GeomagneticCollector(BaseCollector):
    meta = SourceMeta(
        name="geomagnetic",
        display_name="NOAA Planetary K-index",
        update_frequency="hourly",
        api_docs_url="https://www.swpc.noaa.gov/products/planetary-k-index",
        domain="geophysical",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        # First row is header, rest are data: [time_tag, Kp, ...]
        rows = []
        for row in data[1:]:
            try:
                rows.append({
                    "timestamp": pd.to_datetime(row[0], utc=True),
                    "value": float(row[1]),
                })
            except (ValueError, IndexError):
                continue
        df = pd.DataFrame(rows)
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
