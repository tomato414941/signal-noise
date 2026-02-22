from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class KpIndexCollector(BaseCollector):
    """NOAA SWPC Kp geomagnetic activity index.

    Uses the NOAA Space Weather Prediction Center planetary K-index
    endpoint which provides recent 3-hourly Kp observations.
    """

    meta = CollectorMeta(
        name="kp_index",
        display_name="Kp Geomagnetic Index",
        update_frequency="daily",
        api_docs_url="https://www.swpc.noaa.gov/products/planetary-k-index",
        domain="geophysical",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        # First row is the header: ["time_tag", "Kp", "a_running", "station_count"]
        rows = []
        for entry in data[1:]:
            try:
                ts = pd.to_datetime(entry[0], utc=True)
                kp = float(entry[1])
                rows.append({"timestamp": ts, "value": kp})
            except (ValueError, TypeError, IndexError):
                continue
        if not rows:
            raise RuntimeError("No Kp index data")
        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)
