from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class DstIndexCollector(BaseCollector):
    """NOAA SWPC Dst geomagnetic storm index (7-day)."""

    meta = CollectorMeta(
        name="dst_index",
        display_name="Dst Geomagnetic Storm Index",
        update_frequency="hourly",
        api_docs_url="https://www.swpc.noaa.gov/products/geospace-geomagnetic-activity-plot",
        domain="environment",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/products/kyoto-dst.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for entry in data[1:]:  # skip header row
            try:
                ts = pd.to_datetime(entry[0], utc=True)
                dst = float(entry[1])
                rows.append({"timestamp": ts, "value": dst})
            except (ValueError, TypeError, IndexError):
                continue
        if not rows:
            return pd.DataFrame(columns=["timestamp", "value"])
        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)
