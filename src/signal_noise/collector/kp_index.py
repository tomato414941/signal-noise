from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class KpIndexCollector(BaseCollector):
    """GFZ Potsdam Kp geomagnetic activity index."""

    meta = CollectorMeta(
        name="kp_index",
        display_name="Kp Geomagnetic Index",
        update_frequency="daily",
        api_docs_url="https://kp.gfz-potsdam.de/en/",
        domain="geophysical",
        category="space_weather",
    )

    URL = "https://kp.gfz-potsdam.de/app/json/?start={start}&end={end}"

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=365)
        url = self.URL.format(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for entry in data:
            try:
                ts = pd.to_datetime(entry["datetime"], utc=True)
                kp = float(entry["Kp"])
                rows.append({"timestamp": ts, "value": kp})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No Kp index data")
        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)
