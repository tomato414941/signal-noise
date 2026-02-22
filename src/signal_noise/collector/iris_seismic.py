from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class IRISSeismicCollector(BaseCollector):
    """IRIS FDSN global earthquake count (M4.0+) per day."""

    meta = CollectorMeta(
        name="iris_seismic_count",
        display_name="Global Seismic Events M4+ (IRIS)",
        update_frequency="daily",
        api_docs_url="https://service.iris.edu/fdsnws/event/1/",
        domain="geophysical",
        category="seismic",
    )

    URL = (
        "https://service.iris.edu/fdsnws/event/1/query"
        "?starttime={start}&endtime={end}&minmagnitude=4.0&format=text"
    )

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=180)
        url = self.URL.format(
            start=start.strftime("%Y-%m-%dT00:00:00"),
            end=end.strftime("%Y-%m-%dT00:00:00"),
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n")[1:]:  # skip header
            parts = line.split("|")
            if len(parts) >= 2:
                try:
                    ts = pd.to_datetime(parts[1].strip(), utc=True)
                    rows.append({"date": ts.normalize()})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No IRIS seismic data")
        df = pd.DataFrame(rows)
        daily = df.groupby("date").size().reset_index(name="value")
        return daily.sort_values("date").reset_index(drop=True)
