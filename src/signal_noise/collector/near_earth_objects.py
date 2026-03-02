from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NearEarthObjectCollector(BaseCollector):
    """NASA JPL close approach data for near-Earth objects."""

    meta = CollectorMeta(
        name="neo_close_approach",
        display_name="Near-Earth Object Close Approaches",
        update_frequency="daily",
        api_docs_url="https://ssd-api.jpl.nasa.gov/doc/cad.html",
        domain="environment",
        category="celestial",
    )

    URL = (
        "https://ssd-api.jpl.nasa.gov/cad.api"
        "?date-min={start}&date-max={end}&dist-max=0.05&sort=date"
    )

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
        for entry in data.get("data", []):
            try:
                # entry[3] = close approach date, entry[4] = distance (au)
                date_str = entry[3]
                dist = float(entry[4])
                ts = pd.to_datetime(date_str, utc=True)
                rows.append({"date": ts.normalize(), "value": dist})
            except (ValueError, TypeError, IndexError):
                continue
        if not rows:
            raise RuntimeError("No NEO data from JPL")
        df = pd.DataFrame(rows)
        # Count approaches per day
        daily = df.groupby("date")["value"].count().reset_index()
        daily.columns = ["date", "value"]
        return daily.sort_values("date").reset_index(drop=True)
