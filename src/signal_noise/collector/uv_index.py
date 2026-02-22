from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class UVIndexCollector(BaseCollector):
    """Open-Meteo UV Index forecast for New York City.

    Uses the Open-Meteo free API to get daily maximum UV index
    with 30 days of history.
    """

    meta = CollectorMeta(
        name="uv_index_nyc",
        display_name="UV Index (New York)",
        update_frequency="daily",
        api_docs_url="https://open-meteo.com/en/docs",
        domain="earth",
        category="weather",
    )

    URL = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=40.71&longitude=-74.01"
        "&daily=uv_index_max&past_days=30&timezone=UTC"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        daily = data.get("daily", {})
        times = daily.get("time", [])
        values = daily.get("uv_index_max", [])
        if not times or not values:
            raise RuntimeError("No UV index data from Open-Meteo")
        rows = []
        for t, v in zip(times, values):
            if v is not None:
                rows.append({
                    "date": pd.Timestamp(t, tz="UTC"),
                    "value": float(v),
                })
        if not rows:
            raise RuntimeError("No parseable UV data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
