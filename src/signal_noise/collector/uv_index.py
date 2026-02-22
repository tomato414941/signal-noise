from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class UVIndexCollector(BaseCollector):
    """EPA UV Index forecast for major US city (New York)."""

    meta = CollectorMeta(
        name="uv_index_nyc",
        display_name="UV Index (New York)",
        update_frequency="daily",
        api_docs_url="https://www.epa.gov/enviro/uv-index-overview",
        domain="earth",
        category="weather",
    )

    URL = "https://data.epa.gov/efservice/getEnvirofactsUVDAILY/CITY/New%20York/STATE/NY/JSON"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No UV index data")
        rows = []
        for entry in data:
            try:
                date_str = entry.get("DATE_TIME") or entry.get("date_time")
                uv = float(entry.get("UV_INDEX") or entry.get("uv_index", 0))
                if date_str:
                    rows.append({"date": pd.Timestamp(date_str, tz="UTC"), "value": uv})
            except (ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable UV data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
