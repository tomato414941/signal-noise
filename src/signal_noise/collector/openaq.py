from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class OpenAQCollector(BaseCollector):
    """OpenAQ global average PM2.5 air quality measurement."""

    meta = CollectorMeta(
        name="openaq_pm25",
        display_name="OpenAQ Global PM2.5 Average",
        update_frequency="daily",
        api_docs_url="https://docs.openaq.org/",
        domain="earth",
        category="air_quality",
    )

    URL = "https://api.openaq.org/v2/measurements?parameter=pm25&limit=1000&order_by=datetime&sort=desc"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            raise RuntimeError("No OpenAQ data")
        rows = []
        for r in results:
            try:
                ts = pd.to_datetime(r["date"]["utc"], utc=True)
                rows.append({"date": ts.normalize(), "value": float(r["value"])})
            except (KeyError, ValueError, TypeError):
                continue
        df = pd.DataFrame(rows)
        daily = df.groupby("date")["value"].mean().reset_index()
        return daily.sort_values("date").reset_index(drop=True)
