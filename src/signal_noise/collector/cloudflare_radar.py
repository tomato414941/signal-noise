from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CloudflareRadarCollector(BaseCollector):
    """Cloudflare Radar internet traffic trends (28-day).

    Tracks the percentage of HTTP traffic classified as human
    vs bot across Cloudflare's global network.
    """

    meta = CollectorMeta(
        name="cloudflare_http_human",
        display_name="Cloudflare HTTP Human Traffic %",
        update_frequency="daily",
        api_docs_url="https://developers.cloudflare.com/radar/",
        domain="infrastructure",
        category="internet",
    )

    URL = (
        "https://api.cloudflare.com/client/v4/radar/http/timeseries/bot_class"
        "?dateRange=28d&format=json"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        series = data.get("result", {}).get("human", {})
        timestamps = series.get("timestamps", [])
        values = series.get("values", [])
        if not timestamps:
            raise RuntimeError("No Cloudflare Radar data")
        rows = [
            {"timestamp": ts, "value": float(v)}
            for ts, v in zip(timestamps, values)
        ]
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.sort_values("timestamp").reset_index(drop=True)
