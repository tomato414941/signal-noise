from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SolarFlareCollector(BaseCollector):
    """NASA DONKI solar flare events count per day."""

    meta = CollectorMeta(
        name="solar_flare_count",
        display_name="Solar Flare Events (daily)",
        update_frequency="daily",
        api_docs_url="https://ccmc.gsfc.nasa.gov/tools/DONKI/",
        domain="environment",
        category="space_weather",
    )

    URL = (
        "https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get/FLR"
        "?startDate={start}&endDate={end}"
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
        flares = resp.json()
        if not flares:
            raise RuntimeError("No solar flare data from DONKI")
        rows = []
        for flare in flares:
            try:
                ts = pd.to_datetime(flare["beginTime"], utc=True)
                rows.append({"date": ts.normalize()})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable solar flare data")
        df = pd.DataFrame(rows)
        daily = df.groupby("date").size().reset_index(name="value")
        return daily.sort_values("date").reset_index(drop=True)
