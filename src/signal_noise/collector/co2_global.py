from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CO2GlobalCollector(BaseCollector):
    """Global monthly CO2 concentration from NOAA GML (Mauna Loa)."""

    meta = CollectorMeta(
        name="co2_monthly_global",
        display_name="CO2 Monthly Global Average (ppm)",
        update_frequency="monthly",
        api_docs_url="https://gml.noaa.gov/ccgg/trends/",
        domain="environment",
        category="climate",
    )

    URL = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.txt"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            if line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 4:
                try:
                    year, month = int(parts[0]), int(parts[1])
                    co2 = float(parts[3])
                    if co2 > 0:
                        dt = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
                        rows.append({"date": dt, "value": co2})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No CO2 data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
