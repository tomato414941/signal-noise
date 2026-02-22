from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GlobalPrecipitationCollector(BaseCollector):
    """CPC global daily precipitation index (NOAA)."""

    meta = CollectorMeta(
        name="global_precip_index",
        display_name="CPC Global Precipitation Index",
        update_frequency="daily",
        api_docs_url="https://psl.noaa.gov/data/gridded/data.cpc.globalprecip.html",
        domain="earth",
        category="weather",
    )

    URL = (
        "https://psl.noaa.gov/cgi-bin/data/timeseries/timeseries1.pl"
        "?ntype=1&var=Precipitation&level=Surface"
        "&lat1=-90&lat2=90&lon1=0&lon2=360"
        "&isession=0&iplot=0&icolor=0&istartyear=2024&iendyear=2026"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            parts = line.strip().split()
            if len(parts) == 2:
                try:
                    year_frac = float(parts[0])
                    val = float(parts[1])
                    year = int(year_frac)
                    month = int((year_frac - year) * 12) + 1
                    if 1 <= month <= 12:
                        dt = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
                        rows.append({"date": dt, "value": val})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No precipitation data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
