from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NASAFIRMSCollector(BaseCollector):
    """NASA FIRMS active fire detections (NOAA-20 VIIRS global count).

    Downloads the publicly available 24-hour global fire CSV from FIRMS
    (no API key required) and counts fire detections per day.
    """

    meta = CollectorMeta(
        name="nasa_active_fires",
        display_name="NASA FIRMS Active Fire Count",
        update_frequency="daily",
        api_docs_url="https://firms.modaps.eosdis.nasa.gov/",
        domain="environment",
        category="satellite",
    )

    URL = (
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/"
        "noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=120)
        resp.raise_for_status()
        # Read CSV to get the acq_date column
        df_raw = pd.read_csv(io.StringIO(resp.text), usecols=["acq_date"])
        if df_raw.empty:
            raise RuntimeError("No FIRMS data")
        counts = df_raw["acq_date"].value_counts().reset_index()
        counts.columns = ["date", "value"]
        counts["date"] = pd.to_datetime(counts["date"], utc=True)
        counts["value"] = counts["value"].astype(float)
        return counts.sort_values("date").reset_index(drop=True)
