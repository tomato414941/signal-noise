from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GlobalForestWatchCollector(BaseCollector):
    """Global fire activity as a deforestation proxy (FIRMS NOAA-20 VIIRS).

    Uses NASA FIRMS NOAA-20 VIIRS 24-hour global fire CSV to count
    fire detections with high confidence, which correlates with
    deforestation activity in tropical regions.
    """

    meta = CollectorMeta(
        name="glad_deforestation",
        display_name="Global Fire Activity (deforestation proxy)",
        update_frequency="daily",
        api_docs_url="https://firms.modaps.eosdis.nasa.gov/",
        domain="earth",
        category="satellite",
    )

    URL = (
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/"
        "noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=120)
        resp.raise_for_status()
        df_raw = pd.read_csv(
            io.StringIO(resp.text),
            usecols=["acq_date", "confidence"],
        )
        if df_raw.empty:
            raise RuntimeError("No FIRMS fire data")
        # Filter to high-confidence detections as deforestation proxy
        high_conf = df_raw[df_raw["confidence"] == "high"]
        if high_conf.empty:
            # Fall back to all detections if no high-confidence ones
            high_conf = df_raw
        counts = high_conf["acq_date"].value_counts().reset_index()
        counts.columns = ["date", "value"]
        counts["date"] = pd.to_datetime(counts["date"], utc=True)
        counts["value"] = counts["value"].astype(float)
        return counts.sort_values("date").reset_index(drop=True)
