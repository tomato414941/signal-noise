from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NASAFIRMSCollector(BaseCollector):
    """NASA FIRMS active fire detections (VIIRS global count)."""

    meta = CollectorMeta(
        name="nasa_active_fires",
        display_name="NASA FIRMS Active Fire Count",
        update_frequency="daily",
        api_docs_url="https://firms.modaps.eosdis.nasa.gov/api/",
        domain="earth",
        category="satellite",
    )

    URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/DEMO_KEY/VIIRS_SNPP_NRT/world/1"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")
        if len(lines) < 2:
            raise RuntimeError("No FIRMS data")
        # Count fire detections per day from CSV
        date_counts: dict[str, int] = {}
        header = lines[0].split(",")
        date_idx = header.index("acq_date") if "acq_date" in header else 5
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) > date_idx:
                d = parts[date_idx]
                date_counts[d] = date_counts.get(d, 0) + 1
        rows = [
            {"date": pd.Timestamp(d, tz="UTC"), "value": float(c)}
            for d, c in date_counts.items()
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
