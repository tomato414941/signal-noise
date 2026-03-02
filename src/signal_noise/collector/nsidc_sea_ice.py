from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NSIDCSeaIceCollector(BaseCollector):
    """NSIDC Arctic sea ice extent (daily)."""

    meta = CollectorMeta(
        name="arctic_sea_ice_extent",
        display_name="Arctic Sea Ice Extent (million km²)",
        update_frequency="daily",
        api_docs_url="https://nsidc.org/data/seaice_index",
        domain="environment",
        category="climate",
    )

    URL = "https://noaadata.apps.nsidc.org/NOAA/G02135/north/daily/data/N_seaice_extent_daily_v4.0.csv"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("Year") or line.startswith(","):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 4:
                continue
            try:
                year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                extent = float(parts[3])
                if extent > 0:
                    dt = pd.Timestamp(year=year, month=month, day=day, tz="UTC")
                    rows.append({"date": dt, "value": extent})
            except (ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No NSIDC sea ice data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
