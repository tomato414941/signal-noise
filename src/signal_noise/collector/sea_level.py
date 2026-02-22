from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SeaLevelCollector(BaseCollector):
    """CSIRO/NOAA global mean sea level change (satellite altimetry)."""

    meta = CollectorMeta(
        name="global_sea_level",
        display_name="Global Mean Sea Level Change (mm)",
        update_frequency="monthly",
        api_docs_url="https://sealevel.colorado.edu/",
        domain="earth",
        category="marine",
    )

    URL = "https://sealevel.colorado.edu/files/2024_rel1/gmsl_2024rel1_cycles.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for entry in data:
            try:
                year_frac = float(entry.get("year") or entry.get("decimal_year"))
                gmsl = float(entry.get("gmsl") or entry.get("msl"))
                year = int(year_frac)
                day_of_year = int((year_frac - year) * 365) + 1
                dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC") + pd.Timedelta(days=day_of_year - 1)
                rows.append({"date": dt, "value": gmsl})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No sea level data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
