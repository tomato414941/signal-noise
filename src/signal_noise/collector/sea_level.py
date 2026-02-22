from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SeaLevelCollector(BaseCollector):
    """NOAA Laboratory for Satellite Altimetry global mean sea level.

    Downloads the multi-mission altimetry sea level anomaly CSV
    and returns global mean sea level change in mm.
    """

    meta = CollectorMeta(
        name="global_sea_level",
        display_name="Global Mean Sea Level Change (mm)",
        update_frequency="monthly",
        api_docs_url="https://www.star.nesdis.noaa.gov/socd/lsa/SeaLevelRise/",
        domain="earth",
        category="marine",
    )

    URL = (
        "https://www.star.nesdis.noaa.gov/socd/lsa/SeaLevelRise/"
        "slr/slr_sla_gbl_keep_all_66.csv"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        # Skip comment lines (start with #)
        lines = [
            line for line in resp.text.strip().split("\n")
            if not line.startswith("#")
        ]
        if not lines:
            raise RuntimeError("No sea level data")
        csv_text = "\n".join(lines)
        df_raw = pd.read_csv(io.StringIO(csv_text))
        rows = []
        for _, row in df_raw.iterrows():
            try:
                year_frac = float(row.iloc[0])  # "year" column
                # Find the first non-NaN satellite value
                gmsl = None
                for col in df_raw.columns[1:]:
                    val = row[col]
                    if pd.notna(val):
                        gmsl = float(val)
                        break
                if gmsl is None:
                    continue
                year = int(year_frac)
                day_of_year = int((year_frac - year) * 365) + 1
                dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC") + pd.Timedelta(
                    days=day_of_year - 1
                )
                rows.append({"date": dt, "value": gmsl})
            except (ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No sea level data after parsing")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
