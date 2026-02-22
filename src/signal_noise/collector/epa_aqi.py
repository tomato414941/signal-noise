from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class EPAAQICollector(BaseCollector):
    """US PM2.5 concentration (ug/m3) from WHO as AQI proxy."""

    meta = CollectorMeta(
        name="epa_aqi_us",
        display_name="US PM2.5 Average (WHO)",
        update_frequency="yearly",
        api_docs_url="https://www.who.int/data/gho/info/gho-odata-api",
        domain="earth",
        category="air_quality",
    )

    URL = (
        "https://ghoapi.azureedge.net/api/SDGPM25"
        "?$filter=SpatialDim eq 'USA' and Dim1 eq 'RESIDENCEAREATYPE_TOTL'"
        "&$orderby=TimeDim desc"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        if not data:
            raise RuntimeError("No EPA AQI data")
        rows = []
        for entry in data:
            try:
                year = int(entry["TimeDim"])
                val = float(entry["NumericValue"])
                dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                rows.append({"date": dt, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
