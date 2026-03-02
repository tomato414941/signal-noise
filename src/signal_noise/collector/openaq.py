from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class OpenAQCollector(BaseCollector):
    """WHO global average PM2.5 concentration (ug/m3)."""

    meta = CollectorMeta(
        name="openaq_pm25",
        display_name="Global PM2.5 Average (WHO)",
        update_frequency="yearly",
        api_docs_url="https://www.who.int/data/gho/info/gho-odata-api",
        domain="environment",
        category="air_quality",
    )

    URL = (
        "https://ghoapi.azureedge.net/api/SDGPM25"
        "?$filter=SpatialDim eq 'GLOBAL' and Dim1 eq 'RESIDENCEAREATYPE_TOTL'"
        "&$orderby=TimeDim desc"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        if not data:
            raise RuntimeError("No PM2.5 data")
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
