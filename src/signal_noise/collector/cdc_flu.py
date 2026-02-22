from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CDCFluCollector(BaseCollector):
    """CDC ILINet influenza-like illness (ILI) national percentage."""

    meta = CollectorMeta(
        name="cdc_ili_rate",
        display_name="CDC ILI National Rate (%)",
        update_frequency="weekly",
        api_docs_url="https://www.cdc.gov/flu/weekly/",
        domain="health",
        category="epidemiology",
    )

    URL = (
        "https://wonder.cdc.gov/nndss/static/2024/52/2024-52-table1aa.html"
    )
    # Alternative: use WHO FluNet data (JSON)
    ALT_URL = "https://ghoapi.azureedge.net/api/INFLUENZA_POSITIVE?$filter=SpatialDim eq 'USA'&$orderby=TimeDim desc&$top=200"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.ALT_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        if not data:
            raise RuntimeError("No CDC flu data")
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
