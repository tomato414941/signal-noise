from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class WHODiseaseOutbreakCollector(BaseCollector):
    """WHO global measles reported cases as disease outbreak proxy."""

    meta = CollectorMeta(
        name="who_disease_outbreaks",
        display_name="WHO Global Measles Reported Cases",
        update_frequency="yearly",
        api_docs_url="https://www.who.int/data/gho/info/gho-odata-api",
        domain="health",
        category="epidemiology",
    )

    URL = (
        "https://ghoapi.azureedge.net/api/WHS3_62"
        "?$filter=SpatialDim eq 'GLOBAL'&$orderby=TimeDim desc"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        if not data:
            raise RuntimeError("No WHO disease outbreak data")
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
