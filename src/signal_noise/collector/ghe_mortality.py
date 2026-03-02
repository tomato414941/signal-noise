from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GHEMortalityCollector(BaseCollector):
    """WHO Global Health Estimates — all-cause mortality rate."""

    meta = CollectorMeta(
        name="who_mortality_rate",
        display_name="WHO Global All-Cause Mortality Rate",
        update_frequency="yearly",
        api_docs_url="https://www.who.int/data/gho/info/gho-odata-api",
        domain="society",
        category="public_health",
    )

    URL = (
        "https://ghoapi.azureedge.net/api/NCDMORT3070"
        "?$filter=SpatialDim eq 'GLOBAL' and Dim1 eq 'SEX_BTSX'"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        if not data:
            raise RuntimeError("No WHO mortality data")
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
