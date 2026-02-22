from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class ProMEDAlertCollector(BaseCollector):
    """WHO DON disease alerts (monthly count) as health event proxy."""

    meta = CollectorMeta(
        name="health_alerts",
        display_name="Global Health Alerts (monthly)",
        update_frequency="monthly",
        api_docs_url="https://www.who.int/emergencies/disease-outbreak-news",
        domain="health",
        category="public_health",
    )

    URL = (
        "https://ghoapi.azureedge.net/api/MDG_0000000020"
        "?$filter=SpatialDim eq 'GLOBAL'"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        if not data:
            raise RuntimeError("No health alert data")
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
