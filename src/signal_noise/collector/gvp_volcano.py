from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GVPVolcanoCollector(BaseCollector):
    """Smithsonian GVP weekly volcanic activity reports count."""

    meta = CollectorMeta(
        name="gvp_active_volcanoes",
        display_name="GVP Weekly Active Volcanoes",
        update_frequency="weekly",
        api_docs_url="https://volcano.si.edu/reports_weekly.cfm",
        domain="geophysical",
        category="seismic",
    )

    URL = "https://volcano.si.edu/database/webservices.cfm?action=getEruptions&fmt=json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        eruptions = data if isinstance(data, list) else data.get("items", data.get("features", []))
        rows = []
        for item in eruptions:
            props = item.get("properties", item) if isinstance(item, dict) else {}
            start = props.get("StartDate") or props.get("start_date")
            if start:
                try:
                    ts = pd.to_datetime(start, utc=True)
                    rows.append({"date": ts.normalize()})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No GVP volcano data")
        df = pd.DataFrame(rows)
        # Count eruptions starting per month
        df["month"] = df["date"].dt.to_period("M").dt.to_timestamp(tz="UTC")
        monthly = df.groupby("month").size().reset_index(name="value")
        monthly.columns = ["date", "value"]
        return monthly.sort_values("date").reset_index(drop=True)
