from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GlobalForestWatchCollector(BaseCollector):
    """Global Forest Watch GLAD deforestation alerts."""

    meta = CollectorMeta(
        name="glad_deforestation",
        display_name="GLAD Deforestation Alerts (weekly)",
        update_frequency="weekly",
        api_docs_url="https://data-api.globalforestwatch.org/",
        domain="earth",
        category="satellite",
    )

    URL = "https://data-api.globalforestwatch.org/dataset/umd_glad_landsat_alerts/latest/query?sql=SELECT alert__date, COUNT(*) as cnt FROM results WHERE alert__date >= '{start}' GROUP BY alert__date ORDER BY alert__date"

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=90)
        url = self.URL.format(start=start.strftime("%Y-%m-%d"))
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            raise RuntimeError("No GLAD deforestation data")
        rows = [
            {"date": pd.Timestamp(d["alert__date"], tz="UTC"), "value": float(d["cnt"])}
            for d in data if "alert__date" in d and "cnt" in d
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
