from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class MarineTrafficCollector(BaseCollector):
    """UN COMTRADE monthly global trade value (proxy for shipping activity)."""

    meta = CollectorMeta(
        name="global_trade_value",
        display_name="UN COMTRADE Global Monthly Trade Value",
        update_frequency="monthly",
        api_docs_url="https://comtradeapi.un.org/",
        domain="infrastructure",
        category="logistics",
    )

    URL = (
        "https://comtradeapi.un.org/public/v1/preview/C/M/HS"
        "?reporterCode=0&period=2024&partnerCode=0&flowCode=X"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            raise RuntimeError("No COMTRADE data")
        rows = []
        for entry in data:
            try:
                period = str(entry["period"])
                year = int(period[:4])
                month = int(period[4:6]) if len(period) >= 6 else 1
                val = float(entry.get("primaryValue", 0))
                dt = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
                rows.append({"date": dt, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
