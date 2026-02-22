from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class MarineTrafficCollector(BaseCollector):
    """World exports of goods and services from the World Bank API.

    Uses the NE.EXP.GNFS.CD indicator (current US$) for the
    World aggregate as a proxy for global trade volume.
    Replaces the former UN COMTRADE API which returns empty data.
    """

    meta = CollectorMeta(
        name="global_trade_value",
        display_name="World Bank Global Exports (current US$)",
        update_frequency="yearly",
        api_docs_url="https://api.worldbank.org/v2/country/WLD/indicator/NE.EXP.GNFS.CD",
        domain="infrastructure",
        category="logistics",
    )

    URL = (
        "https://api.worldbank.org/v2/country/WLD/indicator/NE.EXP.GNFS.CD"
        "?format=json&per_page=100&date=2000:2025"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            raise RuntimeError("Unexpected World Bank response format")
        entries = payload[1]
        if not entries:
            raise RuntimeError("No World Bank exports data")
        rows = []
        for entry in entries:
            try:
                year = int(entry["date"])
                val = entry.get("value")
                if val is None:
                    continue
                dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                rows.append({"date": dt, "value": float(val)})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable World Bank exports data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
