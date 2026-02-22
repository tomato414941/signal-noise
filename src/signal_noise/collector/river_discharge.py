from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class RiverDischargeCollector(BaseCollector):
    """USGS Mississippi River discharge at Vicksburg (proxy for US hydrology)."""

    meta = CollectorMeta(
        name="mississippi_discharge",
        display_name="Mississippi River Discharge (cfs)",
        update_frequency="daily",
        api_docs_url="https://waterservices.usgs.gov/",
        domain="earth",
        category="hydrology",
    )

    URL = (
        "https://waterservices.usgs.gov/nwis/iv/"
        "?sites=07289000&parameterCd=00060&period=P365D&format=json"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        ts_list = data.get("value", {}).get("timeSeries", [])
        if not ts_list:
            raise RuntimeError("No USGS discharge data")
        values = ts_list[0].get("values", [{}])[0].get("value", [])
        rows = []
        for v in values:
            try:
                ts = pd.to_datetime(v["dateTime"], utc=True)
                val = float(v["value"])
                if val >= 0:
                    rows.append({"timestamp": ts, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable discharge data")
        df = pd.DataFrame(rows)
        daily = df.set_index("timestamp").resample("1D").mean().dropna().reset_index()
        daily.rename(columns={"timestamp": "date"}, inplace=True)
        return daily.sort_values("date").reset_index(drop=True)
