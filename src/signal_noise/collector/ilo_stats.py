from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class ILOUnemploymentCollector(BaseCollector):
    """ILO global unemployment rate."""

    meta = CollectorMeta(
        name="ilo_unemployment_rate",
        display_name="ILO Global Unemployment Rate (%)",
        update_frequency="yearly",
        api_docs_url="https://ilostat.ilo.org/data/",
        domain="macro",
        category="labor",
    )

    URL = (
        "https://rplumber.ilo.org/data/indicator/"
        "?id=UNE_DEAP_SEX_AGE_RT_A&ref_area=X01&sex=SEX_T&classif1=AGE_YTHADULT_YGE15"
        "&timefrom=2000&format=.json"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            data = data.get("data", data.get("value", []))
        if not data:
            raise RuntimeError("No ILO data")
        rows = []
        for entry in data:
            try:
                year = int(entry.get("time") or entry.get("ref_period", {}).get("period"))
                val = float(entry.get("obs_value") or entry.get("value"))
                dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                rows.append({"date": dt, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
