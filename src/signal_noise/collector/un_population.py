from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class UNPopulationCollector(BaseCollector):
    """UN World Population Prospects — total world population."""

    meta = CollectorMeta(
        name="un_world_population",
        display_name="UN World Population Estimate",
        update_frequency="yearly",
        api_docs_url="https://population.un.org/dataportal/about/dataapi",
        domain="macro",
        category="economic",
    )

    URL = (
        "https://population.un.org/dataportalapi/api/v1/data/indicators/49"
        "/locations/900/start/1950/end/2025?pageSize=200"
    )

    def fetch(self) -> pd.DataFrame:
        headers = {"Accept": "application/json"}
        resp = requests.get(self.URL, headers=headers, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            raise RuntimeError("No UN population data")
        rows = []
        for entry in data:
            try:
                year = int(entry["timeLabel"])
                val = float(entry["value"])
                dt = pd.Timestamp(year=year, month=7, day=1, tz="UTC")
                rows.append({"date": dt, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
