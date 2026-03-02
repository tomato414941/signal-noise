from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class UNHCRDisplacedCollector(BaseCollector):
    """UNHCR — global forcibly displaced population (annual)."""

    meta = CollectorMeta(
        name="unhcr_displaced",
        display_name="UNHCR Global Forcibly Displaced Population",
        update_frequency="yearly",
        api_docs_url="https://api.unhcr.org/docs/",
        domain="conflict",
        category="displacement",
    )

    URL = "https://api.unhcr.org/population/v1/population/?limit=1000&yearFrom=2000&yearTo=2030"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        result = resp.json()
        items = result.get("items", [])
        if not items:
            raise RuntimeError("No UNHCR displacement data")
        yearly: dict[int, float] = {}
        for entry in items:
            try:
                year = int(entry["year"])
                total = float(entry.get("totalPopulation", 0))
                yearly[year] = yearly.get(year, 0) + total
            except (KeyError, ValueError, TypeError):
                continue
        rows = [
            {"date": pd.Timestamp(year=y, month=1, day=1, tz="UTC"), "value": v}
            for y, v in sorted(yearly.items())
        ]
        return pd.DataFrame(rows)
