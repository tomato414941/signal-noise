from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class WorldBankFoodPriceCollector(BaseCollector):
    """World Bank food production index (annual, 2014-2016=100)."""

    meta = CollectorMeta(
        name="wb_food_price_index",
        display_name="World Bank Food Production Index",
        update_frequency="yearly",
        api_docs_url="https://data.worldbank.org/indicator/AG.PRD.FOOD.XD",
        domain="economy",
        category="food_price",
    )

    URL = (
        "https://api.worldbank.org/v2/country/WLD/indicator/AG.PRD.FOOD.XD"
        "?format=json&per_page=100&date=2000:2030"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        payload = resp.json()
        if len(payload) < 2 or not payload[1]:
            raise RuntimeError("No World Bank food production data")
        rows = []
        for entry in payload[1]:
            if entry.get("value") is None:
                continue
            try:
                year = int(entry["date"])
                dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                rows.append({"date": dt, "value": float(entry["value"])})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No valid World Bank food production entries")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
