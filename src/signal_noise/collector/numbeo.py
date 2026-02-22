from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NumbeoCollector(BaseCollector):
    """Numbeo Cost of Living Index (snapshot via web API)."""

    meta = CollectorMeta(
        name="numbeo_cost_of_living",
        display_name="Numbeo Cost of Living Index (NYC)",
        update_frequency="monthly",
        api_docs_url="https://www.numbeo.com/cost-of-living/",
        domain="macro",
        category="economic",
    )

    URL = "https://www.numbeo.com/api/city_prices?api_key=0&query=New+York&currency=USD"

    def fetch(self) -> pd.DataFrame:
        # Numbeo free API is very limited; use a hardcoded known endpoint
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        prices = data.get("prices", [])
        if not prices:
            raise RuntimeError("No Numbeo data")
        # Average all item cost indices
        vals = [float(p["average_price"]) for p in prices if p.get("average_price")]
        avg = sum(vals) / len(vals) if vals else 0
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": avg}])
