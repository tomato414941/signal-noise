"""Open Food Facts product database stats.

Tracks total food product entries in the world's largest open
food product database. Growth reflects crowdsourced food
transparency and nutritional awareness efforts.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://world.openfoodfacts.org/api/v2/search"


class OpenFoodFactsProductCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="openfoodfacts_products",
        display_name="Open Food Facts Total Products",
        update_frequency="daily",
        api_docs_url="https://wiki.openfoodfacts.org/API",
        domain="society",
        category="food_security",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _API_URL,
            params={"page_size": "1", "fields": "code"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.json().get("count")
        if count is None:
            raise RuntimeError("No Open Food Facts product count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
