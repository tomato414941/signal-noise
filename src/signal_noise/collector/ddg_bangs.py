"""DuckDuckGo bang count collector.

Tracks the total number of DuckDuckGo bang shortcuts. Growth
reflects search integration ecosystem expansion.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BANGS_URL = "https://duckduckgo.com/bang.js"


class DDGBangCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="ddg_bang_count",
        display_name="DuckDuckGo Bang Count",
        update_frequency="weekly",
        api_docs_url="https://duckduckgo.com/bangs",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_BANGS_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        bangs = resp.json()
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(bangs))}])
