from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NumbeoCollector(BaseCollector):
    """Big Mac Index as a purchasing-power proxy.

    Uses The Economist's Big Mac Index dataset (public CSV on GitHub)
    filtered to the United States as a cost-of-living / purchasing
    power indicator. Replaces the former Numbeo API which requires
    a paid key.
    """

    meta = CollectorMeta(
        name="numbeo_cost_of_living",
        display_name="Big Mac Index (USD, USA)",
        update_frequency="monthly",
        api_docs_url="https://github.com/TheEconomist/big-mac-data",
        domain="economy",
        category="economic",
    )

    URL = "https://raw.githubusercontent.com/TheEconomist/big-mac-data/master/output-data/big-mac-full-index.csv"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        raw = pd.read_csv(io.StringIO(resp.text))
        if raw.empty:
            raise RuntimeError("No Big Mac Index data")
        # Filter for USA
        usa = raw[raw["iso_a3"] == "USA"].copy()
        if usa.empty:
            raise RuntimeError("No USA data in Big Mac Index")
        usa["date"] = pd.to_datetime(usa["date"], utc=True)
        usa["value"] = pd.to_numeric(usa["dollar_price"], errors="coerce")
        result = usa[["date", "value"]].dropna().copy()
        if result.empty:
            raise RuntimeError("No parseable Big Mac data")
        return result.sort_values("date").reset_index(drop=True)
