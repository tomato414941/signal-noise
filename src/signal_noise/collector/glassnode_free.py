from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GlassnodeActiveAddressesCollector(BaseCollector):
    """BTC active (unique) addresses via blockchain.info free API."""

    meta = CollectorMeta(
        name="btc_active_addresses",
        display_name="BTC Active Addresses (blockchain.info)",
        update_frequency="daily",
        api_docs_url="https://www.blockchain.com/explorer/api/charts_api",
        domain="markets",
        category="crypto",
    )

    URL = (
        "https://api.blockchain.info/charts/n-unique-addresses"
        "?timespan=1year&format=json&cors=true"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        values = data.get("values", [])
        if not values:
            raise RuntimeError("No blockchain.info unique-address data")
        rows = [
            {
                "date": pd.Timestamp(v["x"], unit="s", tz="UTC"),
                "value": float(v["y"]),
            }
            for v in values
            if "x" in v and "y" in v
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
