from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GlassnodeActiveAddressesCollector(BaseCollector):
    """Glassnode BTC active addresses (free tier, limited history)."""

    meta = CollectorMeta(
        name="btc_active_addresses",
        display_name="BTC Active Addresses (Glassnode)",
        update_frequency="daily",
        api_docs_url="https://docs.glassnode.com/",
        domain="financial",
        category="crypto",
    )

    URL = "https://api.glassnode.com/v1/metrics/addresses/active_count?a=BTC&i=24h"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No Glassnode data (API key may be required)")
        rows = [
            {"date": pd.Timestamp(d["t"], unit="s", tz="UTC"), "value": float(d["v"])}
            for d in data if "t" in d and "v" in d
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
