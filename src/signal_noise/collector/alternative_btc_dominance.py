from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CoinMarketCapDominanceCollector(BaseCollector):
    """CoinGecko BTC dominance historical (30-day chart)."""

    meta = CollectorMeta(
        name="cg_btc_dom_30d",
        display_name="CoinGecko BTC Dominance (30d)",
        update_frequency="daily",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )

    URL = "https://api.coingecko.com/api/v3/global/market_cap_chart?days=30"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("market_cap_chart", {}).get("market_cap_percentage", {})
        btc = data.get("btc", [])
        if not btc:
            raise RuntimeError("No CoinGecko dominance data")
        rows = [
            {"date": pd.Timestamp(ts, unit="ms", tz="UTC"), "value": float(val)}
            for ts, val in btc
        ]
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
