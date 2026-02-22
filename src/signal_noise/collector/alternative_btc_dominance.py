from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CoinMarketCapDominanceCollector(BaseCollector):
    """CoinGecko BTC dominance snapshot (free /global endpoint, no key)."""

    meta = CollectorMeta(
        name="cg_btc_dom_30d",
        display_name="CoinGecko BTC Dominance (snapshot)",
        update_frequency="daily",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )

    URL = "https://api.coingecko.com/api/v3/global"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", {})
        pct = data.get("market_cap_percentage", {})
        btc_dom = pct.get("btc")
        if btc_dom is None:
            raise RuntimeError("No CoinGecko BTC dominance data")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(btc_dom)}])
