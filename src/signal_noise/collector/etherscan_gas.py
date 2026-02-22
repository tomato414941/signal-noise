from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class EtherscanGasCollector(BaseCollector):
    """Etherscan Ethereum average gas price (Gwei) — free tier."""

    meta = CollectorMeta(
        name="eth_gas_price",
        display_name="Ethereum Average Gas Price (Gwei)",
        update_frequency="daily",
        api_docs_url="https://docs.etherscan.io/api-endpoints/gas-tracker",
        domain="financial",
        category="crypto",
    )

    URL = "https://api.etherscan.io/api?module=gastracker&action=gasoracle"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        result = resp.json().get("result", {})
        avg_gas = float(result.get("ProposeGasPrice", 0))
        if avg_gas <= 0:
            raise RuntimeError("No Etherscan gas data")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": avg_gas}])
