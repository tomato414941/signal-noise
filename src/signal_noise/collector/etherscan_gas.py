from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class EtherscanGasCollector(BaseCollector):
    """Ethereum average gas price (Gwei) via Etherscan V2 + Blocknative fallback."""

    meta = CollectorMeta(
        name="eth_gas_price",
        display_name="Ethereum Average Gas Price (Gwei)",
        update_frequency="daily",
        api_docs_url="https://docs.etherscan.io/etherscan-v2",
        domain="financial",
        category="crypto",
    )

    ETHERSCAN_V2_URL = (
        "https://api.etherscan.io/v2/api"
        "?chainid=1&module=gastracker&action=gasoracle"
    )
    BLOCKNATIVE_URL = "https://api.blocknative.com/gasprices/blockprices"

    def fetch(self) -> pd.DataFrame:
        avg_gas = self._try_etherscan_v2()
        if avg_gas is None:
            avg_gas = self._try_blocknative()
        if avg_gas is None or avg_gas <= 0:
            raise RuntimeError("No Ethereum gas price data from any source")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": avg_gas}])

    def _try_etherscan_v2(self) -> float | None:
        try:
            resp = requests.get(
                self.ETHERSCAN_V2_URL, timeout=self.config.request_timeout
            )
            resp.raise_for_status()
            body = resp.json()
            result = body.get("result", {})
            # result may be a string (error) or a dict (data)
            if isinstance(result, str):
                return None
            return float(result.get("ProposeGasPrice", 0)) or None
        except Exception:
            return None

    def _try_blocknative(self) -> float | None:
        try:
            resp = requests.get(
                self.BLOCKNATIVE_URL, timeout=self.config.request_timeout
            )
            resp.raise_for_status()
            body = resp.json()
            prices = body.get("blockPrices", [{}])[0].get("estimatedPrices", [])
            if prices:
                # Use the 90% confidence estimate
                for p in prices:
                    if p.get("confidence") == 90:
                        return float(p["price"])
                return float(prices[0]["price"])
            return None
        except Exception:
            return None
