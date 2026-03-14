"""Ethereum gas price collectors.

Etherscan V2 gas oracle: current snapshot (no key needed, 1/5s rate limit).
Owlracle: hourly gas price candles (no key needed).
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._utils import build_timeseries_df


class EthGasPriceCollector(BaseCollector):
    """Current ETH base fee via Etherscan V2 gas oracle (Gwei)."""

    meta = CollectorMeta(
        name="eth_gas_price",
        display_name="ETH Gas Price (Base Fee, Gwei)",
        update_frequency="hourly",
        api_docs_url="https://docs.etherscan.io/",
        domain="technology",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://api.etherscan.io/v2/api",
            params={"chainid": 1, "module": "gastracker", "action": "gasoracle"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "1":
            raise RuntimeError(f"Etherscan gas oracle error: {data.get('message')}")
        result = data["result"]
        base_fee = float(result["suggestBaseFee"])
        now = pd.Timestamp.now(tz="UTC").floor("h")
        return build_timeseries_df(
            [{"date": now, "value": base_fee}], "Etherscan gas oracle",
        )


class EthGasHistoryCollector(BaseCollector):
    """Hourly ETH gas price (close) via Owlracle."""

    meta = CollectorMeta(
        name="eth_gas_history",
        display_name="ETH Gas Price History (Hourly Close, Gwei)",
        update_frequency="hourly",
        api_docs_url="https://owlracle.info/docs",
        domain="technology",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://api.owlracle.info/v4/eth/history",
            params={"candles": 100, "timeframe": 60},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        candles = data.get("candles", [])
        if not candles:
            raise RuntimeError("No Owlracle gas candle data")
        rows = [
            {
                "date": pd.to_datetime(c["timestamp"], utc=True),
                "value": float(c["gasPrice"]["close"]),
            }
            for c in candles
            if c.get("gasPrice", {}).get("close") is not None
        ]
        return build_timeseries_df(rows, "Owlracle gas history")
