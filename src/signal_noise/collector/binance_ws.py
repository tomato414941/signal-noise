"""Binance Futures WebSocket streaming collectors.

Real-time liquidation and funding rate data via WebSocket.
Data is aggregated into 1-minute buckets before yielding.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator

import pandas as pd
import websockets

from signal_noise.collector.base import CollectorMeta
from signal_noise.collector.streaming import StreamingCollector

log = logging.getLogger(__name__)

_WS_BASE = "wss://fstream.binance.com/ws"


class BinanceLiquidationStreamCollector(StreamingCollector):
    """Real-time BTC liquidation events from Binance Futures WebSocket.

    Accumulates liquidation events into 1-minute buckets and yields
    liq_ratio (long_liq / total_liq) per bucket.
    """

    meta = CollectorMeta(
        name="liq_stream_btc",
        display_name="BTC Liquidation Stream",
        update_frequency="hourly",
        api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#liquidation-order-streams",
        domain="financial",
        category="crypto_derivatives",
        signal_type="scalar",
        collect_interval=60,
    )

    async def stream(self) -> AsyncIterator[pd.DataFrame]:
        url = f"{_WS_BASE}/btcusdt@forceOrder"
        async with websockets.connect(url, ping_interval=20) as ws:
            bucket_long = 0.0
            bucket_short = 0.0
            bucket_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

            async for msg in ws:
                data = json.loads(msg)
                order = data.get("o", {})
                price = float(order.get("p", 0))
                qty = float(order.get("q", 0))
                notional = price * qty
                side = order.get("S", "")

                if side == "SELL":
                    bucket_long += notional
                else:
                    bucket_short += notional

                now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                if now > bucket_ts:
                    total = bucket_long + bucket_short
                    ratio = bucket_long / total if total > 0 else 0.5
                    yield pd.DataFrame([{
                        "timestamp": bucket_ts.isoformat(),
                        "value": ratio,
                    }])
                    bucket_long = 0.0
                    bucket_short = 0.0
                    bucket_ts = now


class BinanceFundingRateStreamCollector(StreamingCollector):
    """Real-time BTC funding rate from Binance Futures WebSocket.

    Samples the predicted funding rate once per minute from
    the markPrice stream (~3s updates).
    """

    meta = CollectorMeta(
        name="funding_rate_stream_btc",
        display_name="BTC Funding Rate Stream",
        update_frequency="hourly",
        api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream",
        domain="financial",
        category="crypto_derivatives",
        signal_type="scalar",
        collect_interval=60,
    )

    async def stream(self) -> AsyncIterator[pd.DataFrame]:
        url = f"{_WS_BASE}/btcusdt@markPrice@1s"
        async with websockets.connect(url, ping_interval=20) as ws:
            last_minute = None

            async for msg in ws:
                data = json.loads(msg)
                ts = pd.Timestamp(data["E"], unit="ms", tz="UTC")
                minute = ts.floor("min")

                if last_minute is not None and minute <= last_minute:
                    continue
                last_minute = minute

                yield pd.DataFrame([{
                    "timestamp": minute.isoformat(),
                    "value": float(data["r"]),
                }])
