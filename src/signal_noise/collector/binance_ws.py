"""Binance Futures WebSocket streaming collectors.

Real-time liquidation, funding rate, orderbook, and trade flow data
via WebSocket. Data is aggregated into 1-minute buckets before yielding.
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


class BinanceOrderbookCollector(StreamingCollector):
    """BTC orderbook depth from Binance Futures WebSocket.

    Aggregates 100ms snapshots into 1-minute buckets, yielding three
    derived signals: book_imbalance, book_depth_ratio, spread_bps.
    """

    meta = CollectorMeta(
        name="orderbook_btc",
        display_name="BTC Orderbook Depth",
        update_frequency="hourly",
        api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#partial-book-depth-streams",
        domain="financial",
        category="microstructure",
        signal_type="scalar",
        collect_interval=60,
    )
    use_realtime_store = True

    async def stream(self) -> AsyncIterator[pd.DataFrame]:
        url = f"{_WS_BASE}/btcusdt@depth20@100ms"
        async with websockets.connect(url, ping_interval=20) as ws:
            bucket_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            last_snapshot: dict | None = None

            async for msg in ws:
                data = json.loads(msg)
                last_snapshot = data

                now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                if now > bucket_ts and last_snapshot is not None:
                    rows = _compute_orderbook_signals(bucket_ts, last_snapshot)
                    if rows:
                        yield pd.DataFrame(rows)
                    last_snapshot = None
                    bucket_ts = now


def _compute_orderbook_signals(
    ts: datetime, snapshot: dict,
) -> list[dict]:
    """Compute derived signals from an orderbook snapshot."""
    bids = snapshot.get("b") or snapshot.get("bids", [])
    asks = snapshot.get("a") or snapshot.get("asks", [])
    if not bids or not asks:
        return []

    bid_qtys = [float(b[1]) for b in bids]
    ask_qtys = [float(a[1]) for a in asks]
    total_bid = sum(bid_qtys)
    total_ask = sum(ask_qtys)
    total = total_bid + total_ask

    ts_str = ts.isoformat()

    # book_imbalance: (bid_vol - ask_vol) / total across all levels
    imbalance = (total_bid - total_ask) / total if total > 0 else 0.0

    # book_depth_ratio: top 5 bid depth / top 5 ask depth
    top5_bid = sum(bid_qtys[:5])
    top5_ask = sum(ask_qtys[:5])
    depth_ratio = top5_bid / top5_ask if top5_ask > 0 else 1.0

    # spread_bps: (best_ask - best_bid) / mid * 10000
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid = (best_bid + best_ask) / 2
    spread_bps = (best_ask - best_bid) / mid * 10_000 if mid > 0 else 0.0

    return [
        {"timestamp": ts_str, "value": imbalance, "name": "book_imbalance_btc"},
        {"timestamp": ts_str, "value": depth_ratio, "name": "book_depth_ratio_btc"},
        {"timestamp": ts_str, "value": spread_bps, "name": "spread_bps_btc"},
    ]
