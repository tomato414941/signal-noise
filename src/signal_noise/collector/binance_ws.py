"""Binance Futures WebSocket streaming collectors.

Real-time liquidation, funding rate, orderbook, and trade flow data
via WebSocket. Data is aggregated into 1-minute buckets before yielding.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

import pandas as pd
import websockets

from signal_noise.collector.base import CollectorMeta
from signal_noise.collector.streaming import StreamingCollector

log = logging.getLogger(__name__)

_WS_BASE = "wss://fstream.binance.com/ws"
_STREAM_ASSETS: list[tuple[str, str, str]] = [
    ("BTCUSDT", "btc", "BTC"),
    ("ETHUSDT", "eth", "ETH"),
    ("SOLUSDT", "sol", "SOL"),
    ("BNBUSDT", "bnb", "BNB"),
    ("XRPUSDT", "xrp", "XRP"),
]

def _make_liquidation_stream_collector(
    symbol: str, key: str, display: str,
) -> type[StreamingCollector]:
    stream_symbol = symbol.lower()

    class _Collector(StreamingCollector):
        """Real-time liquidation stream from Binance Futures."""

        meta = CollectorMeta(
            name=f"liq_stream_{key}",
            display_name=f"{display} Liquidation Stream",
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#liquidation-order-streams",
            domain="markets",
            category="crypto_derivatives",
            signal_type="scalar",
            collect_interval=60,
        )

        async def stream(self) -> AsyncIterator[pd.DataFrame]:
            url = f"{_WS_BASE}/{stream_symbol}@forceOrder"
            async with websockets.connect(url, ping_interval=20) as ws:
                bucket_long = 0.0
                bucket_short = 0.0
                bucket_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

                while True:
                    deadline = bucket_ts + timedelta(minutes=1)
                    now = datetime.now(timezone.utc)
                    if now >= deadline:
                        total = bucket_long + bucket_short
                        ratio = bucket_long / total if total > 0 else 0.5
                        yield pd.DataFrame([{
                            "timestamp": bucket_ts.isoformat(),
                            "value": ratio,
                        }])
                        bucket_long = 0.0
                        bucket_short = 0.0
                        bucket_ts = now.replace(second=0, microsecond=0)
                        continue

                    timeout = (deadline - now).total_seconds()
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        continue
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

    if key == "btc":
        _Collector.__name__ = "BinanceLiquidationStreamCollector"
        _Collector.__qualname__ = "BinanceLiquidationStreamCollector"
    else:
        _Collector.__name__ = f"BinanceLiquidationStreamCollector_{key}"
        _Collector.__qualname__ = f"BinanceLiquidationStreamCollector_{key}"
    return _Collector


def _make_funding_rate_stream_collector(
    symbol: str, key: str, display: str,
) -> type[StreamingCollector]:
    stream_symbol = symbol.lower()

    class _Collector(StreamingCollector):
        """Real-time funding rate stream from Binance Futures."""

        meta = CollectorMeta(
            name=f"funding_rate_stream_{key}",
            display_name=f"{display} Funding Rate Stream",
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream",
            domain="markets",
            category="crypto_derivatives",
            signal_type="scalar",
            collect_interval=60,
        )

        async def stream(self) -> AsyncIterator[pd.DataFrame]:
            url = f"{_WS_BASE}/{stream_symbol}@markPrice@1s"
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

    if key == "btc":
        _Collector.__name__ = "BinanceFundingRateStreamCollector"
        _Collector.__qualname__ = "BinanceFundingRateStreamCollector"
    else:
        _Collector.__name__ = f"BinanceFundingRateStreamCollector_{key}"
        _Collector.__qualname__ = f"BinanceFundingRateStreamCollector_{key}"
    return _Collector


def _make_orderbook_collector(
    symbol: str, key: str, display: str,
) -> type[StreamingCollector]:
    stream_symbol = symbol.lower()

    class _Collector(StreamingCollector):
        """Orderbook depth stream from Binance Futures."""

        meta = CollectorMeta(
            name=f"orderbook_{key}",
            display_name=f"{display} Orderbook Depth",
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#partial-book-depth-streams",
            domain="markets",
            category="microstructure",
            signal_type="scalar",
            collect_interval=60,
        )
        use_realtime_store = True

        def registered_meta_names(self) -> list[str]:
            return []

        async def stream(self) -> AsyncIterator[pd.DataFrame]:
            url = f"{_WS_BASE}/{stream_symbol}@depth20@100ms"
            async with websockets.connect(url, ping_interval=20) as ws:
                bucket_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                last_snapshot: dict | None = None

                async for msg in ws:
                    data = json.loads(msg)
                    last_snapshot = data

                    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                    if now > bucket_ts and last_snapshot is not None:
                        rows = _compute_orderbook_signals(bucket_ts, last_snapshot, key)
                        if rows:
                            yield pd.DataFrame(rows)
                        last_snapshot = None
                        bucket_ts = now

    if key == "btc":
        _Collector.__name__ = "BinanceOrderbookCollector"
        _Collector.__qualname__ = "BinanceOrderbookCollector"
    else:
        _Collector.__name__ = f"BinanceOrderbookCollector_{key}"
        _Collector.__qualname__ = f"BinanceOrderbookCollector_{key}"
    return _Collector


def _compute_orderbook_signals(
    ts: datetime, snapshot: dict, key: str = "btc",
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
        {"timestamp": ts_str, "value": imbalance, "name": f"book_imbalance_{key}"},
        {"timestamp": ts_str, "value": depth_ratio, "name": f"book_depth_ratio_{key}"},
        {"timestamp": ts_str, "value": spread_bps, "name": f"spread_bps_{key}"},
    ]


class VPINCalculator:
    """Volume-synchronized Probability of Informed Trading.

    Reference: Easley, López de Prado, O'Hara (2012).
    Fills fixed-size volume buckets and computes order imbalance
    across the last N completed buckets.
    """

    def __init__(self, bucket_size: float = 1.0, n_buckets: int = 50):
        self._bucket_size = bucket_size
        self._n_buckets = n_buckets
        self._buckets: deque[tuple[float, float]] = deque(maxlen=n_buckets)
        self._current_buy = 0.0
        self._current_sell = 0.0
        self._current_volume = 0.0

    def update(self, qty: float, is_buy: bool) -> None:
        """Add a trade. May complete one or more buckets."""
        remaining = qty
        while remaining > 1e-12:
            space = self._bucket_size - self._current_volume
            fill = min(remaining, space)
            if is_buy:
                self._current_buy += fill
            else:
                self._current_sell += fill
            self._current_volume += fill
            remaining -= fill
            if self._current_volume >= self._bucket_size - 1e-10:
                self._buckets.append((self._current_buy, self._current_sell))
                self._current_buy = 0.0
                self._current_sell = 0.0
                self._current_volume = 0.0

    @property
    def value(self) -> float | None:
        """VPIN value, or None if fewer than n_buckets completed."""
        if len(self._buckets) < self._n_buckets:
            return None
        total = sum(
            abs(b - s) / self._bucket_size
            for b, s in self._buckets
        )
        return total / len(self._buckets)


def _make_trade_flow_collector(
    symbol: str, key: str, display: str,
) -> type[StreamingCollector]:
    stream_symbol = symbol.lower()

    class _Collector(StreamingCollector):
        """Trade flow stream from Binance Futures aggTrade."""

        meta = CollectorMeta(
            name=f"trade_flow_{key}",
            display_name=f"{display} Trade Flow",
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams",
            domain="markets",
            category="microstructure",
            signal_type="scalar",
            collect_interval=60,
        )
        use_realtime_store = True

        _LARGE_TRADE_USD = 100_000.0

        async def stream(self) -> AsyncIterator[pd.DataFrame]:
            url = f"{_WS_BASE}/{stream_symbol}@aggTrade"
            vpin_calc = VPINCalculator(bucket_size=1.0, n_buckets=50)

            async with websockets.connect(url, ping_interval=20) as ws:
                bucket_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                buy_vol = 0.0
                sell_vol = 0.0
                large_count = 0

                async for msg in ws:
                    data = json.loads(msg)
                    qty = float(data["q"])
                    price = float(data["p"])
                    # m=True means maker is buyer → taker is seller
                    is_buy = not data["m"]

                    if is_buy:
                        buy_vol += qty
                    else:
                        sell_vol += qty

                    if price * qty >= self._LARGE_TRADE_USD:
                        large_count += 1

                    vpin_calc.update(qty, is_buy)

                    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                    if now > bucket_ts:
                        ts_str = bucket_ts.isoformat()
                        rows: list[dict] = [
                            {"timestamp": ts_str, "value": buy_vol - sell_vol,
                             "name": f"trade_flow_{key}"},
                            {"timestamp": ts_str, "value": large_count,
                             "name": f"large_trade_count_{key}"},
                        ]
                        vpin_val = vpin_calc.value
                        if vpin_val is not None:
                            rows.append({
                                "timestamp": ts_str, "value": vpin_val,
                                "name": f"vpin_{key}",
                            })
                        yield pd.DataFrame(rows)
                        buy_vol = 0.0
                        sell_vol = 0.0
                        large_count = 0
                        bucket_ts = now

    if key == "btc":
        _Collector.__name__ = "BinanceTradeFlowCollector"
        _Collector.__qualname__ = "BinanceTradeFlowCollector"
    else:
        _Collector.__name__ = f"BinanceTradeFlowCollector_{key}"
        _Collector.__qualname__ = f"BinanceTradeFlowCollector_{key}"
    return _Collector


BinanceLiquidationStreamCollector = _make_liquidation_stream_collector("BTCUSDT", "btc", "BTC")
BinanceFundingRateStreamCollector = _make_funding_rate_stream_collector("BTCUSDT", "btc", "BTC")
BinanceOrderbookCollector = _make_orderbook_collector("BTCUSDT", "btc", "BTC")
BinanceTradeFlowCollector = _make_trade_flow_collector("BTCUSDT", "btc", "BTC")


def get_binance_ws_collectors() -> dict[str, type[StreamingCollector]]:
    out: dict[str, type[StreamingCollector]] = {}
    for symbol, key, display in _STREAM_ASSETS:
        if key == "btc":
            liq = BinanceLiquidationStreamCollector
            funding = BinanceFundingRateStreamCollector
            orderbook = BinanceOrderbookCollector
            trade = BinanceTradeFlowCollector
        else:
            liq = _make_liquidation_stream_collector(symbol, key, display)
            funding = _make_funding_rate_stream_collector(symbol, key, display)
            orderbook = _make_orderbook_collector(symbol, key, display)
            trade = _make_trade_flow_collector(symbol, key, display)
        out[f"liq_stream_{key}"] = liq
        out[f"funding_rate_stream_{key}"] = funding
        out[f"orderbook_{key}"] = orderbook
        out[f"trade_flow_{key}"] = trade
    return out
