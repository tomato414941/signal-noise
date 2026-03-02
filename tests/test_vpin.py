"""Tests for VPINCalculator and BinanceTradeFlowCollector."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from signal_noise.collector.binance_ws import (
    BinanceTradeFlowCollector,
    VPINCalculator,
)


# ---- VPINCalculator tests ----

def test_vpin_returns_none_before_n_buckets():
    calc = VPINCalculator(bucket_size=1.0, n_buckets=5)
    calc.update(1.0, is_buy=True)  # fills 1 bucket
    assert calc.value is None


def test_vpin_basic_calculation():
    calc = VPINCalculator(bucket_size=1.0, n_buckets=3)
    # Bucket 1: all buy → |1-0|/1 = 1.0
    calc.update(1.0, is_buy=True)
    # Bucket 2: all sell → |0-1|/1 = 1.0
    calc.update(1.0, is_buy=False)
    # Bucket 3: half/half → |0.5-0.5|/1 = 0.0
    calc.update(0.5, is_buy=True)
    calc.update(0.5, is_buy=False)

    v = calc.value
    assert v is not None
    # mean(1.0, 1.0, 0.0) = 0.6667
    assert abs(v - 2.0 / 3.0) < 1e-6


def test_vpin_large_trade_spans_buckets():
    calc = VPINCalculator(bucket_size=1.0, n_buckets=3)
    # Single trade of 3.0 fills 3 buckets, all buy
    calc.update(3.0, is_buy=True)

    v = calc.value
    assert v is not None
    # Each bucket: |1-0|/1 = 1.0, mean = 1.0
    assert abs(v - 1.0) < 1e-6


def test_vpin_mixed_trades():
    calc = VPINCalculator(bucket_size=2.0, n_buckets=2)
    # Trade 1: buy 1.5
    calc.update(1.5, is_buy=True)
    # Trade 2: sell 0.5 → completes bucket 1 (buy=1.5, sell=0.5)
    calc.update(0.5, is_buy=False)
    # Trade 3: sell 2.0 → completes bucket 2 (buy=0, sell=2.0)
    calc.update(2.0, is_buy=False)

    v = calc.value
    assert v is not None
    # Bucket 1: |1.5-0.5|/2 = 0.5
    # Bucket 2: |0-2|/2 = 1.0
    # mean = 0.75
    assert abs(v - 0.75) < 1e-6


def test_vpin_rolling_window():
    calc = VPINCalculator(bucket_size=1.0, n_buckets=2)
    # Fill 3 buckets; only last 2 count (deque maxlen=2)
    calc.update(1.0, is_buy=True)   # bucket 1: all buy
    calc.update(1.0, is_buy=False)  # bucket 2: all sell
    calc.update(0.5, is_buy=True)   # bucket 3: half/half
    calc.update(0.5, is_buy=False)

    v = calc.value
    assert v is not None
    # Buckets kept: [all sell=1.0, half/half=0.0], mean=0.5
    assert abs(v - 0.5) < 1e-6


# ---- BinanceTradeFlowCollector tests ----

class FakeWebSocket:
    def __init__(self, messages):
        self._messages = messages
        self._idx = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._idx >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._idx]
        self._idx += 1
        return msg


def _agg_trade_msg(price: float, qty: float, is_buyer_maker: bool) -> str:
    return json.dumps({
        "e": "aggTrade", "s": "BTCUSDT",
        "p": str(price), "q": str(qty), "m": is_buyer_maker,
    })


async def _collect_stream(stream, timeout=1.0):
    results = []
    async def _drain():
        async for df in stream:
            results.append(df)
    try:
        await asyncio.wait_for(_drain(), timeout=timeout)
    except asyncio.TimeoutError:
        pass
    return results


def test_trade_flow_meta():
    c = BinanceTradeFlowCollector()
    assert c.meta.name == "trade_flow_btc"
    assert c.meta.category == "microstructure"
    assert c.use_realtime_store is True


@pytest.mark.asyncio
async def test_trade_flow_aggregation():
    c = BinanceTradeFlowCollector()

    messages = [
        _agg_trade_msg(50000.0, 0.5, False),   # taker buy 0.5
        _agg_trade_msg(50000.0, 0.3, True),    # taker sell 0.3
        _agg_trade_msg(50000.0, 3.0, False),   # taker buy 3.0, large ($150k)
    ]

    t0 = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 3, 1, 10, 1, tzinfo=timezone.utc)
    call_count = 0

    def fake_now(tz=None):
        nonlocal call_count
        call_count += 1
        # 1=init, 2-4=after each message check; flush on last msg only
        return t0 if call_count <= 3 else t1

    with patch("signal_noise.collector.binance_ws.websockets") as mock_ws:
        fake = FakeWebSocket(messages)
        mock_ws.connect.return_value = fake

        with patch("signal_noise.collector.binance_ws.datetime") as mock_dt:
            mock_dt.now = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            results = await _collect_stream(c.stream(), timeout=0.5)

    assert len(results) >= 1
    df = results[0]
    by_name = {row["name"]: row["value"] for _, row in df.iterrows()}

    # trade_flow = buy(0.5+3.0) - sell(0.3) = 3.2
    assert abs(by_name["trade_flow_btc"] - 3.2) < 1e-6
    # large_trade_count: 1 trade ($150k > $100k)
    assert by_name["large_trade_count_btc"] == 1
    # VPIN not present yet (need 50 buckets)
    assert "vpin_btc" not in by_name
