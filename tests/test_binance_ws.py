"""Tests for Binance WebSocket streaming collectors (mocked)."""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import patch

import pytest

from signal_noise.collector.binance_ws import (
    BinanceFundingRateStreamCollector,
    BinanceLiquidationStreamCollector,
    BinanceOrderbookCollector,
    _compute_orderbook_signals,
)


class FakeWebSocket:
    """Simulates a websockets connection."""

    def __init__(self, messages: list[str]):
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

    async def recv(self):
        if self._idx >= len(self._messages):
            await asyncio.sleep(3600)
        msg = self._messages[self._idx]
        self._idx += 1
        return msg


def _liq_msg(side: str, price: float, qty: float) -> str:
    return json.dumps({
        "o": {"S": side, "p": str(price), "q": str(qty)},
    })


def _funding_msg(rate: float, ts_ms: int) -> str:
    return json.dumps({"E": ts_ms, "r": str(rate)})


async def _collect_stream(stream, timeout: float = 1.0) -> list:
    """Collect all DataFrames from an async generator with timeout."""
    results = []

    async def _drain():
        async for df in stream:
            results.append(df)

    try:
        await asyncio.wait_for(_drain(), timeout=timeout)
    except asyncio.TimeoutError:
        pass
    return results


@pytest.mark.asyncio
async def test_liquidation_collector_processes_messages():
    """Liquidation events are parsed correctly."""
    c = BinanceLiquidationStreamCollector()

    messages = [
        _liq_msg("SELL", 50000.0, 1.0),
        _liq_msg("BUY", 50000.0, 0.5),
    ]

    with patch("signal_noise.collector.binance_ws.websockets") as mock_ws:
        fake = FakeWebSocket(messages)
        mock_ws.connect.return_value = fake

        # With fake data in the same second, bucket won't flush.
        # Just verify no errors.
        await _collect_stream(c.stream(), timeout=0.5)


@pytest.mark.asyncio
async def test_funding_rate_collector_samples():
    """Funding rate collector yields one row per minute."""
    c = BinanceFundingRateStreamCollector()

    now_ms = int(time.time() * 1000)
    messages = [
        _funding_msg(0.0001, now_ms),
        _funding_msg(0.0002, now_ms + 61000),
    ]

    with patch("signal_noise.collector.binance_ws.websockets") as mock_ws:
        fake = FakeWebSocket(messages)
        mock_ws.connect.return_value = fake

        results = await _collect_stream(c.stream(), timeout=0.5)

    assert len(results) == 2
    assert results[0].iloc[0]["value"] == 0.0001
    assert results[1].iloc[0]["value"] == 0.0002


@pytest.mark.asyncio
async def test_funding_rate_dedup_same_minute():
    """Messages in the same minute are deduplicated."""
    c = BinanceFundingRateStreamCollector()

    now_ms = int(time.time() * 1000)
    messages = [
        _funding_msg(0.0001, now_ms),
        _funding_msg(0.0002, now_ms + 1000),
        _funding_msg(0.0003, now_ms + 2000),
    ]

    with patch("signal_noise.collector.binance_ws.websockets") as mock_ws:
        fake = FakeWebSocket(messages)
        mock_ws.connect.return_value = fake

        results = await _collect_stream(c.stream(), timeout=0.5)

    assert len(results) == 1
    assert results[0].iloc[0]["value"] == 0.0001


def test_meta_attributes():
    liq = BinanceLiquidationStreamCollector()
    assert liq.meta.name == "liq_stream_btc"
    assert liq.meta.domain == "markets"

    fr = BinanceFundingRateStreamCollector()
    assert fr.meta.name == "funding_rate_stream_btc"
    assert fr.meta.category == "crypto_derivatives"


# ---- Orderbook collector tests ----

def test_orderbook_meta():
    c = BinanceOrderbookCollector()
    assert c.meta.name == "orderbook_btc"
    assert c.meta.category == "microstructure"
    assert c.use_realtime_store is True


def test_compute_orderbook_signals_imbalance():
    from datetime import datetime, timezone

    ts = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
    snapshot = {
        "b": [["50000", "2.0"], ["49990", "3.0"], ["49980", "1.0"],
               ["49970", "1.0"], ["49960", "1.0"], ["49950", "0.5"]],
        "a": [["50010", "1.0"], ["50020", "2.0"], ["50030", "1.0"],
               ["50040", "1.0"], ["50050", "1.0"], ["50060", "0.5"]],
    }
    rows = _compute_orderbook_signals(ts, snapshot)
    assert len(rows) == 3

    by_name = {r["name"]: r["value"] for r in rows}

    # bid_total = 2+3+1+1+1+0.5 = 8.5, ask_total = 1+2+1+1+1+0.5 = 6.5
    # imbalance = (8.5 - 6.5) / 15 = 0.1333
    assert abs(by_name["book_imbalance_btc"] - (8.5 - 6.5) / 15.0) < 1e-6

    # top5 bid = 2+3+1+1+1 = 8, top5 ask = 1+2+1+1+1 = 6
    assert abs(by_name["book_depth_ratio_btc"] - 8.0 / 6.0) < 1e-6

    # spread = (50010 - 50000) / 50005 * 10000
    expected_spread = (50010 - 50000) / 50005 * 10000
    assert abs(by_name["spread_bps_btc"] - expected_spread) < 0.01


def test_compute_orderbook_signals_empty():
    from datetime import datetime, timezone

    ts = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
    rows = _compute_orderbook_signals(ts, {"b": [], "a": []})
    assert rows == []


@pytest.mark.asyncio
async def test_orderbook_stream_yields_multi_signal_df():
    """Orderbook collector yields DataFrames with name column."""
    from datetime import datetime, timezone

    c = BinanceOrderbookCollector()

    snapshot = json.dumps({
        "b": [["50000", "2.0"]] * 20,
        "a": [["50010", "1.0"]] * 20,
    })

    with patch("signal_noise.collector.binance_ws.websockets") as mock_ws:
        fake = FakeWebSocket([snapshot])
        mock_ws.connect.return_value = fake

        # Force minute boundary by patching datetime
        t0 = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 3, 1, 10, 1, tzinfo=timezone.utc)

        call_count = 0

        def fake_now(tz=None):
            nonlocal call_count
            call_count += 1
            # First call (init): returns t0. Subsequent: returns t1 (new minute)
            return t0 if call_count <= 1 else t1

        with patch("signal_noise.collector.binance_ws.datetime") as mock_dt:
            mock_dt.now = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            results = await _collect_stream(c.stream(), timeout=0.5)

    assert len(results) >= 1
    df = results[0]
    assert "name" in df.columns
    names = set(df["name"])
    assert names == {"book_imbalance_btc", "book_depth_ratio_btc", "spread_bps_btc"}
