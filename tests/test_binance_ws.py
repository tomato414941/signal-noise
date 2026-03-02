"""Tests for Binance WebSocket streaming collectors (mocked)."""

from __future__ import annotations

import json
import time
from unittest.mock import patch

import pytest

from signal_noise.collector.binance_ws import (
    BinanceFundingRateStreamCollector,
    BinanceLiquidationStreamCollector,
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


def _liq_msg(side: str, price: float, qty: float) -> str:
    return json.dumps({
        "o": {"S": side, "p": str(price), "q": str(qty)},
    })


def _funding_msg(rate: float, ts_ms: int) -> str:
    return json.dumps({"E": ts_ms, "r": str(rate)})


@pytest.mark.asyncio
async def test_liquidation_collector_processes_messages():
    """Liquidation events are processed without error."""
    c = BinanceLiquidationStreamCollector()

    messages = [
        _liq_msg("SELL", 50000.0, 1.0),
        _liq_msg("BUY", 50000.0, 0.5),
    ]

    with patch("signal_noise.collector.binance_ws.websockets") as mock_ws:
        fake = FakeWebSocket(messages)
        mock_ws.connect.return_value = fake

        results = []
        async for df in c.stream():
            results.append(df)

    # All messages in the same minute bucket → no flush until minute changes.
    # With only 2 messages that don't cross a minute boundary, 0 yields is expected.
    # This verifies processing doesn't crash.


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

        results = []
        async for df in c.stream():
            results.append(df)

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

        results = []
        async for df in c.stream():
            results.append(df)

    # All in same minute -> only first yields
    assert len(results) == 1
    assert results[0].iloc[0]["value"] == 0.0001


def test_meta_attributes():
    liq = BinanceLiquidationStreamCollector()
    assert liq.meta.name == "liq_stream_btc"
    assert liq.meta.domain == "financial"

    fr = BinanceFundingRateStreamCollector()
    assert fr.meta.name == "funding_rate_stream_btc"
    assert fr.meta.category == "crypto_derivatives"
