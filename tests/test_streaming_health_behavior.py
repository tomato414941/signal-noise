from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from signal_noise.collector.binance_ws import (
    BinanceLiquidationStreamCollector,
    BinanceOrderbookCollector,
)
from signal_noise.scheduler.loop import _sync_streaming_meta
from signal_noise.store.sqlite_store import SignalStore


class IdleWebSocket:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def recv(self) -> str:
        await asyncio.sleep(3600)
        return ""


class MessageWebSocket:
    def __init__(self, messages: list[str]):
        self._messages = messages
        self._index = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def recv(self) -> str:
        if self._index < len(self._messages):
            msg = self._messages[self._index]
            self._index += 1
            return msg
        await asyncio.sleep(3600)
        return ""


async def _collect_stream(stream, timeout: float = 0.5) -> list:
    results = []

    async def _drain():
        async for df in stream:
            results.append(df)

    try:
        await asyncio.wait_for(_drain(), timeout=timeout)
    except asyncio.TimeoutError:
        pass
    return results


@pytest.fixture
def store(tmp_path: Path) -> SignalStore:
    s = SignalStore(tmp_path / "test.db")
    yield s
    s.close()


@pytest.mark.asyncio
async def test_liquidation_stream_emits_heartbeat_without_events():
    collector = BinanceLiquidationStreamCollector()
    t0 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 3, 1, 10, 1, 0, tzinfo=timezone.utc)
    now_values = iter([t0, t1, t1])

    def fake_now(tz=None):
        return next(now_values)

    with patch("signal_noise.collector.binance_ws.websockets") as mock_ws:
        mock_ws.connect.return_value = IdleWebSocket()
        with patch("signal_noise.collector.binance_ws.datetime") as mock_dt:
            mock_dt.now = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            results = await _collect_stream(collector.stream())

    assert len(results) == 1
    assert results[0].iloc[0]["timestamp"] == t0.isoformat()
    assert results[0].iloc[0]["value"] == 0.5


@pytest.mark.asyncio
async def test_liquidation_stream_flushes_accumulated_bucket_on_minute_boundary():
    collector = BinanceLiquidationStreamCollector()
    message = json.dumps({"o": {"S": "SELL", "p": "100", "q": "2"}})
    t0 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    t0_mid = datetime(2026, 3, 1, 10, 0, 30, tzinfo=timezone.utc)
    t1 = datetime(2026, 3, 1, 10, 1, 0, tzinfo=timezone.utc)
    now_values = iter([t0, t0_mid, t1, t1])

    def fake_now(tz=None):
        return next(now_values)

    with patch("signal_noise.collector.binance_ws.websockets") as mock_ws:
        mock_ws.connect.return_value = MessageWebSocket([message])
        with patch("signal_noise.collector.binance_ws.datetime") as mock_dt:
            mock_dt.now = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            results = await _collect_stream(collector.stream())

    assert len(results) == 1
    assert results[0].iloc[0]["timestamp"] == t0.isoformat()
    assert results[0].iloc[0]["value"] == 1.0


def test_sync_streaming_meta_removes_orderbook_parent_meta(store: SignalStore):
    store.save_meta("orderbook_btc", "markets", "microstructure", 60, "scalar")
    collector = BinanceOrderbookCollector()

    _sync_streaming_meta(store, collector)

    assert store.get_meta("orderbook_btc") is None
