from __future__ import annotations

import asyncio

import pandas as pd
import pytest

from signal_noise.collector.base import CollectorMeta
from signal_noise.collector.streaming import StreamingCollector


class MockStreamCollector(StreamingCollector):
    """Test collector that yields a fixed number of DataFrames."""

    meta = CollectorMeta(
        name="mock_stream",
        display_name="Mock Stream",
        update_frequency="hourly",
        api_docs_url="",
        domain="markets",
        category="crypto",
        signal_type="scalar",
        collect_interval=60,
    )

    def __init__(self, n_yields: int = 3, fail_after: int = 0):
        super().__init__()
        self.n_yields = n_yields
        self.fail_after = fail_after
        self.call_count = 0

    async def stream(self):
        self.call_count += 1
        for i in range(self.n_yields):
            if self.fail_after and i + 1 >= self.fail_after and self.call_count == 1:
                raise ConnectionError("Mock disconnect")
            yield pd.DataFrame([{
                "timestamp": f"2026-03-01T00:0{i}:00",
                "value": float(i),
            }])
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_fetch_returns_empty():
    c = MockStreamCollector()
    df = c.fetch()
    assert df.empty


@pytest.mark.asyncio
async def test_stream_yields_dataframes():
    c = MockStreamCollector(n_yields=3)
    results = []
    async for df in c.stream():
        results.append(df)
    assert len(results) == 3
    assert results[0].iloc[0]["value"] == 0.0
    assert results[2].iloc[0]["value"] == 2.0


@pytest.mark.asyncio
async def test_connect_with_retry_recovers():
    c = MockStreamCollector(n_yields=3, fail_after=2)
    c.reconnect_delay = 0.01
    c.max_reconnect_delay = 0.05

    results = []
    async for df in c.connect_with_retry():
        results.append(df)
        if len(results) >= 4:
            break

    # First call: 1 yield then fail at i=1. Second call: all 3 yields.
    assert len(results) == 4


@pytest.mark.asyncio
async def test_connect_with_retry_cancel():
    c = MockStreamCollector(n_yields=100)

    async def _consume():
        async for _ in c.connect_with_retry():
            pass

    task = asyncio.create_task(_consume())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
