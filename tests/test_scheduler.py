from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.scheduler.loop import run_collector_loop, run_scheduler
from signal_noise.store.sqlite_store import SignalStore


class _DummyCollector(BaseCollector):
    meta = CollectorMeta(
        name="dummy",
        display_name="Dummy",
        update_frequency="daily",
        api_docs_url="",
        domain="financial",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        return pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})


@pytest.fixture
def store(tmp_path: Path) -> SignalStore:
    s = SignalStore(tmp_path / "test.db")
    yield s
    s.close()


class TestRunCollectorLoop:
    @pytest.mark.asyncio
    async def test_fetches_and_saves(self, store: SignalStore) -> None:
        collector = _DummyCollector()
        call_count = 0

        original_sleep = asyncio.sleep

        async def _fake_sleep(seconds: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError
            await original_sleep(0)

        with patch("signal_noise.scheduler.loop.asyncio.sleep", side_effect=_fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await run_collector_loop(collector, store, interval=3600)

        result = store.get_data("dummy")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_continues_on_error(self, store: SignalStore) -> None:
        collector = _DummyCollector()
        collector.fetch = MagicMock(side_effect=[Exception("boom"), pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})])
        call_count = 0

        async def _fake_sleep(seconds: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError

        with patch("signal_noise.scheduler.loop.asyncio.sleep", side_effect=_fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await run_collector_loop(collector, store, interval=3600)

        result = store.get_data("dummy")
        assert len(result) == 1


class TestRunScheduler:
    @pytest.mark.asyncio
    async def test_creates_tasks_for_all(self, store: SignalStore) -> None:
        collectors = {"dummy": _DummyCollector}
        call_count = 0

        async def _fake_sleep(seconds: float) -> None:
            nonlocal call_count
            call_count += 1
            # Allow jitter sleep (1st call), cancel on interval sleep (2nd)
            if call_count >= 2:
                raise asyncio.CancelledError

        with patch("signal_noise.scheduler.loop.asyncio.sleep", side_effect=_fake_sleep):
            with patch("signal_noise.collector.COLLECTORS", collectors):
                with pytest.raises(asyncio.CancelledError):
                    await run_scheduler(store, collectors=collectors)

        result = store.get_data("dummy")
        assert len(result) == 1
