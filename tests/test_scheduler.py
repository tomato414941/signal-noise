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


class _FailThenSucceedCollector(BaseCollector):
    """Collector that fails N times then succeeds."""
    meta = CollectorMeta(
        name="flaky",
        display_name="Flaky",
        update_frequency="daily",
        api_docs_url="",
        domain="financial",
        category="crypto",
    )

    def __init__(self, fail_count: int = 5, **kwargs):
        super().__init__(**kwargs)
        self._fail_count = fail_count
        self._call_count = 0

    def fetch(self) -> pd.DataFrame:
        self._call_count += 1
        if self._call_count <= self._fail_count:
            raise RuntimeError(f"fail #{self._call_count}")
        return pd.DataFrame({"timestamp": ["2024-01-01"], "value": [42.0]})


class _AlwaysFailCollector(BaseCollector):
    meta = CollectorMeta(
        name="broken",
        display_name="Broken",
        update_frequency="daily",
        api_docs_url="",
        domain="financial",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        raise RuntimeError("always fail")


class TestCircuitBreakerRecovery:
    @pytest.mark.asyncio
    async def test_half_open_recovery_on_success(self, store: SignalStore) -> None:
        """After max_failures, cooldown then half-open retry succeeds."""
        collector = _FailThenSucceedCollector(fail_count=5)
        sleep_calls: list[float] = []

        async def _fake_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)
            if len(sleep_calls) >= 7:
                raise asyncio.CancelledError

        with patch("signal_noise.scheduler.loop.asyncio.sleep", side_effect=_fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await run_collector_loop(
                    collector, store, interval=60,
                    max_failures=5, base_cooldown=10.0,
                )

        result = store.get_data("flaky")
        assert len(result) == 1
        assert 10.0 in sleep_calls

    @pytest.mark.asyncio
    async def test_half_open_retry_fails_doubles_cooldown(self, store: SignalStore) -> None:
        """After half-open retry fails, cooldown doubles."""
        collector = _AlwaysFailCollector()
        sleep_calls: list[float] = []

        async def _fake_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)
            if any(s == 20.0 for s in sleep_calls):
                raise asyncio.CancelledError

        with patch("signal_noise.scheduler.loop.asyncio.sleep", side_effect=_fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await run_collector_loop(
                    collector, store, interval=60,
                    max_failures=5, base_cooldown=10.0,
                )

        assert 10.0 in sleep_calls
        assert 20.0 in sleep_calls

    @pytest.mark.asyncio
    async def test_cooldown_capped_at_max(self, store: SignalStore) -> None:
        """Cooldown should not exceed max_cooldown."""
        collector = _AlwaysFailCollector()
        sleep_calls: list[float] = []
        cooldown_count = 0

        async def _fake_sleep(seconds: float) -> None:
            nonlocal cooldown_count
            sleep_calls.append(seconds)
            if seconds > 60:
                cooldown_count += 1
            if cooldown_count >= 3:
                raise asyncio.CancelledError

        with patch("signal_noise.scheduler.loop.asyncio.sleep", side_effect=_fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await run_collector_loop(
                    collector, store, interval=60,
                    max_failures=5, base_cooldown=100.0, max_cooldown=200.0,
                )

        cooldown_sleeps = [s for s in sleep_calls if s > 60]
        assert cooldown_sleeps[0] == 100.0
        assert cooldown_sleeps[1] == 200.0
        assert cooldown_sleeps[2] == 200.0


class _SlowCollector(BaseCollector):
    """Collector that blocks longer than the fetch timeout."""
    meta = CollectorMeta(
        name="slow",
        display_name="Slow",
        update_frequency="daily",
        api_docs_url="",
        domain="financial",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        import time
        time.sleep(10)
        return pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})


class TestFetchTimeout:
    @pytest.mark.asyncio
    async def test_fetch_timeout_triggers_failure(self, store: SignalStore) -> None:
        """A slow collector should be timed out and recorded as failed."""
        collector = _SlowCollector()
        call_count = 0

        async def _fake_sleep(seconds: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError

        with patch("signal_noise.scheduler.loop.asyncio.sleep", side_effect=_fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await run_collector_loop(
                    collector, store, interval=3600,
                    fetch_timeout=0.1,
                )

        result = store.get_data("slow")
        assert result.empty

    @pytest.mark.asyncio
    async def test_normal_collector_within_timeout(self, store: SignalStore) -> None:
        """A fast collector should not be affected by the timeout."""
        collector = _DummyCollector()
        call_count = 0

        async def _fake_sleep(seconds: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError

        with patch("signal_noise.scheduler.loop.asyncio.sleep", side_effect=_fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await run_collector_loop(
                    collector, store, interval=3600,
                    fetch_timeout=10.0,
                )

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
                # return_exceptions=True means gather returns exceptions instead of raising
                await run_scheduler(store, collectors=collectors)

        result = store.get_data("dummy")
        assert len(result) == 1
