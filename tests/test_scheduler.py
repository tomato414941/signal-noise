from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pandas as pd
import pytest

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.scheduler.loop import (
    _dispatcher,
    _worker,
    run_scheduler,
)
from signal_noise.scheduler.state import (
    CircuitBreakerState,
    ScheduleEntry,
    ScheduleQueue,
)
from signal_noise.store.sqlite_store import SignalStore


class _DummyCollector(BaseCollector):
    meta = CollectorMeta(
        name="dummy",
        display_name="Dummy",
        update_frequency="daily",
        api_docs_url="",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        return pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})


class _FailCollector(BaseCollector):
    meta = CollectorMeta(
        name="broken",
        display_name="Broken",
        update_frequency="daily",
        api_docs_url="",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        raise RuntimeError("always fail")


@pytest.fixture
def store(tmp_path: Path) -> SignalStore:
    s = SignalStore(tmp_path / "test.db")
    yield s
    s.close()


# ── ScheduleQueue tests ──


class TestScheduleQueue:
    def test_push_and_pop_due(self) -> None:
        q = ScheduleQueue()
        entry = ScheduleEntry(
            next_run=time.monotonic() - 1,
            name="test",
            interval=3600,
            meta_dict={"domain": "markets", "category": "crypto"},
        )
        q.push(entry)
        assert len(q) == 1
        popped = q.pop_due()
        assert popped is not None
        assert popped.name == "test"
        assert len(q) == 0

    def test_pop_due_returns_none_when_not_due(self) -> None:
        q = ScheduleQueue()
        entry = ScheduleEntry(
            next_run=time.monotonic() + 9999,
            name="future",
            interval=3600,
            meta_dict={},
        )
        q.push(entry)
        assert q.pop_due() is None

    def test_reschedule(self) -> None:
        q = ScheduleQueue()
        entry = ScheduleEntry(
            next_run=time.monotonic() - 1,
            name="test",
            interval=10,
            meta_dict={},
        )
        q.push(entry)
        popped = q.pop_due()
        assert popped is not None
        q.reschedule(popped)
        assert len(q) == 1
        assert q.pop_due() is None  # not due yet (10s in the future)

    def test_peek_delay(self) -> None:
        q = ScheduleQueue()
        entry = ScheduleEntry(
            next_run=time.monotonic() + 5.0,
            name="test",
            interval=3600,
            meta_dict={},
        )
        q.push(entry)
        assert 4.0 < q.peek_delay() <= 5.0

    def test_ordering(self) -> None:
        q = ScheduleQueue()
        now = time.monotonic()
        q.push(ScheduleEntry(next_run=now + 10, name="later", interval=60, meta_dict={}))
        q.push(ScheduleEntry(next_run=now - 2, name="first", interval=60, meta_dict={}))
        q.push(ScheduleEntry(next_run=now - 1, name="second", interval=60, meta_dict={}))
        assert q.pop_due().name == "first"
        assert q.pop_due().name == "second"


# ── CircuitBreakerState tests ──


class TestCircuitBreakerState:
    def test_record_failure_below_threshold(self) -> None:
        cb = CircuitBreakerState()
        tripped = cb.record_failure(max_failures=5, base_cooldown=300.0, max_cooldown=3600.0)
        assert not tripped
        assert cb.consecutive_failures == 1

    def test_record_failure_trips_at_threshold(self) -> None:
        cb = CircuitBreakerState()
        for _ in range(4):
            cb.record_failure(5, 300.0, 3600.0)
        tripped = cb.record_failure(5, 300.0, 3600.0)
        assert tripped
        assert cb.is_in_cooldown

    def test_record_success_resets(self) -> None:
        cb = CircuitBreakerState()
        for _ in range(5):
            cb.record_failure(5, 300.0, 3600.0)
        assert cb.is_in_cooldown
        cb.record_success(300.0)
        assert cb.consecutive_failures == 0
        assert not cb.is_in_cooldown

    def test_cooldown_doubles(self) -> None:
        cb = CircuitBreakerState(cooldown=100.0)
        cb.record_failure(1, 100.0, 400.0)  # trips immediately
        assert cb.cooldown == 200.0
        cb.consecutive_failures = 0
        cb.in_cooldown_until = 0.0
        cb.record_failure(1, 100.0, 400.0)
        assert cb.cooldown == 400.0  # capped
        cb.consecutive_failures = 0
        cb.in_cooldown_until = 0.0
        cb.record_failure(1, 100.0, 400.0)
        assert cb.cooldown == 400.0  # stays at max

    def test_get_breaker_creates_on_demand(self) -> None:
        q = ScheduleQueue()
        b1 = q.get_breaker("foo")
        b2 = q.get_breaker("foo")
        assert b1 is b2
        assert b1.consecutive_failures == 0


# ── Worker tests ──


class TestWorker:
    @pytest.mark.asyncio
    async def test_worker_fetches_and_saves(self, store: SignalStore) -> None:
        schedule = ScheduleQueue()
        registry = {"dummy": _DummyCollector}
        semaphore = asyncio.Semaphore(5)
        work_queue: asyncio.Queue[ScheduleEntry] = asyncio.Queue()

        entry = ScheduleEntry(
            next_run=0, name="dummy", interval=3600,
            meta_dict={"domain": "markets", "category": "crypto",
                        "interval": 3600, "signal_type": "scalar"},
        )
        await work_queue.put(entry)

        task = asyncio.create_task(
            _worker(0, work_queue, schedule, registry, store, semaphore,
                    fetch_timeout=10.0),
        )
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        result = store.get_data("dummy")
        assert len(result) == 1
        assert len(schedule) == 1  # rescheduled

    @pytest.mark.asyncio
    async def test_worker_handles_failure(self, store: SignalStore) -> None:
        schedule = ScheduleQueue()
        registry = {"broken": _FailCollector}
        semaphore = asyncio.Semaphore(5)
        work_queue: asyncio.Queue[ScheduleEntry] = asyncio.Queue()

        entry = ScheduleEntry(
            next_run=0, name="broken", interval=3600,
            meta_dict={"domain": "markets", "category": "crypto",
                        "interval": 3600, "signal_type": "scalar"},
        )
        await work_queue.put(entry)

        task = asyncio.create_task(
            _worker(0, work_queue, schedule, registry, store, semaphore,
                    fetch_timeout=10.0, max_failures=5),
        )
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        result = store.get_data("broken")
        assert result.empty
        breaker = schedule.get_breaker("broken")
        assert breaker.consecutive_failures == 1
        assert len(schedule) == 1  # rescheduled


# ── Dispatcher tests ──


class TestDispatcher:
    @pytest.mark.asyncio
    async def test_dispatches_due_entries(self) -> None:
        schedule = ScheduleQueue()
        work_queue: asyncio.Queue[ScheduleEntry] = asyncio.Queue()

        schedule.push(ScheduleEntry(
            next_run=time.monotonic() - 1, name="due_now",
            interval=3600, meta_dict={},
        ))

        task = asyncio.create_task(_dispatcher(schedule, work_queue))
        entry = await asyncio.wait_for(work_queue.get(), timeout=2.0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert entry.name == "due_now"

    @pytest.mark.asyncio
    async def test_skips_cooldown_entries(self) -> None:
        schedule = ScheduleQueue()
        work_queue: asyncio.Queue[ScheduleEntry] = asyncio.Queue()

        entry = ScheduleEntry(
            next_run=time.monotonic() - 1, name="cooldown_test",
            interval=3600, meta_dict={},
        )
        schedule.push(entry)

        breaker = schedule.get_breaker("cooldown_test")
        breaker.in_cooldown_until = time.monotonic() + 9999

        task = asyncio.create_task(_dispatcher(schedule, work_queue))
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert work_queue.empty()
        assert len(schedule) == 1  # re-queued with cooldown delay


# ── Integration: run_scheduler ──


class _QuickCollector(BaseCollector):
    meta = CollectorMeta(
        name="quick",
        display_name="Quick",
        update_frequency="hourly",
        api_docs_url="",
        domain="markets",
        category="crypto",
        collect_interval=1,  # 1 second → jitter ~0.1s
    )

    def fetch(self) -> pd.DataFrame:
        return pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})


class TestRunScheduler:
    @pytest.mark.asyncio
    async def test_schedules_and_collects(self, store: SignalStore) -> None:
        collectors = {"quick": _QuickCollector}

        async def _stop_after_collection() -> None:
            for _ in range(50):
                await asyncio.sleep(0.1)
                result = store.get_data("quick")
                if len(result) >= 1:
                    return
            pytest.fail("Collector did not run within timeout")

        stop_task = asyncio.create_task(_stop_after_collection())

        async def _run_with_timeout() -> None:
            task = asyncio.create_task(
                run_scheduler(store, collectors=collectors, n_workers=2, fetch_timeout=5.0),
            )
            await stop_task
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await _run_with_timeout()

        result = store.get_data("quick")
        assert len(result) == 1
