from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone

import pandas as pd

from signal_noise.collector._registry import CollectorRegistry, ensure_registry, meta_to_dict
from signal_noise.collector.base import BaseCollector
from signal_noise.collector.streaming import StreamingCollector
from signal_noise.scheduler.state import CircuitBreakerState, ScheduleEntry, ScheduleQueue
from signal_noise.store.event_bus import EventBus, SignalEvent
from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


def _compute_jitter(name: str, interval: int) -> float:
    """Deterministic jitter from collector name. Same name = same offset.

    Spread is proportional to the collection interval (10% of interval),
    capped so that even annual collectors run within a few hours of startup.
    """
    h = hashlib.md5(name.encode(), usedforsecurity=False).digest()
    frac = int.from_bytes(h[:4], "little") / (2**32)
    max_jitter = min(interval * 0.1, interval / 2, 7200.0)  # cap at 2h
    return frac * max_jitter


# ── Streaming (unchanged) ──


async def run_streaming_collector(
    collector: StreamingCollector,
    store: SignalStore,
    *,
    event_bus: EventBus | None = None,
) -> None:
    """Run a streaming collector as a long-lived task.

    Supports multi-signal DataFrames (with a ``name`` column) and
    realtime store routing (when ``collector.use_realtime_store`` is True).
    """
    use_rt = getattr(collector, "use_realtime_store", False)
    log.info("Starting stream: %s (realtime=%s)", collector.meta.name, use_rt)
    async for df in collector.connect_with_retry():
        if df.empty:
            continue
        try:
            if "name" in df.columns:
                for sig_name, group in df.groupby("name"):
                    _save_stream_data(
                        store, str(sig_name), group, collector, use_rt,
                    )
                    if event_bus is not None:
                        await _publish_events(event_bus, str(sig_name), group, [])
            else:
                anomalies = store.check_anomalies(collector.meta.name, df)
                _save_stream_data(
                    store, collector.meta.name, df, collector, use_rt,
                )
                if event_bus is not None:
                    await _publish_events(
                        event_bus, collector.meta.name, df, anomalies,
                    )
        except Exception:
            log.exception("Failed to save stream data for %s", collector.meta.name)


def _save_stream_data(
    store: SignalStore,
    name: str,
    df: pd.DataFrame,
    collector: StreamingCollector,
    use_realtime: bool,
) -> int:
    """Route stream data to the appropriate store table."""
    if use_realtime:
        rows = store.save_realtime_collection_result(
            name, df,
            collector.meta.domain, collector.meta.category,
            collector.meta.interval, collector.meta.signal_type,
        )
    else:
        rows = store.save_collection_result(
            name, df,
            collector.meta.domain, collector.meta.category,
            collector.meta.interval, collector.meta.signal_type,
        )
    log.info("Stream %s: saved %d rows", name, rows)
    return rows


def _sync_streaming_meta(
    store: SignalStore,
    collector: StreamingCollector,
) -> None:
    names = collector.registered_meta_names()
    if collector.meta.name not in names:
        store.delete_meta(collector.meta.name)
    for name in names:
        store.save_meta(
            name, collector.meta.domain, collector.meta.category,
            collector.meta.interval, collector.meta.signal_type,
        )


async def _publish_events(
    bus: EventBus, name: str, df: pd.DataFrame, anomalies: list[dict],
) -> None:
    """Publish update and anomaly events for a successful collection."""
    if not df.empty:
        latest = df.iloc[-1]
        ts_col = "timestamp" if "timestamp" in df.columns else "date"
        val_col = "value" if "value" in df.columns else df.columns[-1]
        ts = str(latest[ts_col]) if ts_col in df.columns else ""
        val = float(latest[val_col]) if val_col in df.columns and pd.notna(latest[val_col]) else None
        await bus.publish(SignalEvent(
            name=name, timestamp=ts, value=val, event_type="update",
        ))
    for a in anomalies:
        await bus.publish(SignalEvent(
            name=name,
            timestamp=str(a.get("timestamp", "")),
            value=a.get("value"),
            event_type="anomaly",
            detail=f"z={a.get('z_score', 0):.1f}",
        ))


# ── Priority queue scheduler ──


def _normalize_entry_meta(meta: dict[str, object]) -> dict[str, object]:
    return {
        "domain": str(meta.get("domain", "")),
        "category": str(meta.get("category", "")),
        "interval": int(meta.get("interval", 86400)),
        "signal_type": str(meta.get("signal_type", "scalar")),
    }


def _resolve_entry_meta(
    registry: CollectorRegistry,
    name: str,
) -> dict[str, object] | None:
    meta = registry.get_meta(name)
    if meta is not None:
        return _normalize_entry_meta(meta)

    cls = registry.load(name)
    if cls is None:
        return None
    return _normalize_entry_meta(meta_to_dict(cls.meta))


def _enqueue_polling_collectors(
    store: SignalStore,
    registry: CollectorRegistry,
    *,
    now: float,
) -> tuple[ScheduleQueue, list[str]]:
    schedule = ScheduleQueue()
    streaming_names: list[str] = []

    for name in registry.keys():
        if registry.is_streaming(name):
            streaming_names.append(name)
            continue

        meta = _resolve_entry_meta(registry, name)
        if meta is None:
            log.warning("Skipping %s: failed to resolve collector metadata", name)
            continue

        store.save_meta(
            name,
            str(meta["domain"]),
            str(meta["category"]),
            int(meta["interval"]),
            str(meta["signal_type"]),
        )
        schedule.push(ScheduleEntry(
            next_run=now + _compute_jitter(name, int(meta["interval"])),
            name=name,
            interval=int(meta["interval"]),
            meta_dict=meta,
        ))

    return schedule, streaming_names


def _launch_streaming_tasks(
    store: SignalStore,
    registry: CollectorRegistry,
    streaming_names: list[str],
    *,
    event_bus: EventBus | None = None,
) -> list[asyncio.Task]:
    tasks: list[asyncio.Task] = []

    for name in streaming_names:
        cls = registry.load(name)
        if cls is None:
            log.warning("Skipping stream %s: failed to load", name)
            continue

        collector = cls()
        if not isinstance(collector, StreamingCollector):
            log.warning("Skipping stream %s: collector is not streaming", name)
            continue

        _sync_streaming_meta(store, collector)
        tasks.append(asyncio.create_task(
            run_streaming_collector(collector, store, event_bus=event_bus),
            name=f"stream:{name}",
        ))
        log.info("Launched stream: %s", name)

    if streaming_names:
        tasks.append(asyncio.create_task(
            _daily_rollup(store), name="daily-rollup",
        ))

    return tasks


async def run_scheduler(
    store: SignalStore,
    collectors: CollectorRegistry | Mapping[str, type[BaseCollector]] | None = None,
    *,
    max_concurrent_fetches: int = 10,
    event_bus: EventBus | None = None,
    n_workers: int = 4,
    fetch_timeout: float = 90.0,
    max_failures: int = 5,
    base_cooldown: float = 300.0,
    max_cooldown: float = 3600.0,
) -> None:
    """Central scheduler: priority queue + worker pool.

    Modules are loaded on first execution. Collector instances are
    created per-fetch and released immediately. Circuit breaker state
    lives in the ScheduleQueue, not in coroutine locals.
    """
    from signal_noise.collector import COLLECTORS

    targets = ensure_registry(collectors or COLLECTORS)
    semaphore = asyncio.Semaphore(max_concurrent_fetches)
    schedule, streaming_names = _enqueue_polling_collectors(
        store,
        targets,
        now=time.monotonic(),
    )

    log.info(
        "Scheduler: %d polling queued, %d streaming, %d workers",
        len(schedule), len(streaming_names), n_workers,
    )

    tasks = _launch_streaming_tasks(
        store,
        targets,
        streaming_names,
        event_bus=event_bus,
    )

    # ── Phase 3: dispatcher + workers ──
    work_queue: asyncio.Queue[ScheduleEntry] = asyncio.Queue(maxsize=n_workers * 2)

    tasks.append(asyncio.create_task(
        _dispatcher(schedule, work_queue), name="dispatcher",
    ))

    for i in range(n_workers):
        tasks.append(asyncio.create_task(
            _worker(
                i, work_queue, schedule, targets, store, semaphore,
                event_bus=event_bus,
                fetch_timeout=fetch_timeout,
                max_failures=max_failures,
                base_cooldown=base_cooldown,
                max_cooldown=max_cooldown,
            ),
            name=f"worker-{i}",
        ))

    await asyncio.gather(*tasks, return_exceptions=True)


async def _dispatcher(
    schedule: ScheduleQueue,
    work_queue: asyncio.Queue[ScheduleEntry],
) -> None:
    """Sleep until next deadline, then push due entries to workers."""
    while True:
        delay = schedule.peek_delay()
        if delay > 0:
            await asyncio.sleep(min(delay, 60.0))
            continue

        entry = schedule.pop_due()
        if entry is None:
            await asyncio.sleep(1.0)
            continue

        breaker = schedule.get_breaker(entry.name)
        if breaker.is_in_cooldown:
            remaining = breaker.in_cooldown_until - time.monotonic()
            schedule.reschedule_after(entry, max(remaining, 1.0))
            continue

        await work_queue.put(entry)


async def _worker(
    worker_id: int,
    work_queue: asyncio.Queue[ScheduleEntry],
    schedule: ScheduleQueue,
    registry: CollectorRegistry | Mapping[str, type[BaseCollector]],
    store: SignalStore,
    semaphore: asyncio.Semaphore,
    *,
    event_bus: EventBus | None = None,
    fetch_timeout: float = 90.0,
    max_failures: int = 5,
    base_cooldown: float = 300.0,
    max_cooldown: float = 3600.0,
) -> None:
    """Pick entry from work_queue, load module, fetch, save, release."""
    resolved = ensure_registry(registry)
    while True:
        entry = await work_queue.get()
        try:
            await _execute_scheduled_collection(
                worker_id,
                entry,
                schedule,
                resolved,
                store,
                semaphore,
                event_bus=event_bus,
                fetch_timeout=fetch_timeout,
                max_failures=max_failures,
                base_cooldown=base_cooldown,
                max_cooldown=max_cooldown,
            )
        finally:
            work_queue.task_done()


async def _execute_scheduled_collection(
    worker_id: int,
    entry: ScheduleEntry,
    schedule: ScheduleQueue,
    registry: CollectorRegistry,
    store: SignalStore,
    semaphore: asyncio.Semaphore,
    *,
    event_bus: EventBus | None = None,
    fetch_timeout: float = 90.0,
    max_failures: int = 5,
    base_cooldown: float = 300.0,
    max_cooldown: float = 3600.0,
) -> None:
    breaker = schedule.get_breaker(entry.name)
    cls = registry.load(entry.name)
    if cls is None:
        log.warning("Worker-%d: cannot load %s", worker_id, entry.name)
        schedule.reschedule(entry)
        return

    try:
        collector = cls()
        effective_fetch_timeout = max(fetch_timeout, collector.retry_timeout_budget())
        async with semaphore:
            df = await asyncio.wait_for(
                asyncio.to_thread(collector.fetch_with_retry),
                timeout=effective_fetch_timeout,
            )
        anomalies = store.check_anomalies(entry.name, df)
        if anomalies:
            log.warning(
                "Anomalies in %s: %d outliers (z>4.0): %s",
                entry.name, len(anomalies),
                ", ".join(
                    f"{a['timestamp']}={a['value']}(z={a['z_score']})"
                    for a in anomalies[:3]
                ),
            )

        rows = store.save_collection_result(
            entry.name, df,
            entry.meta_dict.get("domain", ""),
            entry.meta_dict.get("category", ""),
            entry.meta_dict.get("interval", 86400),
            entry.meta_dict.get("signal_type", "scalar"),
        )
        breaker.record_success(base_cooldown)
        log.info("Worker-%d: collected %s: %d rows", worker_id, entry.name, rows)

        if event_bus is not None:
            await _publish_events(event_bus, entry.name, df, anomalies)
    except Exception as exc:
        await _handle_worker_failure(
            worker_id,
            entry,
            schedule,
            breaker,
            store,
            exc,
            event_bus=event_bus,
            max_failures=max_failures,
            base_cooldown=base_cooldown,
            max_cooldown=max_cooldown,
        )
        return

    schedule.reschedule(entry)


async def _handle_worker_failure(
    worker_id: int,
    entry: ScheduleEntry,
    schedule: ScheduleQueue,
    breaker: CircuitBreakerState,
    store: SignalStore,
    exc: Exception,
    *,
    event_bus: EventBus | None = None,
    max_failures: int = 5,
    base_cooldown: float = 300.0,
    max_cooldown: float = 3600.0,
) -> None:
    log.exception("Worker-%d: %s failed", worker_id, entry.name)
    detail = str(exc) or type(exc).__name__
    store.save_collection_failure(entry.name, detail[:200])

    tripped = breaker.record_failure(max_failures, base_cooldown, max_cooldown)
    if not tripped:
        schedule.reschedule(entry)
        return

    log.warning(
        "Circuit breaker: %s cooldown (%.0fs) after %d failures",
        entry.name, breaker.cooldown / 2, max_failures,
    )
    store.log_collection_event(
        entry.name, "circuit_break_cooldown",
        f"{max_failures} failures, cooldown {breaker.cooldown / 2:.0f}s",
    )
    if event_bus is not None:
        await event_bus.publish(SignalEvent(
            name=entry.name,
            timestamp="",
            value=None,
            event_type="circuit_break",
            detail=f"{max_failures} failures",
        ))
    remaining = breaker.in_cooldown_until - time.monotonic()
    schedule.reschedule_after(entry, max(remaining, 1.0))


async def _daily_rollup(store: SignalStore) -> None:
    """Run daily at ~00:05 UTC: rollup realtime signals + purge old data."""
    while True:
        now = datetime.now(timezone.utc)
        next_run = now.replace(hour=0, minute=5, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait_secs = (next_run - now).total_seconds()
        log.info("Next realtime rollup in %.0f seconds", wait_secs)
        await asyncio.sleep(wait_secs)
        try:
            metas = store.query_meta(interval=60)
            rolled = 0
            for m in metas:
                rolled += store.rollup_daily(m["name"])
            purged = store.purge_realtime(days=30)
            log.info("Daily rollup: %d rows rolled, %d purged", rolled, purged)
        except Exception:
            log.exception("Daily rollup failed")
