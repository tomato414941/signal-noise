from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from signal_noise.collector.base import BaseCollector
from signal_noise.collector.streaming import StreamingCollector
from signal_noise.store.event_bus import EventBus, SignalEvent
from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


def _compute_jitter(name: str, interval: int) -> float:
    """Deterministic jitter from collector name. Same name = same offset.

    Spread is proportional to the collection interval (10% of interval,
    capped at half the interval) to avoid thundering herd at startup.
    """
    h = hashlib.md5(name.encode(), usedforsecurity=False).digest()
    frac = int.from_bytes(h[:4], "little") / (2**32)
    max_jitter = min(interval * 0.1, interval / 2)
    return frac * max_jitter


async def run_collector_loop(
    collector: BaseCollector,
    store: SignalStore,
    interval: int,
    *,
    jitter: float = 0.0,
    max_failures: int = 5,
    base_cooldown: float = 300.0,
    max_cooldown: float = 3600.0,
    fetch_semaphore: asyncio.Semaphore | None = None,
    fetch_timeout: float = 300.0,
    event_bus: EventBus | None = None,
) -> None:
    """Single collector loop: fetch -> save -> sleep -> repeat.

    Circuit breaker with half-open recovery:
    - After max_failures consecutive failures, enter cooldown.
    - Wait with exponential backoff, then attempt a half-open retry.
    - On success: reset and resume normal operation.
    - On failure: double cooldown (capped at max_cooldown) and retry.
    """
    if jitter > 0:
        log.debug("Collector %s starting in %.1fs (jitter)", collector.meta.name, jitter)
        await asyncio.sleep(jitter)

    async def _fetch() -> pd.DataFrame:
        if fetch_semaphore:
            async with fetch_semaphore:
                return await asyncio.to_thread(collector.fetch)
        return await asyncio.to_thread(collector.fetch)

    failures = 0
    cooldown = base_cooldown
    while True:
        try:
            df = await asyncio.wait_for(_fetch(), timeout=fetch_timeout)
            anomalies = store.check_anomalies(collector.meta.name, df)
            if anomalies:
                log.warning(
                    "Anomalies in %s: %d outliers (z>4.0): %s",
                    collector.meta.name, len(anomalies),
                    ", ".join(f"{a['timestamp']}={a['value']}(z={a['z_score']})" for a in anomalies[:3]),
                )
            rows = store.save_collection_result(
                collector.meta.name, df,
                collector.meta.domain, collector.meta.category,
                collector.meta.interval, collector.meta.signal_type,
            )
            failures = 0
            cooldown = base_cooldown
            log.info("Collected %s: %d rows", collector.meta.name, rows)
            if event_bus is not None:
                await _publish_events(event_bus, collector.meta.name, df, anomalies)
        except Exception as exc:
            failures += 1
            log.exception("Collector %s failed (%d/%d)", collector.meta.name, failures, max_failures)
            store.save_collection_failure(collector.meta.name, str(exc)[:200])
            if failures >= max_failures:
                log.warning(
                    "Circuit breaker: %s entering half-open cooldown (%.0fs) after %d failures",
                    collector.meta.name, cooldown, max_failures,
                )
                store.log_collection_event(
                    collector.meta.name, "circuit_break_cooldown",
                    f"{max_failures} failures, cooldown {cooldown:.0f}s",
                )
                if event_bus is not None:
                    await event_bus.publish(SignalEvent(
                        name=collector.meta.name,
                        timestamp="",
                        value=None,
                        event_type="circuit_break",
                        detail=f"{max_failures} failures, cooldown {cooldown:.0f}s",
                    ))
                await asyncio.sleep(cooldown)
                cooldown = min(cooldown * 2, max_cooldown)
                # Half-open: try once more
                try:
                    df = await asyncio.wait_for(_fetch(), timeout=fetch_timeout)
                    store.save_collection_result(
                        collector.meta.name, df,
                        collector.meta.domain, collector.meta.category,
                        collector.meta.interval, collector.meta.signal_type,
                        event="half_open_recovered",
                    )
                    failures = 0
                    cooldown = base_cooldown
                    log.info("Half-open recovery: %s succeeded (%d rows)", collector.meta.name, len(df))
                except Exception as retry_exc:
                    log.warning(
                        "Half-open retry failed for %s: %s", collector.meta.name, retry_exc,
                    )
                    store.log_collection_event(
                        collector.meta.name, "half_open_failed",
                        str(retry_exc)[:200],
                    )
                    continue
        await asyncio.sleep(interval)


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
                # Multi-signal: group by name and save each separately
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


async def run_scheduler(
    store: SignalStore,
    collectors: dict[str, type[BaseCollector]] | None = None,
    *,
    max_concurrent_fetches: int = 20,
    event_bus: EventBus | None = None,
) -> None:
    """Start all collector loops as concurrent tasks.

    max_concurrent_fetches limits how many collectors can call their
    provider APIs simultaneously, preventing burst traffic.
    Streaming collectors are launched via run_streaming_collector.
    """
    from signal_noise.collector import COLLECTORS

    targets = collectors or COLLECTORS
    semaphore = asyncio.Semaphore(max_concurrent_fetches)
    log.info(
        "Starting scheduler: %d collectors, max %d concurrent fetches",
        len(targets), max_concurrent_fetches,
    )

    tasks = []
    n_streaming = 0
    for name in targets:
        cls = targets[name]
        if cls is None:
            log.warning("Skipping %s: failed to load", name)
            continue
        collector = cls()
        interval = collector.meta.interval
        store.save_meta(
            name, collector.meta.domain, collector.meta.category,
            interval, collector.meta.signal_type,
        )
        if isinstance(collector, StreamingCollector):
            task = asyncio.create_task(
                run_streaming_collector(collector, store, event_bus=event_bus),
                name=f"stream:{name}",
            )
            n_streaming += 1
            log.info("Scheduled stream %s", name)
        else:
            j = _compute_jitter(name, interval)
            task = asyncio.create_task(
                run_collector_loop(
                    collector, store, interval,
                    jitter=j, fetch_semaphore=semaphore,
                    event_bus=event_bus,
                ),
                name=f"collector:{name}",
            )
            log.info("Scheduled %s (every %ds, jitter %.1fs)", name, interval, j)
        tasks.append(task)
    log.info(
        "Pre-registered %d collectors (%d polling, %d streaming)",
        len(targets), len(targets) - n_streaming, n_streaming,
    )

    # Daily rollup task for realtime signals
    if n_streaming > 0:
        tasks.append(asyncio.create_task(
            _daily_rollup(store), name="daily-rollup",
        ))

    await asyncio.gather(*tasks, return_exceptions=True)


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
