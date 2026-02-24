from __future__ import annotations

import asyncio
import logging

from signal_noise.collector.base import BaseCollector
from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


async def run_collector_loop(
    collector: BaseCollector,
    store: SignalStore,
    interval: int,
    *,
    max_failures: int = 5,
) -> None:
    """Single collector loop: fetch -> save -> sleep -> repeat.

    Circuit breaker: stops after max_failures consecutive failures.
    """
    failures = 0
    while True:
        try:
            df = await asyncio.to_thread(collector.fetch)
            anomalies = store.check_anomalies(collector.meta.name, df)
            if anomalies:
                log.warning(
                    "Anomalies in %s: %d outliers (z>4.0): %s",
                    collector.meta.name, len(anomalies),
                    ", ".join(f"{a['timestamp']}={a['value']}(z={a['z_score']})" for a in anomalies[:3]),
                )
            store.save(collector.meta.name, df)
            store.save_meta(
                collector.meta.name,
                collector.meta.domain,
                collector.meta.category,
                collector.meta.interval,
                collector.meta.signal_type,
            )
            store.reset_failures(collector.meta.name)
            store.log_event(collector.meta.name, "collected", rows=len(df))
            failures = 0
            log.info("Collected %s: %d rows", collector.meta.name, len(df))
        except Exception as exc:
            failures += 1
            log.exception("Collector %s failed (%d/%d)", collector.meta.name, failures, max_failures)
            store.increment_failures(collector.meta.name)
            store.log_event(collector.meta.name, "failed", detail=str(exc)[:200])
            if failures >= max_failures:
                log.error(
                    "Circuit breaker: %s stopped after %d consecutive failures",
                    collector.meta.name, max_failures,
                )
                store.log_event(collector.meta.name, "circuit_break", detail=f"{max_failures} consecutive failures")
                return
        await asyncio.sleep(interval)


async def run_scheduler(
    store: SignalStore,
    collectors: dict[str, type[BaseCollector]] | None = None,
) -> None:
    """Start all collector loops as concurrent tasks."""
    from signal_noise.collector import COLLECTORS

    targets = collectors or COLLECTORS
    tasks = []
    for name, cls in targets.items():
        collector = cls()
        interval = collector.meta.interval
        task = asyncio.create_task(
            run_collector_loop(collector, store, interval),
            name=f"collector:{name}",
        )
        tasks.append(task)
        log.info("Scheduled %s (every %ds)", name, interval)

    await asyncio.gather(*tasks)
