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
) -> None:
    """Single collector loop: fetch -> save -> sleep -> repeat."""
    while True:
        try:
            df = await asyncio.to_thread(collector.fetch)
            store.save(collector.meta.name, df)
            store.save_meta(
                collector.meta.name,
                collector.meta.domain,
                collector.meta.category,
                collector.meta.interval,
                collector.meta.signal_type,
            )
            store.reset_failures(collector.meta.name)
            log.info("Collected %s: %d rows", collector.meta.name, len(df))
        except Exception:
            log.exception("Collector %s failed", collector.meta.name)
            store.increment_failures(collector.meta.name)
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
