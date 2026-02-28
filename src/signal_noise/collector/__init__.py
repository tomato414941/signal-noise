from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


from signal_noise.collector._loader import LazyCollectors  # noqa: E402

COLLECTORS = LazyCollectors()



def collect_all(
    collectors: list[str] | None = None,
    store: "SignalStore | None" = None,
) -> dict[str, pd.DataFrame]:
    targets = collectors or list(COLLECTORS.keys())
    results: dict[str, pd.DataFrame] = {}
    for name in targets:
        cls = COLLECTORS.get(name)
        if not cls:
            log.warning("Unknown collector: %s", name)
            continue
        try:
            collector = cls()
            results[name] = collector.collect(store=store)
        except Exception as e:
            log.warning("Failed to collect %s: %s", name, e)
    return results
