from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


def _build_registry():  # noqa: E302
    from signal_noise.collector._manifest import build_manifest, load_manifest
    from signal_noise.collector._lazy import LazyCollectorRegistry

    manifest = load_manifest()
    if manifest is None:
        manifest = build_manifest()
    return LazyCollectorRegistry(manifest["collectors"])


COLLECTORS = _build_registry()



def collect_all(
    collectors: list[str] | None = None,
    store: "SignalStore | None" = None,
) -> dict[str, pd.DataFrame]:
    targets = collectors or list(COLLECTORS.keys())
    results: dict[str, pd.DataFrame] = {}
    for name in targets:
        cls = COLLECTORS.load(name)
        if not cls:
            log.warning("Unknown collector: %s", name)
            continue
        try:
            collector = cls()
            results[name] = collector.collect(store=store)
        except Exception as e:
            log.warning("Failed to collect %s: %s", name, e)
    return results
