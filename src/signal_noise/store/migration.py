from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector
    from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


def migrate_parquet_to_sqlite(
    raw_dir: Path,
    store: SignalStore,
    collectors: dict[str, type[BaseCollector]],
) -> int:
    """Import existing Parquet files into SQLite store.

    Returns the number of signals migrated.
    """
    if not raw_dir.exists():
        log.info("No raw directory found at %s, skipping migration", raw_dir)
        return 0

    # Build name -> collector class lookup
    name_to_cls = {name: cls for name, cls in collectors.items()}

    migrated = 0
    for parquet_path in sorted(raw_dir.glob("*.parquet")):
        name = parquet_path.stem
        try:
            df = pd.read_parquet(parquet_path)
            if df.empty:
                continue
            store.save(name, df)

            # Register metadata if collector is known
            cls = name_to_cls.get(name)
            if cls:
                meta = cls.meta  # type: ignore[attr-defined]
                store.save_meta(name, meta.domain, meta.category, meta.interval)
            else:
                store.save_meta(name, "", "", 86400)

            migrated += 1
            log.info("Migrated %s: %d rows", name, len(df))
        except Exception:
            log.exception("Failed to migrate %s", name)

    return migrated
