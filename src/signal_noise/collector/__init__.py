from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector

log = logging.getLogger(__name__)


def _get_collectors() -> dict[str, type[BaseCollector]]:
    from signal_noise.collector.btc_ohlcv import BtcOhlcvCollector
    from signal_noise.collector.temporal import DayOfWeekCollector, HourOfDayCollector

    return {
        "btc_ohlcv": BtcOhlcvCollector,
        "day_of_week": DayOfWeekCollector,
        "hour_of_day": HourOfDayCollector,
    }


COLLECTORS = _get_collectors()


def collect_all(sources: list[str] | None = None) -> dict[str, pd.DataFrame]:
    targets = sources or list(COLLECTORS.keys())
    results: dict[str, pd.DataFrame] = {}
    for name in targets:
        cls = COLLECTORS.get(name)
        if not cls:
            log.warning("Unknown source: %s", name)
            continue
        try:
            collector = cls()
            results[name] = collector.collect()
        except Exception as e:
            log.error("Failed to collect %s: %s", name, e)
    return results
