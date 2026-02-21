from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector

log = logging.getLogger(__name__)


def _get_collectors() -> dict[str, type[BaseCollector]]:
    from signal_noise.collector.btc_dominance import BtcDominanceCollector
    from signal_noise.collector.btc_ohlcv import BtcOhlcvCollector
    from signal_noise.collector.ccxt_generic import get_ccxt_collectors
    from signal_noise.collector.climate_indices import (
        ArcticOscillationCollector,
        EnsoCollector,
        NaoCollector,
    )
    from signal_noise.collector.earthquake import EarthquakeCountCollector
    from signal_noise.collector.electricity import ElectricityCollector
    from signal_noise.collector.eth_btc import EthBtcCollector
    from signal_noise.collector.fear_greed import FearGreedCollector
    from signal_noise.collector.geomagnetic import GeomagneticCollector
    from signal_noise.collector.google_trends import GoogleTrendsCollector
    from signal_noise.collector.hashrate import HashrateCollector
    from signal_noise.collector.moon import MoonPhaseCollector
    from signal_noise.collector.solar import SolarXrayCollector
    from signal_noise.collector.sunspot import SunspotCollector
    from signal_noise.collector.temporal import DayOfWeekCollector, HourOfDayCollector
    from signal_noise.collector.weather import NYWeatherCollector
    from signal_noise.collector.wikipedia import WikipediaBtcCollector
    from signal_noise.collector.yahoo_finance import DXYCollector, GoldCollector, SP500Collector
    from signal_noise.collector.yahoo_generic import get_yahoo_collectors

    collectors: dict[str, type[BaseCollector]] = {
        "day_of_week": DayOfWeekCollector,
        "hour_of_day": HourOfDayCollector,
        "btc_ohlcv": BtcOhlcvCollector,
        "fear_greed": FearGreedCollector,
        "geomagnetic": GeomagneticCollector,
        "hashrate": HashrateCollector,
        "dxy": DXYCollector,
        "gold": GoldCollector,
        "sp500": SP500Collector,
        "eth_btc": EthBtcCollector,
        "btc_dominance": BtcDominanceCollector,
        "wikipedia_btc": WikipediaBtcCollector,
        "google_trends": GoogleTrendsCollector,
        "ny_weather": NYWeatherCollector,
        "electricity": ElectricityCollector,
        # Nature / physical phenomena
        "moon_phase": MoonPhaseCollector,
        "earthquake_count": EarthquakeCountCollector,
        "solar_xray": SolarXrayCollector,
        "sunspot": SunspotCollector,
        "enso": EnsoCollector,
        "arctic_oscillation": ArcticOscillationCollector,
        "nao": NaoCollector,
    }
    collectors.update(get_yahoo_collectors())
    collectors.update(get_ccxt_collectors())
    return collectors


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
            log.warning("Failed to collect %s: %s", name, e)
    return results
