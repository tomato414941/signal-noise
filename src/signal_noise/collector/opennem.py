"""OpenNEM Australia National Electricity Market collectors.

Tracks power generation by fuel type, demand, and price for the
Australian NEM. Renewable share is a direct measure of energy
transition velocity in the Asia-Pacific region.
"""
from __future__ import annotations

import time

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://data.opennem.org.au/v3/stats/au/NEM/power/7d.json"

_cache: dict | None = None
_cache_ts: float = 0.0


def _fetch_nem(timeout: int = 30) -> dict:
    global _cache, _cache_ts
    now = time.monotonic()
    if _cache is not None and (now - _cache_ts) < 600:
        return _cache
    resp = requests.get(_API_URL, timeout=timeout)
    resp.raise_for_status()
    _cache = resp.json()
    _cache_ts = now
    return _cache


def _latest_power(data: dict, fuel_tech: str) -> float | None:
    for ds in data.get("data", []):
        if ds.get("fuel_tech") == fuel_tech and ds.get("type") == "power":
            values = ds.get("history", {}).get("data", [])
            for v in reversed(values):
                if v is not None:
                    return float(v)
    return None


def _make_opennem_collector(
    name: str, display_name: str, fuel_tech: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://opennem.org.au/",
            domain="economy",
            category="energy",
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_nem(timeout=self.config.request_timeout)
            val = _latest_power(data, fuel_tech)
            if val is None:
                raise RuntimeError(f"No OpenNEM data for {fuel_tech}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": val}])

    _Collector.__name__ = f"OpenNEM_{name}"
    _Collector.__qualname__ = f"OpenNEM_{name}"
    return _Collector


class OpenNEMDemandCollector(BaseCollector):
    meta = CollectorMeta(
        name="opennem_demand",
        display_name="Australia NEM Demand (MW)",
        update_frequency="hourly",
        api_docs_url="https://opennem.org.au/",
        domain="economy",
        category="energy",
    )

    def fetch(self) -> pd.DataFrame:
        data = _fetch_nem(timeout=self.config.request_timeout)
        for ds in data.get("data", []):
            if ds.get("id") == "au.nem.demand" or ds.get("fuel_tech") == "au.nem.demand":
                values = ds.get("history", {}).get("data", [])
                for v in reversed(values):
                    if v is not None:
                        now = pd.Timestamp.now(tz="UTC").normalize()
                        return pd.DataFrame([{"date": now, "value": float(v)}])
        raise RuntimeError("No OpenNEM demand data")


class OpenNEMPriceCollector(BaseCollector):
    meta = CollectorMeta(
        name="opennem_price",
        display_name="Australia NEM Spot Price (AUD/MWh)",
        update_frequency="hourly",
        api_docs_url="https://opennem.org.au/",
        domain="economy",
        category="energy",
    )

    def fetch(self) -> pd.DataFrame:
        data = _fetch_nem(timeout=self.config.request_timeout)
        for ds in data.get("data", []):
            if ds.get("type") == "price":
                values = ds.get("history", {}).get("data", [])
                for v in reversed(values):
                    if v is not None:
                        now = pd.Timestamp.now(tz="UTC").normalize()
                        return pd.DataFrame([{"date": now, "value": float(v)}])
        raise RuntimeError("No OpenNEM price data")


_FUEL_SIGNALS: list[tuple[str, str, str]] = [
    ("opennem_solar_utility", "Australia NEM Solar Utility (MW)", "solar_utility"),
    ("opennem_solar_rooftop", "Australia NEM Solar Rooftop (MW)", "solar_rooftop"),
    ("opennem_wind", "Australia NEM Wind (MW)", "wind"),
    ("opennem_coal_black", "Australia NEM Black Coal (MW)", "coal_black"),
    ("opennem_coal_brown", "Australia NEM Brown Coal (MW)", "coal_brown"),
    ("opennem_gas_ccgt", "Australia NEM Gas CCGT (MW)", "gas_ccgt"),
    ("opennem_hydro", "Australia NEM Hydro (MW)", "hydro"),
    ("opennem_battery_discharge", "Australia NEM Battery Discharge (MW)", "battery_discharging"),
]


def get_opennem_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_opennem_collector(name, display, ft)
        for name, display, ft in _FUEL_SIGNALS
    }
