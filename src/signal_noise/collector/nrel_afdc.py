"""NREL Alternative Fuels Data Center (AFDC) collectors.

Tracks EV charging station counts in the US using the DEMO_KEY.
https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://developer.nrel.gov/api/alt-fuel-stations/v1.json"
_API_KEY = "DEMO_KEY"


class NRELChargingStationsCollector(BaseCollector):
    """Total US EV charging stations count."""

    meta = CollectorMeta(
        name="nrel_ev_stations_us",
        display_name="US EV Charging Stations (Total)",
        update_frequency="daily",
        api_docs_url="https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/",
        domain="technology",
        category="ev",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _BASE_URL,
            params={
                "api_key": _API_KEY,
                "fuel_type": "ELEC",
                "country": "US",
                "status": "E",
                "limit": "1",
            },
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        total = resp.json().get("total_results", 0)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])


# State-level collectors for top EV states
_EV_STATES = [
    ("CA", "nrel_ev_stations_ca", "EV Charging Stations: California"),
    ("TX", "nrel_ev_stations_tx", "EV Charging Stations: Texas"),
    ("NY", "nrel_ev_stations_ny", "EV Charging Stations: New York"),
    ("FL", "nrel_ev_stations_fl", "EV Charging Stations: Florida"),
    ("WA", "nrel_ev_stations_wa", "EV Charging Stations: Washington"),
]


def _make_state_collector(
    state: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/",
            domain="technology",
            category="ev",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                _BASE_URL,
                params={
                    "api_key": _API_KEY,
                    "fuel_type": "ELEC",
                    "country": "US",
                    "state": state,
                    "status": "E",
                    "limit": "1",
                },
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            total = resp.json().get("total_results", 0)
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(total)}])

    _Collector.__name__ = f"NREL_{name}"
    _Collector.__qualname__ = f"NREL_{name}"
    return _Collector


def get_nrel_afdc_collectors() -> dict[str, type[BaseCollector]]:
    result: dict[str, type[BaseCollector]] = {
        "nrel_ev_stations_us": NRELChargingStationsCollector,
    }
    for state, name, display in _EV_STATES:
        result[name] = _make_state_collector(state, name, display)
    return result
