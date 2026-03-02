from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class FAADelayCountCollector(BaseCollector):
    """Current count of US airports with active delays/ground stops (FAA NAS Status).

    High delay count = severe weather or ATC system stress.
    Correlates with airline stock intraday moves and insurance events.
    """

    meta = CollectorMeta(
        name="faa_delay_count",
        display_name="FAA Airports with Active Delays",
        update_frequency="hourly",
        api_docs_url="https://nasstatus.faa.gov/api/airport-status-information",
        domain="technology",
        category="aviation",
    )

    URL = "https://nasstatus.faa.gov/api/airport-status-information"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        delay_count = 0
        ground_stop_count = 0

        if isinstance(data, list):
            for airport in data:
                if airport.get("delay", False):
                    delay_count += 1
                if airport.get("groundStop", {}).get("isActive", False):
                    ground_stop_count += 1
        elif isinstance(data, dict):
            for key, airport in data.items():
                if isinstance(airport, dict):
                    if airport.get("delay", False):
                        delay_count += 1

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame([{
            "timestamp": ts,
            "value": float(delay_count),
        }])


class FAAGroundStopCollector(BaseCollector):
    """Current count of FAA Ground Stop programs.

    Ground Stops completely halt departures to an airport.
    Multiple simultaneous ground stops = severe disruption event.
    """

    meta = CollectorMeta(
        name="faa_ground_stop_count",
        display_name="FAA Active Ground Stops",
        update_frequency="hourly",
        api_docs_url="https://nasstatus.faa.gov/api/airport-status-information",
        domain="technology",
        category="aviation",
    )

    URL = "https://nasstatus.faa.gov/api/airport-status-information"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        count = 0
        if isinstance(data, list):
            for airport in data:
                gs = airport.get("groundStop", {})
                if isinstance(gs, dict) and gs.get("isActive", False):
                    count += 1
                elif isinstance(gs, bool) and gs:
                    count += 1

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame([{"timestamp": ts, "value": float(count)}])
