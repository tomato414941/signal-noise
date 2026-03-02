from __future__ import annotations

import xml.etree.ElementTree as ET

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_URL = "https://nasstatus.faa.gov/api/airport-status-information"


def _parse_faa_xml(text: str) -> tuple[int, int]:
    """Parse FAA NAS Status XML and return (delay_count, ground_stop_count)."""
    root = ET.fromstring(text)
    delay_count = 0
    ground_stop_count = 0
    for delay_type in root.iter("Delay_type"):
        name_el = delay_type.find("Name")
        if name_el is None:
            continue
        name = (name_el.text or "").strip()
        if name == "Ground Delay Programs":
            delay_count += len(delay_type.findall(".//Ground_Delay"))
        elif name == "Ground Stop Programs":
            ground_stop_count += len(delay_type.findall(".//Program"))
        elif "Arrival" in name or "Departure" in name:
            delay_count += len(delay_type.findall(".//Delay"))
    return delay_count, ground_stop_count


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

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        delay_count, _ = _parse_faa_xml(resp.text)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame([{"timestamp": ts, "value": float(delay_count)}])


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

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        _, ground_stop_count = _parse_faa_xml(resp.text)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame([{"timestamp": ts, "value": float(ground_stop_count)}])
