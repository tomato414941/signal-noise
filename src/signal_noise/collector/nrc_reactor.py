from __future__ import annotations

import re

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NRCReactorPowerCollector(BaseCollector):
    """Average power level across all US nuclear reactors (NRC daily report).

    Low average = multiple unplanned outages = energy supply stress.
    Normal average ~90%. Drops below 80% signal grid stress.
    """

    meta = CollectorMeta(
        name="nrc_reactor_avg_power",
        display_name="NRC US Nuclear Reactor Average Power %",
        update_frequency="daily",
        api_docs_url="https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/",
        domain="economy",
        category="energy",
    )

    URL = "https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/powerreactorstatusforlast365days.txt"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()

        by_day: dict[str, list[float]] = {}
        for line in resp.text.strip().split("\n"):
            parts = line.split("|")
            if len(parts) < 4:
                continue
            date_str = parts[0].strip()
            if not re.match(r"\d{2}/\d{2}/\d{4}", date_str):
                continue
            try:
                power = float(parts[2].strip())
                day = pd.Timestamp(date_str, tz="UTC").strftime("%Y-%m-%d")
                by_day.setdefault(day, []).append(power)
            except (ValueError, IndexError):
                continue

        if not by_day:
            raise RuntimeError("No NRC reactor data")

        rows = [
            {"date": pd.Timestamp(day, tz="UTC"),
             "value": sum(vals) / len(vals)}
            for day, vals in by_day.items()
        ]
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class NRCReactorOutageCountCollector(BaseCollector):
    """Daily count of US nuclear reactors at 0% power (unplanned outages).

    Spikes in simultaneous outages signal maintenance clustering or
    widespread grid/safety issues.
    """

    meta = CollectorMeta(
        name="nrc_reactor_outage_count",
        display_name="NRC US Nuclear Reactors at 0% Power",
        update_frequency="daily",
        api_docs_url="https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/",
        domain="economy",
        category="energy",
    )

    URL = "https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/powerreactorstatusforlast365days.txt"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()

        by_day: dict[str, int] = {}
        for line in resp.text.strip().split("\n"):
            parts = line.split("|")
            if len(parts) < 4:
                continue
            date_str = parts[0].strip()
            if not re.match(r"\d{2}/\d{2}/\d{4}", date_str):
                continue
            try:
                power = float(parts[2].strip())
                day = pd.Timestamp(date_str, tz="UTC").strftime("%Y-%m-%d")
                if power == 0:
                    by_day[day] = by_day.get(day, 0) + 1
                else:
                    by_day.setdefault(day, 0)
            except (ValueError, IndexError):
                continue

        if not by_day:
            raise RuntimeError("No NRC reactor data")

        rows = [
            {"date": pd.Timestamp(day, tz="UTC"), "value": float(count)}
            for day, count in by_day.items()
        ]
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
