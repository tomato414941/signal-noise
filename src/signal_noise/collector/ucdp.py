from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


_UCDP_BASE = "https://ucdpapi.pcr.uu.se/api"


class UCDPBattleDeathsCollector(BaseCollector):
    """UCDP Battle-Related Deaths — global annual best estimate."""

    meta = CollectorMeta(
        name="ucdp_battle_deaths",
        display_name="UCDP Global Battle-Related Deaths",
        update_frequency="yearly",
        api_docs_url="https://ucdp.uu.se/apidocs/",
        domain="conflict",
        category="armed_conflict",
    )

    URL = f"{_UCDP_BASE}/battledeaths/24.1?pagesize=1000"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("Result", [])
        if not data:
            raise RuntimeError("No UCDP battle deaths data")
        yearly: dict[int, float] = {}
        for entry in data:
            try:
                year = int(entry["Year"])
                best = float(entry.get("BdBest", 0))
                yearly[year] = yearly.get(year, 0) + best
            except (KeyError, ValueError, TypeError):
                continue
        rows = [
            {"date": pd.Timestamp(year=y, month=1, day=1, tz="UTC"), "value": v}
            for y, v in sorted(yearly.items())
        ]
        return pd.DataFrame(rows)


class UCDPConflictCountCollector(BaseCollector):
    """UCDP Armed Conflicts — number of active conflicts per year."""

    meta = CollectorMeta(
        name="ucdp_conflict_count",
        display_name="UCDP Active Armed Conflicts Count",
        update_frequency="yearly",
        api_docs_url="https://ucdp.uu.se/apidocs/",
        domain="conflict",
        category="armed_conflict",
    )

    URL = f"{_UCDP_BASE}/armedconflict/24.1?pagesize=1000"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("Result", [])
        if not data:
            raise RuntimeError("No UCDP conflict data")
        yearly: dict[int, int] = {}
        for entry in data:
            try:
                year = int(entry["Year"])
                yearly[year] = yearly.get(year, 0) + 1
            except (KeyError, ValueError, TypeError):
                continue
        rows = [
            {"date": pd.Timestamp(year=y, month=1, day=1, tz="UTC"), "value": v}
            for y, v in sorted(yearly.items())
        ]
        return pd.DataFrame(rows)
