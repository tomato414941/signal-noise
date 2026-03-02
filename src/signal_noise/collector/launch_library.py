from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class RocketLaunchCountCollector(BaseCollector):
    """Monthly orbital launch count from Launch Library 2.

    Tracks the cadence of space launches worldwide.
    Acceleration = growing space economy; deceleration = industry contraction.
    """

    meta = CollectorMeta(
        name="rocket_launch_count",
        display_name="Orbital Launches (monthly count)",
        update_frequency="monthly",
        api_docs_url="https://thespacedevs.com/llapi",
        domain="infrastructure",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        end = datetime.now(UTC)
        start = end - timedelta(days=365 * 2)
        url = (
            f"https://ll.thespacedevs.com/2.3.0/launches/"
            f"?net__gte={start.strftime('%Y-%m-%d')}"
            f"&net__lte={end.strftime('%Y-%m-%d')}"
            f"&limit=100&offset=0"
            f"&mode=list"
        )
        headers = {"User-Agent": "signal-noise/0.1 (research)"}
        resp = requests.get(url, headers=headers, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

        if not results:
            raise RuntimeError("No launch data")

        by_month: Counter[str] = Counter()
        for launch in results:
            net = launch.get("net", "")
            if net:
                month = net[:7]
                by_month[month] += 1

        rows = [
            {"date": pd.Timestamp(f"{month}-15", tz="UTC"), "value": float(count)}
            for month, count in by_month.items()
        ]
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class RocketLaunchSuccessRateCollector(BaseCollector):
    """Rolling 6-month launch success rate.

    Declining success rate signals systemic quality issues in
    space industry supply chain.
    """

    meta = CollectorMeta(
        name="rocket_launch_success_rate",
        display_name="Orbital Launch Success Rate (6-month rolling)",
        update_frequency="monthly",
        api_docs_url="https://thespacedevs.com/llapi",
        domain="infrastructure",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        end = datetime.now(UTC)
        start = end - timedelta(days=365 * 2)
        url = (
            f"https://ll.thespacedevs.com/2.3.0/launches/"
            f"?net__gte={start.strftime('%Y-%m-%d')}"
            f"&net__lte={end.strftime('%Y-%m-%d')}"
            f"&limit=100&offset=0"
            f"&mode=list"
        )
        headers = {"User-Agent": "signal-noise/0.1 (research)"}
        resp = requests.get(url, headers=headers, timeout=self.config.request_timeout)
        resp.raise_for_status()
        results = resp.json().get("results", [])

        if not results:
            raise RuntimeError("No launch data")

        launches = []
        for launch in results:
            net = launch.get("net", "")
            status = launch.get("status", {})
            status_id = status.get("id", 0) if isinstance(status, dict) else 0
            if net:
                launches.append({
                    "date": pd.Timestamp(net[:10], tz="UTC"),
                    "success": 1.0 if status_id == 3 else 0.0,
                })

        df = pd.DataFrame(launches).sort_values("date")
        df = df.set_index("date")
        rolling = df["success"].rolling("180D").mean().dropna().reset_index()
        rolling.columns = ["date", "value"]
        rolling["value"] = rolling["value"] * 100.0
        monthly = rolling.set_index("date").resample("MS").last().dropna().reset_index()
        return monthly.sort_values("date").reset_index(drop=True)
