from __future__ import annotations

from collections import Counter

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SolarFlareCollector(BaseCollector):
    """Daily solar flare count from NOAA SWPC.

    Counts X, M, and C class flares in the past 30 days.
    High activity = geomagnetic storm risk, satellite/GPS disruption.
    """

    meta = CollectorMeta(
        name="solar_flare_count",
        display_name="NOAA Solar Flare Daily Count",
        update_frequency="daily",
        api_docs_url="https://services.swpc.noaa.gov/json/goes/primary/xray-flares-latest.json",
        domain="environment",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/json/goes/primary/xray-flares-latest.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        events = resp.json()
        by_day: Counter[str] = Counter()
        for ev in events:
            ts = ev.get("begin_time") or ev.get("max_time")
            if not ts:
                continue
            day = ts[:10]
            by_day[day] += 1
        if not by_day:
            raise RuntimeError("No solar flare data")
        rows = [
            {"date": pd.Timestamp(day, tz="UTC"), "value": float(count)}
            for day, count in by_day.items()
        ]
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class KpIndexCollector(BaseCollector):
    """Planetary K-index (Kp) from NOAA SWPC.

    3-hourly geomagnetic activity index (0-9).
    Kp >= 5 = geomagnetic storm, affects power grids and communications.
    """

    meta = CollectorMeta(
        name="kp_index",
        display_name="NOAA Kp Index (daily max)",
        update_frequency="daily",
        api_docs_url="https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json",
        domain="environment",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        by_day: dict[str, float] = {}
        for row in data:
            if not isinstance(row, list) or len(row) < 2:
                continue
            try:
                day = str(row[0])[:10]
                kp = float(row[1])
                if day not in by_day or kp > by_day[day]:
                    by_day[day] = kp
            except (ValueError, TypeError):
                continue
        if not by_day:
            raise RuntimeError("No Kp data")
        rows = [
            {"date": pd.Timestamp(day, tz="UTC"), "value": val}
            for day, val in by_day.items()
        ]
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class SolarWindSpeedCollector(BaseCollector):
    """Solar wind speed from NOAA DSCOVR satellite.

    Normal: ~400 km/s; CME arrival: 600-2000 km/s.
    """

    meta = CollectorMeta(
        name="solar_wind_speed",
        display_name="NOAA Solar Wind Speed (km/s)",
        update_frequency="hourly",
        api_docs_url="https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json",
        domain="environment",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for row in data:
            if not isinstance(row, list) or len(row) < 3:
                continue
            try:
                ts = pd.Timestamp(row[0], tz="UTC")
                speed = float(row[2])
                rows.append({"timestamp": ts, "value": speed})
            except (ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No solar wind data")
        df = pd.DataFrame(rows)
        df = df.set_index("timestamp").resample("1h").mean().dropna().reset_index()
        return df.sort_values("timestamp").reset_index(drop=True)


class SunspotNumberCollector(BaseCollector):
    """Daily sunspot number from NOAA SWPC.

    Tracks 11-year solar cycle; high sunspot count = active Sun.
    """

    meta = CollectorMeta(
        name="swpc_sunspot_number",
        display_name="NOAA SWPC Daily Sunspot Number",
        update_frequency="daily",
        api_docs_url="https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json",
        domain="environment",
        category="space_weather",
    )

    URL = "https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for entry in data:
            try:
                ts = pd.Timestamp(entry["time-tag"], tz="UTC")
                ssn = float(entry["ssn"])
                rows.append({"date": ts, "value": ssn})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No sunspot data")
        df = pd.DataFrame(rows)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5)
        df = df[df["date"] >= cutoff]
        return df.sort_values("date").reset_index(drop=True)
