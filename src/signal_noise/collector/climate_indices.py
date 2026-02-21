from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# Season midpoint mapping for ONI data
_SEASON_MONTH = {
    "DJF": 1, "JFM": 2, "FMA": 3, "MAM": 4,
    "AMJ": 5, "MJJ": 6, "JJA": 7, "JAS": 8,
    "ASO": 9, "SON": 10, "OND": 11, "NDJ": 12,
}


class EnsoCollector(BaseCollector):
    """Oceanic Niño Index (ONI) — 3-month running mean SST anomaly in Niño 3.4 region.

    Positive = El Niño, Negative = La Niña.
    Strong influence on global commodity prices.
    """

    meta = SourceMeta(
        name="enso",
        display_name="ENSO Oceanic Niño Index",
        update_frequency="monthly",
        data_type="climate",
        api_docs_url="https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt",
    )

    URL = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()

        rows = []
        for line in resp.text.strip().split("\n"):
            parts = line.split()
            if len(parts) < 5 or parts[0] not in _SEASON_MONTH:
                continue
            try:
                season, year = parts[0], int(parts[1])
                anom = float(parts[4])
                month = _SEASON_MONTH[season]
                date = pd.Timestamp(year=year, month=month, day=15, tz="UTC")
                rows.append({"date": date, "value": anom})
            except (ValueError, IndexError):
                continue

        if not rows:
            raise RuntimeError("No ENSO data parsed")

        df = pd.DataFrame(rows)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5)
        df = df[df["date"] >= cutoff]
        return df.sort_values("date").reset_index(drop=True)


class ArcticOscillationCollector(BaseCollector):
    """Monthly Arctic Oscillation (AO) index from NOAA CPC.

    Positive AO = strong polar vortex, mild winters.
    Negative AO = weak polar vortex, cold outbreaks, energy demand spikes.
    """

    meta = SourceMeta(
        name="arctic_oscillation",
        display_name="Arctic Oscillation Index (monthly)",
        update_frequency="monthly",
        data_type="climate",
        api_docs_url="https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_ao_index/ao.shtml",
    )

    URL = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_ao_index/monthly.ao.index.b50.current.ascii"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        return self._parse_monthly_index(resp.text)

    @staticmethod
    def _parse_monthly_index(text: str) -> pd.DataFrame:
        rows = []
        for line in text.strip().split("\n"):
            parts = line.split()
            if len(parts) < 3:
                continue
            try:
                year, month = int(parts[0]), int(parts[1])
                value = float(parts[2])
                if value < -90:
                    continue
                date = pd.Timestamp(year=year, month=month, day=15, tz="UTC")
                rows.append({"date": date, "value": value})
            except (ValueError, IndexError):
                continue

        if not rows:
            raise RuntimeError("No AO data parsed")

        df = pd.DataFrame(rows)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5)
        df = df[df["date"] >= cutoff]
        return df.sort_values("date").reset_index(drop=True)


class NaoCollector(BaseCollector):
    """Monthly North Atlantic Oscillation (NAO) index from NOAA CPC.

    Positive NAO = strong Icelandic low, warm European winters.
    Influences European equity and energy markets.
    """

    meta = SourceMeta(
        name="nao",
        display_name="North Atlantic Oscillation Index (monthly)",
        update_frequency="monthly",
        data_type="climate",
        api_docs_url="https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/nao.shtml",
    )

    URL = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/norm.nao.monthly.b5001.current.ascii"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        return ArcticOscillationCollector._parse_monthly_index(resp.text)
