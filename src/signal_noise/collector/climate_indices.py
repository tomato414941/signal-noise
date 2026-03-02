from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

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

    meta = CollectorMeta(
        name="enso",
        display_name="ENSO Oceanic Niño Index",
        update_frequency="monthly",
        api_docs_url="https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt",
        domain="environment",
        category="climate",
    )

    URL = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()

        rows = []
        for line in resp.text.strip().split("\n"):
            parts = line.split()
            if len(parts) < 4 or parts[0] not in _SEASON_MONTH:
                continue
            try:
                season, year = parts[0], int(parts[1])
                anom = float(parts[-1])
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

    meta = CollectorMeta(
        name="arctic_oscillation",
        display_name="Arctic Oscillation Index (monthly)",
        update_frequency="monthly",
        api_docs_url="https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_ao_index/ao.shtml",
        domain="environment",
        category="climate",
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

    meta = CollectorMeta(
        name="nao",
        display_name="North Atlantic Oscillation Index (monthly)",
        update_frequency="monthly",
        api_docs_url="https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/nao.shtml",
        domain="environment",
        category="climate",
    )

    URL = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/norm.nao.monthly.b5001.current.ascii"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        return ArcticOscillationCollector._parse_monthly_index(resp.text)


class SoiCollector(BaseCollector):
    """Southern Oscillation Index (SOI) — standardized sea-level pressure
    difference between Tahiti and Darwin.

    Negative SOI = El Niño, Positive SOI = La Niña.
    Complements ONI with an atmospheric perspective.
    """

    meta = CollectorMeta(
        name="soi",
        display_name="Southern Oscillation Index (SOI)",
        update_frequency="monthly",
        api_docs_url="https://www.cpc.ncep.noaa.gov/data/indices/soi",
        domain="environment",
        category="climate",
    )

    URL = "https://www.cpc.ncep.noaa.gov/data/indices/soi"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            parts = line.split()
            if len(parts) < 13:
                continue
            try:
                year = int(parts[0])
            except ValueError:
                continue
            for month_idx in range(1, 13):
                try:
                    val = float(parts[month_idx])
                    if val < -90:
                        continue
                    date = pd.Timestamp(year=year, month=month_idx, day=15, tz="UTC")
                    rows.append({"date": date, "value": val})
                except (ValueError, IndexError):
                    continue
        if not rows:
            raise RuntimeError("No SOI data parsed")
        df = pd.DataFrame(rows)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5)
        df = df[df["date"] >= cutoff]
        return df.sort_values("date").reset_index(drop=True)


class PdoCollector(BaseCollector):
    """Pacific Decadal Oscillation (PDO) — leading SST pattern in
    North Pacific (poleward of 20N).

    Multi-decadal influence on fisheries, drought, and Pacific-rim climate.
    """

    meta = CollectorMeta(
        name="pdo",
        display_name="Pacific Decadal Oscillation (PDO)",
        update_frequency="monthly",
        api_docs_url="https://www.ncei.noaa.gov/pub/data/cmb/ersst/v5/index/ersst.v5.pdo.dat",
        domain="environment",
        category="climate",
    )

    URL = "https://www.ncei.noaa.gov/pub/data/cmb/ersst/v5/index/ersst.v5.pdo.dat"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            parts = line.split()
            if len(parts) < 13:
                continue
            try:
                year = int(parts[0])
            except ValueError:
                continue
            for month_idx in range(1, 13):
                try:
                    val = float(parts[month_idx])
                    if val > 90 or val < -90:
                        continue
                    date = pd.Timestamp(year=year, month=month_idx, day=15, tz="UTC")
                    rows.append({"date": date, "value": val})
                except (ValueError, IndexError):
                    continue
        if not rows:
            raise RuntimeError("No PDO data parsed")
        df = pd.DataFrame(rows)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5)
        df = df[df["date"] >= cutoff]
        return df.sort_values("date").reset_index(drop=True)


class AmoCollector(BaseCollector):
    """Atlantic Multidecadal Oscillation (AMO) — detrended SST anomaly
    in North Atlantic.

    Warm AMO phase → more Atlantic hurricanes, Sahel rainfall, European warmth.
    """

    meta = CollectorMeta(
        name="amo",
        display_name="Atlantic Multidecadal Oscillation (AMO)",
        update_frequency="monthly",
        api_docs_url="https://www.psl.noaa.gov/data/timeseries/AMO/",
        domain="environment",
        category="climate",
    )

    URL = "https://www.psl.noaa.gov/data/correlation/amon.us.long.data"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        rows = []
        lines = resp.text.strip().split("\n")
        for line in lines:
            parts = line.split()
            if len(parts) < 13:
                continue
            try:
                year = int(parts[0])
            except ValueError:
                continue
            for month_idx in range(1, 13):
                try:
                    val = float(parts[month_idx])
                    if val < -90:
                        continue
                    date = pd.Timestamp(year=year, month=month_idx, day=15, tz="UTC")
                    rows.append({"date": date, "value": val})
                except (ValueError, IndexError):
                    continue
        if not rows:
            raise RuntimeError("No AMO data parsed")
        df = pd.DataFrame(rows)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5)
        df = df[df["date"] >= cutoff]
        return df.sort_values("date").reset_index(drop=True)


class IodCollector(BaseCollector):
    """Indian Ocean Dipole (IOD) Mode Index — SST gradient between
    western and eastern equatorial Indian Ocean.

    Positive IOD = drought in Australia/Indonesia, wet in East Africa.
    """

    meta = CollectorMeta(
        name="iod",
        display_name="Indian Ocean Dipole (IOD) Mode Index",
        update_frequency="monthly",
        api_docs_url="https://www.psl.noaa.gov/gcos_wgsp/Timeseries/DMI/",
        domain="environment",
        category="climate",
    )

    URL = "https://www.psl.noaa.gov/gcos_wgsp/Timeseries/Data/dmi.had.long.data"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        rows = []
        lines = resp.text.strip().split("\n")
        for line in lines:
            parts = line.split()
            if len(parts) < 13:
                continue
            try:
                year = int(parts[0])
            except ValueError:
                continue
            for month_idx in range(1, 13):
                try:
                    val = float(parts[month_idx])
                    if val < -90:
                        continue
                    date = pd.Timestamp(year=year, month=month_idx, day=15, tz="UTC")
                    rows.append({"date": date, "value": val})
                except (ValueError, IndexError):
                    continue
        if not rows:
            raise RuntimeError("No IOD data parsed")
        df = pd.DataFrame(rows)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 5)
        df = df[df["date"] >= cutoff]
        return df.sort_values("date").reset_index(drop=True)
