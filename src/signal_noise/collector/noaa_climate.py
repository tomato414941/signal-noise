from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class GlobalTempAnomalyCollector(BaseCollector):
    """NOAA Global Land & Ocean Temperature Anomaly (monthly)."""

    meta = SourceMeta(
        name="noaa_global_temp",
        display_name="NOAA Global Temperature Anomaly",
        update_frequency="monthly",
        api_docs_url="https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series",
        domain="earth",
        category="climate",
    )

    def fetch(self) -> pd.DataFrame:
        url = (
            "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/"
            "global/time-series/globe/land_ocean/1/0/2015-2026.json"
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", {})

        if not data:
            raise RuntimeError("No NOAA global temperature data")

        rows = []
        for yyyymm, entry in data.items():
            anomaly = entry.get("anomaly")
            if anomaly is None:
                continue
            year = int(yyyymm[:4])
            month = int(yyyymm[4:])
            dt = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
            rows.append({"date": dt, "value": float(anomaly)})

        if not rows:
            raise RuntimeError("No valid NOAA temperature data")

        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)


class LandTempAnomalyCollector(BaseCollector):
    """NOAA Global Land-Only Temperature Anomaly (monthly)."""

    meta = SourceMeta(
        name="noaa_land_temp",
        display_name="NOAA Land Temperature Anomaly",
        update_frequency="monthly",
        api_docs_url="https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series",
        domain="earth",
        category="climate",
    )

    def fetch(self) -> pd.DataFrame:
        url = (
            "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/"
            "global/time-series/globe/land/1/0/2015-2026.json"
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", {})

        if not data:
            raise RuntimeError("No NOAA land temperature data")

        rows = []
        for yyyymm, entry in data.items():
            anomaly = entry.get("anomaly")
            if anomaly is None:
                continue
            year = int(yyyymm[:4])
            month = int(yyyymm[4:])
            dt = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
            rows.append({"date": dt, "value": float(anomaly)})

        if not rows:
            raise RuntimeError("No valid NOAA land temperature data")

        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)


class CO2DailyCollector(BaseCollector):
    """NOAA/GML Mauna Loa daily CO2 concentration (ppm)."""

    meta = SourceMeta(
        name="noaa_co2_daily",
        display_name="Mauna Loa CO2 (Daily)",
        update_frequency="daily",
        api_docs_url="https://gml.noaa.gov/ccgg/trends/data.html",
        domain="earth",
        category="climate",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.csv"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()

        # Skip comment lines starting with #
        lines = [
            line for line in resp.text.splitlines()
            if line.strip() and not line.startswith("#")
        ]

        if not lines:
            raise RuntimeError("No CO2 data from NOAA")

        csv_text = "\n".join(lines)
        df_raw = pd.read_csv(io.StringIO(csv_text), header=0)

        # Columns: year, month, day, decimal_date, co2_ppm
        rows = []
        for _, r in df_raw.iterrows():
            try:
                year = int(r.iloc[0])
                month = int(r.iloc[1])
                day = int(r.iloc[2])
                co2 = float(r.iloc[4])
                if co2 < 0:
                    continue
                dt = pd.Timestamp(year=year, month=month, day=day, tz="UTC")
                rows.append({"date": dt, "value": co2})
            except (ValueError, IndexError):
                continue

        if not rows:
            raise RuntimeError("No valid CO2 data")

        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)


class NASAGlobalTempCollector(BaseCollector):
    """NASA GISS Global Land-Ocean Temperature Index (monthly)."""

    meta = SourceMeta(
        name="nasa_giss_temp",
        display_name="NASA GISS Global Temperature Index",
        update_frequency="monthly",
        api_docs_url="https://data.giss.nasa.gov/gistemp/",
        domain="earth",
        category="climate",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()

        # Skip first header line "Land-Ocean: Global Means"
        lines = resp.text.splitlines()
        csv_start = 0
        for i, line in enumerate(lines):
            if line.startswith("Year,"):
                csv_start = i
                break

        csv_text = "\n".join(lines[csv_start:])
        df_raw = pd.read_csv(io.StringIO(csv_text))

        rows = []
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        for _, r in df_raw.iterrows():
            year = int(r["Year"])
            if year < 2015:
                continue
            for m_idx, m_name in enumerate(months):
                val = r.get(m_name)
                if val is None or val == "***" or pd.isna(val):
                    continue
                try:
                    dt = pd.Timestamp(year=year, month=m_idx + 1, day=1, tz="UTC")
                    rows.append({"date": dt, "value": float(val)})
                except (ValueError, TypeError):
                    continue

        if not rows:
            raise RuntimeError("No valid NASA GISS data")

        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
