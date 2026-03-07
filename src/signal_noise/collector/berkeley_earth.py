from __future__ import annotations

import io

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta


class BerkeleyEarthGlobalTempCollector(BaseCollector):
    """Berkeley Earth global land-ocean temperature anomaly (monthly)."""

    meta = CollectorMeta(
        name="berkeley_global_temp",
        display_name="Berkeley Earth Global Temperature Anomaly",
        update_frequency="monthly",
        api_docs_url="https://berkeleyearth.org/data/",
        domain="environment",
        category="climate",
    )

    URL = (
        "https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com/"
        "Global/Land_and_Ocean_complete.txt"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()

        df_raw = pd.read_csv(
            io.StringIO(resp.text),
            comment="%",
            sep=r"\s+",
            header=None,
            names=[
                "year",
                "month",
                "anomaly",
                "uncertainty",
                "annual_anomaly",
                "annual_uncertainty",
                "five_year_anomaly",
                "five_year_uncertainty",
                "ten_year_anomaly",
                "ten_year_uncertainty",
                "twenty_year_anomaly",
                "twenty_year_uncertainty",
            ],
        ).dropna(subset=["year", "month", "anomaly"])

        df_raw["year"] = df_raw["year"].astype(int)
        df_raw["month"] = df_raw["month"].astype(int)
        reset_points = df_raw.index[
            (df_raw["year"].diff() < 0)
            | ((df_raw["year"].diff() == 0) & (df_raw["month"].diff() < 0))
        ]
        if not reset_points.empty:
            df_raw = df_raw.iloc[: reset_points[0]]

        df_raw = df_raw[df_raw["year"] >= 2015]
        if df_raw.empty:
            raise RuntimeError("No Berkeley Earth global temperature data")

        rows = [
            {
                "date": pd.Timestamp(year=int(row.year), month=int(row.month), day=1, tz="UTC"),
                "value": float(row.anomaly),
            }
            for row in df_raw.itertuples(index=False)
        ]
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
