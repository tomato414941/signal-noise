"""CDC NWSS Wastewater Surveillance — SARS-CoV-2 pathogen levels.

Free API via Socrata SODA. No authentication required.
API: https://data.cdc.gov/resource/2ew6-ywp6

Fields are text type so aggregation is done in Python.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_nwss_cache = SharedAPICache(ttl=300)

_NWSS_URL = "https://data.cdc.gov/resource/2ew6-ywp6.json"


def _fetch_wastewater_data() -> pd.DataFrame:
    """Fetch recent wastewater data and aggregate by date."""
    params = {
        "$select": "date_end, detect_prop_15d, ptc_15d",
        "$order": "date_end DESC",
        "$limit": "50000",
        "$where": "detect_prop_15d IS NOT NULL",
    }
    resp = requests.get(_NWSS_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return pd.DataFrame(columns=["date", "detect", "change"])

    rows = []
    for rec in data:
        detect = rec.get("detect_prop_15d")
        ptc = rec.get("ptc_15d")
        if detect is None:
            continue
        rows.append({
            "date": rec["date_end"],
            "detect": float(detect),
            "change": float(ptc) if ptc is not None else None,
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    grouped = df.groupby("date").agg(
        detect=("detect", "mean"),
        change=("change", "mean"),
    ).reset_index()
    return grouped.sort_values("date").reset_index(drop=True)


class CDCWastewaterDetectionCollector(BaseCollector):
    """National average SARS-CoV-2 wastewater detection rate (%)."""

    meta = CollectorMeta(
        name="cdc_wastewater_detection",
        display_name="CDC Wastewater Detection Rate",
        update_frequency="weekly",
        api_docs_url="https://data.cdc.gov/Public-Health-Surveillance/NWSS-Public-SARS-CoV-2-Wastewater-Metric-Data/2ew6-ywp6",
        domain="health",
        category="epidemiology",
    )

    def fetch(self) -> pd.DataFrame:
        agg = _nwss_cache.get_or_fetch("nwss", _fetch_wastewater_data)
        return agg[["date", "detect"]].rename(columns={"detect": "value"})


class CDCWastewaterChangeCollector(BaseCollector):
    """National average SARS-CoV-2 wastewater percent change (15-day)."""

    meta = CollectorMeta(
        name="cdc_wastewater_change",
        display_name="CDC Wastewater % Change",
        update_frequency="weekly",
        api_docs_url="https://data.cdc.gov/Public-Health-Surveillance/NWSS-Public-SARS-CoV-2-Wastewater-Metric-Data/2ew6-ywp6",
        domain="health",
        category="epidemiology",
    )

    def fetch(self) -> pd.DataFrame:
        agg = _nwss_cache.get_or_fetch("nwss", _fetch_wastewater_data)
        df = agg[["date", "change"]].rename(columns={"change": "value"})
        return df.dropna(subset=["value"]).reset_index(drop=True)
