"""OpenFDA — drug adverse events and food/drug recall counts.

Free API, no key required. Returns daily/weekly count time series.
API: https://open.fda.gov/apis/
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://api.fda.gov"


def _fetch_fda_counts(endpoint: str, count_field: str, limit: int = 1000) -> pd.DataFrame:
    """Fetch count time series from OpenFDA."""
    url = f"{_BASE_URL}/{endpoint}?count={count_field}&limit={limit}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    if not results:
        return pd.DataFrame(columns=["date", "value"])
    rows = []
    for r in results:
        t = str(r["time"])
        date = pd.Timestamp(f"{t[:4]}-{t[4:6]}-{t[6:8]}")
        rows.append({"date": date, "value": float(r["count"])})
    df = pd.DataFrame(rows)
    return df.sort_values("date").reset_index(drop=True)


class FDADrugAdverseEventsCollector(BaseCollector):
    """Daily count of FDA drug adverse event reports (FAERS)."""

    meta = CollectorMeta(
        name="fda_drug_adverse_events",
        display_name="FDA Drug Adverse Events",
        update_frequency="daily",
        api_docs_url="https://open.fda.gov/apis/drug/event/",
        domain="society",
        category="public_health",
    )

    def fetch(self) -> pd.DataFrame:
        return _fetch_fda_counts("drug/event.json", "receivedate")


class FDADrugRecallsCollector(BaseCollector):
    """Weekly count of FDA drug recall enforcement actions."""

    meta = CollectorMeta(
        name="fda_drug_recalls",
        display_name="FDA Drug Recalls",
        update_frequency="weekly",
        api_docs_url="https://open.fda.gov/apis/drug/enforcement/",
        domain="society",
        category="public_health",
    )

    def fetch(self) -> pd.DataFrame:
        return _fetch_fda_counts("drug/enforcement.json", "report_date")


class FDAFoodRecallsCollector(BaseCollector):
    """Weekly count of FDA food recall enforcement actions."""

    meta = CollectorMeta(
        name="fda_food_recalls",
        display_name="FDA Food Recalls",
        update_frequency="weekly",
        api_docs_url="https://open.fda.gov/apis/food/enforcement/",
        domain="society",
        category="public_health",
    )

    def fetch(self) -> pd.DataFrame:
        return _fetch_fda_counts("food/enforcement.json", "report_date")
