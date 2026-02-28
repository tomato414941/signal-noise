"""BLS (Bureau of Labor Statistics) collectors.

Uses BLS Public Data API v2.  No API key required for basic access (v1).
v2 with key allows more requests: set BLS_API_KEY env var.

All 46 series are fetched in batched requests (max 25 per request without key,
50 with key) and cached via SharedAPICache to avoid rate limit issues.

Docs: https://www.bls.gov/developers/
"""
from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

import pandas as pd
import requests

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta

log = logging.getLogger(__name__)

_BLS_BASE = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
_bls_cache = SharedAPICache(ttl=3600)  # cache for 1 hour (data is monthly)

# (series_id, collector_name, display_name, frequency, domain, category)
BLS_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── CPI (Consumer Price Index) ─────────────────────────────
    ("CUSR0000SA0", "bls_cpi_all", "BLS CPI All Urban (SA)", "monthly", "macro", "inflation"),
    ("CUSR0000SA0L1E", "bls_cpi_core", "BLS CPI Core (ex Food/Energy)", "monthly", "macro", "inflation"),
    ("CUSR0000SAF1", "bls_cpi_food", "BLS CPI Food", "monthly", "macro", "inflation"),
    ("CUSR0000SACE", "bls_cpi_energy", "BLS CPI Energy", "monthly", "macro", "inflation"),
    ("CUSR0000SAH1", "bls_cpi_shelter", "BLS CPI Shelter", "monthly", "macro", "inflation"),
    ("CUSR0000SETB01", "bls_cpi_gasoline", "BLS CPI Gasoline", "monthly", "macro", "inflation"),
    ("CUSR0000SAM", "bls_cpi_medical", "BLS CPI Medical Care", "monthly", "macro", "inflation"),
    ("CUSR0000SAE", "bls_cpi_education", "BLS CPI Education/Communication", "monthly", "macro", "inflation"),
    ("CUSR0000SETA01", "bls_cpi_new_vehicles", "BLS CPI New Vehicles", "monthly", "macro", "inflation"),
    ("CUSR0000SETA02", "bls_cpi_used_vehicles", "BLS CPI Used Vehicles", "monthly", "macro", "inflation"),
    ("CUSR0000SAA", "bls_cpi_apparel", "BLS CPI Apparel", "monthly", "macro", "inflation"),

    # ── PPI (Producer Price Index) ─────────────────────────────
    ("WPUFD49104", "bls_ppi_final", "BLS PPI Final Demand", "monthly", "macro", "inflation"),
    ("WPUFD4131", "bls_ppi_core", "BLS PPI Core (ex Food/Energy)", "monthly", "macro", "inflation"),
    ("WPUFD41", "bls_ppi_goods", "BLS PPI Finished Goods", "monthly", "macro", "inflation"),

    # ── Employment ─────────────────────────────────────────────
    ("CES0000000001", "bls_nfp_total", "BLS Total Nonfarm Payrolls", "monthly", "macro", "labor"),
    ("CES0500000001", "bls_nfp_private", "BLS Private Payrolls", "monthly", "macro", "labor"),
    ("CES3000000001", "bls_nfp_mfg", "BLS Manufacturing Payrolls", "monthly", "macro", "labor"),
    ("CES4200000001", "bls_nfp_retail", "BLS Retail Trade Payrolls", "monthly", "macro", "labor"),
    ("CES6500000001", "bls_nfp_edu_health", "BLS Education/Health Payrolls", "monthly", "macro", "labor"),
    ("CES7000000001", "bls_nfp_leisure", "BLS Leisure/Hospitality Payrolls", "monthly", "macro", "labor"),
    ("CES2000000001", "bls_nfp_construction", "BLS Construction Payrolls", "monthly", "macro", "labor"),
    ("CES9000000001", "bls_nfp_govt", "BLS Government Payrolls", "monthly", "macro", "labor"),
    ("CES6000000001", "bls_nfp_professional", "BLS Professional/Business Payrolls", "monthly", "macro", "labor"),
    ("CES4000000001", "bls_nfp_trade_transport", "BLS Trade/Transport/Utilities Payrolls", "monthly", "macro", "labor"),
    ("CES5000000001", "bls_nfp_info", "BLS Information Payrolls", "monthly", "macro", "labor"),
    ("CES5500000001", "bls_nfp_financial", "BLS Financial Activities Payrolls", "monthly", "macro", "labor"),

    # ── Unemployment ───────────────────────────────────────────
    ("LNS14000000", "bls_unemp_rate", "BLS Unemployment Rate", "monthly", "macro", "labor"),
    ("LNS14000001", "bls_unemp_rate_men", "BLS Unemployment Rate (Men)", "monthly", "macro", "labor"),
    ("LNS14000002", "bls_unemp_rate_women", "BLS Unemployment Rate (Women)", "monthly", "macro", "labor"),
    ("LNS12000000", "bls_labor_force", "BLS Civilian Labor Force", "monthly", "macro", "labor"),
    ("LNS11300000", "bls_participation", "BLS Labor Force Participation Rate", "monthly", "macro", "labor"),
    ("LNS12032194", "bls_u6_rate", "BLS U-6 Unemployment Rate", "monthly", "macro", "labor"),

    # ── Unemployment components ────────────────────────────────
    ("LNS13023621", "bls_job_losers", "BLS Unemployment - Job Losers", "monthly", "macro", "labor"),
    ("LNS13023557", "bls_reentrants", "BLS Unemployment - Reentrants", "monthly", "macro", "labor"),

    # ── Average hourly/weekly earnings ─────────────────────────
    ("CES0500000003", "bls_avg_hourly_priv", "BLS Avg Hourly Earnings (Private)", "monthly", "macro", "labor"),
    ("CES0500000011", "bls_avg_weekly_priv", "BLS Avg Weekly Earnings (Private)", "monthly", "macro", "labor"),
    ("CES0500000002", "bls_avg_hours_priv", "BLS Avg Weekly Hours (Private)", "monthly", "macro", "labor"),

    # ── Import/Export prices ───────────────────────────────────
    ("EIUIR", "bls_import_price", "BLS Import Price Index (All)", "monthly", "macro", "trade"),
    ("EIUIR100", "bls_import_ex_fuel", "BLS Import Price ex Fuel", "monthly", "macro", "trade"),
    ("EIUIQ", "bls_export_price", "BLS Export Price Index (All)", "monthly", "macro", "trade"),

    # ── Productivity ───────────────────────────────────────────
    ("PRS85006092", "bls_nfb_productivity", "BLS Nonfarm Productivity", "quarterly", "macro", "economic"),
    ("PRS85006112", "bls_unit_labor_cost", "BLS Unit Labor Costs", "quarterly", "macro", "economic"),

    # ── JOLTS (Job Openings) ───────────────────────────────────
    ("JTS000000000000000JOL", "bls_jolts_openings", "BLS JOLTS Job Openings", "monthly", "macro", "labor"),
    ("JTS000000000000000HIR", "bls_jolts_hires", "BLS JOLTS Hires", "monthly", "macro", "labor"),
    ("JTS000000000000000TSR", "bls_jolts_separations", "BLS JOLTS Total Separations", "monthly", "macro", "labor"),
    ("JTS000000000000000QUR", "bls_jolts_quits", "BLS JOLTS Quits", "monthly", "macro", "labor"),
]

_ALL_SERIES_IDS = [s[0] for s in BLS_SERIES]
_SERIES_ID_BY_NAME = {s[1]: s[0] for s in BLS_SERIES}


def _fetch_bls_batch(series_ids: list[str], timeout: int = 30) -> dict[str, list[dict]]:
    """Fetch multiple BLS series in a single API call.

    Returns {series_id: [data_items]} mapping.
    """
    now = datetime.now(UTC)
    start_year = now.year - 19
    end_year = now.year

    payload: dict = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    api_key = os.environ.get("BLS_API_KEY")
    if api_key:
        payload["registrationkey"] = api_key

    resp = requests.post(_BLS_BASE, json=payload, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()

    if body.get("status") == "REQUEST_NOT_PROCESSED":
        msgs = body.get("message", [])
        raise RuntimeError(f"BLS API rejected request: {'; '.join(msgs)}")

    result: dict[str, list[dict]] = {}
    for series in body.get("Results", {}).get("series", []):
        sid = series.get("seriesID", "")
        result[sid] = series.get("data", [])

    return result


def _get_all_bls_data(timeout: int = 30) -> dict[str, list[dict]]:
    """Fetch all BLS series using batched requests, cached for 1 hour."""
    api_key = os.environ.get("BLS_API_KEY")
    batch_size = 50 if api_key else 25

    def _fetch() -> dict[str, list[dict]]:
        all_data: dict[str, list[dict]] = {}
        for i in range(0, len(_ALL_SERIES_IDS), batch_size):
            batch = _ALL_SERIES_IDS[i : i + batch_size]
            result = _fetch_bls_batch(batch, timeout=timeout)
            all_data.update(result)
            log.info("BLS batch %d/%d: fetched %d series",
                     i // batch_size + 1,
                     (len(_ALL_SERIES_IDS) + batch_size - 1) // batch_size,
                     len(result))
        return all_data

    return _bls_cache.get_or_fetch("all_series", _fetch)


def _parse_bls_items(items: list[dict]) -> list[dict]:
    """Parse BLS data items into {date, value} rows."""
    rows: list[dict] = []
    for item in items:
        year = item.get("year")
        period = item.get("period", "")
        val = item.get("value")
        if not year or not val:
            continue
        if period.startswith("M") and period != "M13":
            month = int(period[1:])
            dt = f"{year}-{month:02d}-01"
        elif period.startswith("Q"):
            q = int(period[1:])
            month = (q - 1) * 3 + 1
            dt = f"{year}-{month:02d}-01"
        elif period.startswith("A"):
            dt = f"{year}-01-01"
        else:
            continue
        try:
            rows.append({
                "date": pd.to_datetime(dt, utc=True),
                "value": float(val),
            })
        except (ValueError, TypeError):
            continue
    return rows


def _make_bls_collector(
    series_id: str, name: str, display_name: str,
    frequency: str, domain: str, category: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://www.bls.gov/developers/",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            all_data = _get_all_bls_data(timeout=self.config.request_timeout)
            items = all_data.get(series_id, [])
            if not items:
                raise RuntimeError(f"No BLS data for {series_id}")
            rows = _parse_bls_items(items)
            if not rows:
                raise RuntimeError(f"No parseable BLS data for {series_id}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"BLS_{name}"
    _Collector.__qualname__ = f"BLS_{name}"
    return _Collector


def get_bls_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for series_id, name, display, freq, domain, cat in BLS_SERIES:
        collectors[name] = _make_bls_collector(
            series_id, name, display, freq, domain, cat,
        )
    return collectors
