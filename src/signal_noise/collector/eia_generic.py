"""EIA (U.S. Energy Information Administration) collectors.

Uses EIA API v2.  Requires EIA_API_KEY environment variable.
Free key: https://www.eia.gov/opendata/register.php

Collectors sharing the same route/facet/frequency are batched into a single
request and cached via SharedAPICache to minimize API calls.
"""
from __future__ import annotations

import logging
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd
import requests

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta

log = logging.getLogger(__name__)

_EIA_API_KEY: str | None = None
_eia_cache = SharedAPICache(ttl=3600)  # cache for 1 hour


def _get_eia_key() -> str:
    global _EIA_API_KEY
    if _EIA_API_KEY:
        return _EIA_API_KEY

    key = os.environ.get("EIA_API_KEY")
    if not key:
        secret = Path.home() / ".secrets" / "eia"
        if secret.exists():
            for line in secret.read_text().splitlines():
                if line.startswith("export EIA_API_KEY="):
                    key = line.split("=", 1)[1].strip().strip("'\"")
                    break
    if not key:
        raise RuntimeError(
            "EIA_API_KEY not set. Get a free key at https://www.eia.gov/opendata/register.php"
        )
    _EIA_API_KEY = key
    return key


_BASE_URL = "https://api.eia.gov/v2"

# (route, facet_key, facet_value, data_col, frequency, collector_name, display_name, domain, category)
EIA_SERIES: list[tuple[str, str, str, str, str, str, str, str, str]] = [
    # ── Petroleum spot prices ──────────────────────────────────
    ("petroleum/pri/spt/data", "series", "RWTC", "value", "daily",
     "eia_wti_spot", "EIA WTI Crude Spot", "markets", "commodity"),
    ("petroleum/pri/spt/data", "series", "RBRTE", "value", "daily",
     "eia_brent_spot", "EIA Brent Crude Spot", "markets", "commodity"),
    ("petroleum/pri/spt/data", "series", "EER_EPMRU_PF4_RGC_DPG", "value", "daily",
     "eia_gasoline_spot", "EIA Gasoline Spot (Gulf)", "markets", "commodity"),
    ("petroleum/pri/spt/data", "series", "EER_EPD2DXL0_PF4_RGC_DPG", "value", "daily",
     "eia_diesel_spot", "EIA Diesel Spot (Gulf)", "markets", "commodity"),
    ("petroleum/pri/spt/data", "series", "EER_EPD2F_PF4_Y35NY_DPG", "value", "daily",
     "eia_heating_oil_spot", "EIA Heating Oil Spot NY Harbor", "markets", "commodity"),
    ("petroleum/pri/spt/data", "series", "EER_EPJK_PF4_RGC_DPG", "value", "daily",
     "eia_jet_fuel_spot", "EIA Jet Fuel Spot", "markets", "commodity"),
    ("petroleum/pri/spt/data", "series", "EER_EPLLPA_PF4_Y44MB_DPG", "value", "daily",
     "eia_propane_spot", "EIA Propane Spot", "markets", "commodity"),

    # ── Natural gas spot prices ────────────────────────────────
    ("natural-gas/pri/fut/data", "series", "RNGWHHD", "value", "daily",
     "eia_henryhub_spot", "EIA Henry Hub Spot", "markets", "commodity"),
    ("natural-gas/pri/fut/data", "series", "RNGC1", "value", "daily",
     "eia_natgas_futures", "EIA Natural Gas Futures (1-mo)", "markets", "commodity"),

    # ── Petroleum supply/demand ────────────────────────────────
    ("petroleum/sum/sndw/data", "series", "WCRSTUS1", "value", "weekly",
     "eia_crude_stocks", "EIA US Crude Stocks", "markets", "commodity"),
    ("petroleum/sum/sndw/data", "series", "WGTSTUS1", "value", "weekly",
     "eia_gasoline_stocks", "EIA US Gasoline Stocks", "markets", "commodity"),
    ("petroleum/sum/sndw/data", "series", "WDISTUS1", "value", "weekly",
     "eia_distillate_stocks", "EIA US Distillate Stocks", "markets", "commodity"),
    ("petroleum/sum/sndw/data", "series", "WCRFPUS2", "value", "weekly",
     "eia_refinery_input", "EIA US Refinery Crude Input", "markets", "commodity"),
    ("petroleum/sum/sndw/data", "series", "WCESTUS1", "value", "weekly",
     "eia_crude_excl_spr", "EIA US Crude Stocks excl SPR", "markets", "commodity"),
    ("petroleum/sum/sndw/data", "series", "WCRIMUS2", "value", "weekly",
     "eia_crude_imports", "EIA US Crude Imports", "markets", "commodity"),
    ("petroleum/sum/sndw/data", "series", "WRPNTUS2", "value", "weekly",
     "eia_refinery_util", "EIA US Refinery Utilization %", "markets", "commodity"),

    # ── Petroleum production ───────────────────────────────────
    ("petroleum/crd/crpdn/data", "series", "MCRFPUS2", "value", "monthly",
     "eia_us_crude_prod", "EIA US Crude Production", "economy", "economic"),

    # ── Natural gas supply/demand ──────────────────────────────
    ("natural-gas/sum/snd/data", "series", "N9070US2", "value", "monthly",
     "eia_natgas_production", "EIA US Natural Gas Production", "economy", "economic"),
    ("natural-gas/sum/snd/data", "series", "N9140US2", "value", "monthly",
     "eia_natgas_consumption", "EIA US Natural Gas Consumption", "economy", "economic"),
    ("natural-gas/stor/wkly/data", "series", "NW2_EPG0_SWO_R48_BCF", "value", "weekly",
     "eia_natgas_storage", "EIA US Natural Gas Storage", "markets", "commodity"),

    # ── Coal ───────────────────────────────────────────────────
    ("coal/market-sales-price/data", "stateRegionId", "US", "price", "annual",
     "eia_coal_price", "EIA US Coal Avg Price", "markets", "commodity"),

    # ── Electricity ────────────────────────────────────────────
    ("electricity/retail-sales/data", "sectorid", "ALL", "price", "monthly",
     "eia_elec_price_total", "EIA US Electricity Price (All)", "markets", "commodity"),
    ("electricity/retail-sales/data", "sectorid", "RES", "price", "monthly",
     "eia_elec_price_res", "EIA US Electricity Price (Residential)", "markets", "commodity"),
    ("electricity/retail-sales/data", "sectorid", "COM", "price", "monthly",
     "eia_elec_price_com", "EIA US Electricity Price (Commercial)", "markets", "commodity"),
    ("electricity/retail-sales/data", "sectorid", "IND", "price", "monthly",
     "eia_elec_price_ind", "EIA US Electricity Price (Industrial)", "markets", "commodity"),
    ("electricity/retail-sales/data", "sectorid", "ALL", "revenue", "monthly",
     "eia_elec_revenue", "EIA US Electricity Revenue (All)", "economy", "economic"),
    ("electricity/retail-sales/data", "sectorid", "ALL", "sales", "monthly",
     "eia_elec_sales", "EIA US Electricity Sales (All)", "economy", "economic"),

    # ── US energy totals ───────────────────────────────────────
    ("total-energy/data", "msn", "TETCBUS", "value", "monthly",
     "eia_total_consumption", "EIA US Total Energy Consumption", "economy", "economic"),
    ("total-energy/data", "msn", "TEPRBUS", "value", "monthly",
     "eia_total_production", "EIA US Total Energy Production", "economy", "economic"),
    ("total-energy/data", "msn", "CLPRPUS", "value", "monthly",
     "eia_coal_production", "EIA US Coal Production", "economy", "economic"),
    ("total-energy/data", "msn", "REPRBUS", "value", "monthly",
     "eia_renewable_production", "EIA US Renewable Energy Production", "economy", "economic"),
    ("total-energy/data", "msn", "NUETPUS", "value", "monthly",
     "eia_nuclear_generation", "EIA US Nuclear Generation", "economy", "economic"),

    # ── STEO (Short-Term Energy Outlook) ──────────────────────
    ("steo/data", "seriesId", "PAPR_WORLD", "value", "monthly",
     "eia_world_oil_production", "EIA World Oil Production", "economy", "economic"),
    ("steo/data", "seriesId", "PATC_WORLD", "value", "monthly",
     "eia_world_oil_consumption", "EIA World Oil Consumption", "economy", "economic"),
    ("steo/data", "seriesId", "PASC_OECD_T3", "value", "monthly",
     "eia_oecd_oil_stocks", "EIA OECD Oil Stocks", "markets", "commodity"),
    ("steo/data", "seriesId", "BREPUUS", "value", "monthly",
     "eia_steo_brent_forecast", "EIA Brent Price Forecast", "markets", "commodity"),
    ("steo/data", "seriesId", "WTIPUUS", "value", "monthly",
     "eia_steo_wti_forecast", "EIA WTI Price Forecast", "markets", "commodity"),
    ("steo/data", "seriesId", "NGHHUUS", "value", "monthly",
     "eia_steo_henryhub_forecast", "EIA Henry Hub Price Forecast", "markets", "commodity"),

    # ── US gasoline retail prices ─────────────────────────────
    ("petroleum/pri/gnd/data", "series", "EMM_EPMR_PTE_NUS_DPG", "value", "weekly",
     "eia_gasoline_retail", "EIA US Gasoline Retail Price", "markets", "commodity"),
    ("petroleum/pri/gnd/data", "series", "EMM_EPMRU_PTE_NUS_DPG", "value", "weekly",
     "eia_gasoline_retail_reg", "EIA US Gasoline Regular Retail", "markets", "commodity"),
    ("petroleum/pri/gnd/data", "series", "EMD_EPD2D_PTE_NUS_DPG", "value", "weekly",
     "eia_diesel_retail", "EIA US Diesel Retail Price", "markets", "commodity"),

    # ── International petroleum ────────────────────────────────
    # TODO: international/data uses productId/activityId/countryRegionId facets,
    # not seriesId. Needs dedicated fetch logic (not batch-compatible).
    # ("international/data", ..., "eia_opec_production", ...),
    # ("international/data", ..., "eia_russia_oil_prod", ...),
    # ("international/data", ..., "eia_saudi_oil_prod", ...),
    # ("international/data", ..., "eia_china_oil_prod", ...),
    # ("international/data", ..., "eia_china_oil_consumption", ...),
]

# Build group index: (route, facet_key, frequency) -> [(facet_value, data_col, name), ...]
_GROUP_INDEX: dict[tuple[str, str, str], list[tuple[str, str, str]]] = defaultdict(list)
for _r, _fk, _fv, _dc, _freq, _name, *_ in EIA_SERIES:
    _GROUP_INDEX[(_r, _fk, _freq)].append((_fv, _dc, _name))

# Lookup: collector_name -> (route, facet_key, facet_value, data_col, frequency)
_SERIES_LOOKUP: dict[str, tuple[str, str, str, str, str]] = {
    s[5]: (s[0], s[1], s[2], s[3], s[4]) for s in EIA_SERIES
}


def _fetch_eia_group(
    route: str, facet_key: str, frequency: str,
    members: list[tuple[str, str, str]],
    timeout: int = 30,
) -> dict[str, list[dict]]:
    """Fetch multiple EIA series sharing the same route/facet/frequency.

    Returns {(facet_value, data_col): [rows]} mapping.
    """
    api_key = _get_eia_key()
    url = f"{_BASE_URL}/{route}"

    facet_values = list({fv for fv, _, _ in members})
    data_cols = list({dc for _, dc, _ in members})

    params: list[tuple[str, str]] = [
        ("api_key", api_key),
        ("frequency", frequency),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("length", "5000"),
    ]
    for fv in facet_values:
        params.append((f"facets[{facet_key}][]", fv))
    for dc in data_cols:
        params.append(("data[]", dc))

    all_rows: dict[tuple[str, str], list[dict]] = defaultdict(list)
    offset = 0
    while True:
        req_params = params + [("offset", str(offset))]
        resp = requests.get(url, params=req_params, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()
        data = body.get("response", {}).get("data", [])
        if not data:
            break
        for r in data:
            period = r.get("period")
            fv = r.get(facet_key)
            if not period or not fv:
                continue
            for dc in data_cols:
                val = r.get(dc)
                if val is not None:
                    try:
                        all_rows[(fv, dc)].append({
                            "date": pd.to_datetime(str(period), utc=True),
                            "value": float(val),
                        })
                    except (ValueError, TypeError):
                        continue
        total = int(body.get("response", {}).get("total", 0))
        offset += len(data)
        if offset >= total:
            break

    return dict(all_rows)


def _get_eia_group_data(
    route: str, facet_key: str, frequency: str, timeout: int = 30,
) -> dict[tuple[str, str], list[dict]]:
    """Fetch an EIA group with caching."""
    cache_key = f"{route}|{facet_key}|{frequency}"
    members = _GROUP_INDEX[(route, facet_key, frequency)]

    def _fetch() -> dict[tuple[str, str], list[dict]]:
        result = _fetch_eia_group(route, facet_key, frequency, members, timeout)
        log.info("EIA batch %s: fetched %d series", cache_key, len(result))
        return result

    return _eia_cache.get_or_fetch(cache_key, _fetch)


def _make_eia_collector(
    route: str, facet_key: str, facet_value: str,
    data_col: str, frequency: str,
    name: str, display_name: str,
    domain: str, category: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://www.eia.gov/opendata/",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            group_data = _get_eia_group_data(
                route, facet_key, frequency,
                timeout=self.config.request_timeout,
            )
            rows = group_data.get((facet_value, data_col), [])
            if not rows:
                raise RuntimeError(f"No EIA data for {name} ({facet_value})")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"EIA_{name}"
    _Collector.__qualname__ = f"EIA_{name}"
    return _Collector


def get_eia_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for route, fk, fv, dc, freq, name, display, domain, cat in EIA_SERIES:
        collectors[name] = _make_eia_collector(
            route, fk, fv, dc, freq, name, display, domain, cat,
        )
    return collectors
