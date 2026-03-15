"""EIA (U.S. Energy Information Administration) collectors.

Uses EIA API v2.  Requires EIA_API_KEY environment variable.
Free key: https://www.eia.gov/opendata/register.php

Collectors sharing the same route/facet/frequency are batched into a single
request and cached via SharedAPICache to minimize API calls.
"""
from __future__ import annotations

import logging
from collections import defaultdict

import pandas as pd
import requests

from signal_noise.collector._auth import load_secret
from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._utils import build_timeseries_df

log = logging.getLogger(__name__)

_eia_cache = SharedAPICache(ttl=3600)  # cache for 1 hour


def _get_eia_key() -> str:
    return load_secret("eia", "EIA_API_KEY",
                       signup_url="https://www.eia.gov/opendata/register.php")


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
     "eia_elec_price_total", "EIA US Electricity Price (All)", "markets", "commodity",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "RES", "price", "monthly",
     "eia_elec_price_res", "EIA US Electricity Price (Residential)", "markets", "commodity",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "COM", "price", "monthly",
     "eia_elec_price_com", "EIA US Electricity Price (Commercial)", "markets", "commodity",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "IND", "price", "monthly",
     "eia_elec_price_ind", "EIA US Electricity Price (Industrial)", "markets", "commodity",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "ALL", "revenue", "monthly",
     "eia_elec_revenue", "EIA US Electricity Revenue (All)", "economy", "economic",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "ALL", "sales", "monthly",
     "eia_elec_sales", "EIA US Electricity Sales (All)", "economy", "economic",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "RES", "sales", "monthly",
     "eia_elec_sales_res", "EIA US Electricity Sales (Residential)", "economy", "economic",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "COM", "sales", "monthly",
     "eia_elec_sales_com", "EIA US Electricity Sales (Commercial)", "economy", "economic",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "IND", "sales", "monthly",
     "eia_elec_sales_ind", "EIA US Electricity Sales (Industrial)", "economy", "economic",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "RES", "revenue", "monthly",
     "eia_elec_revenue_res", "EIA US Electricity Revenue (Residential)", "economy", "economic",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "COM", "revenue", "monthly",
     "eia_elec_revenue_com", "EIA US Electricity Revenue (Commercial)", "economy", "economic",
     {"stateid": "US"}),
    ("electricity/retail-sales/data", "sectorid", "IND", "revenue", "monthly",
     "eia_elec_revenue_ind", "EIA US Electricity Revenue (Industrial)", "economy", "economic",
     {"stateid": "US"}),

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
    ("steo/data", "seriesId", "COPRPUS", "value", "monthly",
     "eia_steo_us_crude_prod", "EIA STEO U.S. Crude Oil Production", "economy", "economic"),
    ("steo/data", "seriesId", "NGMPPUS", "value", "monthly",
     "eia_steo_natgas_marketed_prod", "EIA STEO Natural Gas Marketed Production", "economy", "economic"),
    ("steo/data", "seriesId", "NGNWPUS", "value", "monthly",
     "eia_steo_natgas_net_withdrawals", "EIA STEO Natural Gas Net Withdrawals", "markets", "commodity"),
    ("steo/data", "seriesId", "EPEOPUS", "value", "monthly",
     "eia_steo_power_generation", "EIA STEO Electric Power Generation", "economy", "economic"),

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

    # ── Renewable energy ─────────────────────────────────────
    ("total-energy/data", "msn", "SOTCBUS", "value", "monthly",
     "eia_solar_generation", "EIA US Solar Generation", "economy", "energy"),
    ("total-energy/data", "msn", "WYTCBUS", "value", "monthly",
     "eia_wind_generation", "EIA US Wind Generation", "economy", "energy"),
    ("total-energy/data", "msn", "GETCBUS", "value", "monthly",
     "eia_geothermal_generation", "EIA US Geothermal Generation", "economy", "energy"),
    ("total-energy/data", "msn", "HYTCPUS", "value", "monthly",
     "eia_hydro_generation", "EIA US Hydro Generation", "economy", "energy"),

    # ── State-level electricity prices ───────────────────────
    ("electricity/retail-sales/data", "sectorid", "ALL", "price", "monthly",
     "eia_elec_price_ca", "EIA Electricity Price: California", "markets", "commodity",
     {"stateid": "CA"}),
    ("electricity/retail-sales/data", "sectorid", "ALL", "price", "monthly",
     "eia_elec_price_tx", "EIA Electricity Price: Texas", "markets", "commodity",
     {"stateid": "TX"}),
    ("electricity/retail-sales/data", "sectorid", "ALL", "price", "monthly",
     "eia_elec_price_ny", "EIA Electricity Price: New York", "markets", "commodity",
     {"stateid": "NY"}),
    ("electricity/retail-sales/data", "sectorid", "ALL", "price", "monthly",
     "eia_elec_price_fl", "EIA Electricity Price: Florida", "markets", "commodity",
     {"stateid": "FL"}),

    # ── STEO additional ──────────────────────────────────────
    ("steo/data", "seriesId", "RETCBUS", "value", "monthly",
     "eia_steo_renewable_total", "EIA STEO US Renewable Energy Total", "economy", "energy"),
    ("steo/data", "seriesId", "SOWGPUS", "value", "monthly",
     "eia_steo_solar_generation", "EIA STEO Solar Generation Forecast", "economy", "energy"),
    ("steo/data", "seriesId", "WNDGPUS", "value", "monthly",
     "eia_steo_wind_generation", "EIA STEO Wind Generation Forecast", "economy", "energy"),
]

def _extra_facet_key(extra_facets: dict[str, str] | None) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((extra_facets or {}).items()))


def _unpack_series_spec(
    spec: tuple,
) -> tuple[str, str, str, str, str, str, str, str, str, dict[str, str] | None]:
    route, facet_key, facet_value, data_col, frequency, name, display, domain, category, *rest = spec
    extra_facets = rest[0] if rest else None
    return (
        route,
        facet_key,
        facet_value,
        data_col,
        frequency,
        name,
        display,
        domain,
        category,
        extra_facets,
    )


# Build group index: (route, facet_key, frequency, extra_facets) -> [(facet_value, data_col, name), ...]
_GROUP_INDEX: dict[tuple[str, str, str, tuple[tuple[str, str], ...]], list[tuple[str, str, str]]] = defaultdict(list)
for spec in EIA_SERIES:
    _r, _fk, _fv, _dc, _freq, _name, *_rest, _extra = _unpack_series_spec(spec)
    _GROUP_INDEX[(_r, _fk, _freq, _extra_facet_key(_extra))].append((_fv, _dc, _name))

# Lookup: collector_name -> (route, facet_key, facet_value, data_col, frequency, extra_facets)
_SERIES_LOOKUP: dict[str, tuple[str, str, str, str, str, dict[str, str] | None]] = {
    spec[5]: (
        spec[0], spec[1], spec[2], spec[3], spec[4],
        spec[9] if len(spec) > 9 else None,
    )
    for spec in EIA_SERIES
}


def _fetch_eia_group(
    route: str, facet_key: str, frequency: str,
    members: list[tuple[str, str, str]],
    extra_facets: dict[str, str] | None = None,
    timeout: int = 30,
) -> dict[str, list[dict]]:
    """Fetch multiple EIA series sharing the same route/facet/frequency.

    Returns {(facet_value, data_col): [rows]} mapping.
    """
    api_key = _get_eia_key()
    url = f"{_BASE_URL}/{route}"

    facet_values = list({fv for fv, _, _ in members})
    data_cols = list({dc for _, dc, _ in members})

    # Limit to last 3 years to avoid fetching entire history (slow from EU)
    start_date = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 3)).strftime("%Y-%m-%d")

    params: list[tuple[str, str]] = [
        ("api_key", api_key),
        ("frequency", frequency),
        ("start", start_date),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("length", "5000"),
    ]
    for fv in facet_values:
        params.append((f"facets[{facet_key}][]", fv))
    for extra_key, extra_value in (extra_facets or {}).items():
        params.append((f"facets[{extra_key}][]", extra_value))
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
    route: str,
    facet_key: str,
    frequency: str,
    extra_facets: dict[str, str] | None = None,
    timeout: int = 30,
) -> dict[tuple[str, str], list[dict]]:
    """Fetch an EIA group with caching."""
    extra_key = _extra_facet_key(extra_facets)
    cache_key = f"{route}|{facet_key}|{frequency}|{extra_key}"
    members = _GROUP_INDEX[(route, facet_key, frequency, extra_key)]

    def _fetch() -> dict[tuple[str, str], list[dict]]:
        result = _fetch_eia_group(
            route,
            facet_key,
            frequency,
            members,
            extra_facets,
            timeout,
        )
        log.info("EIA batch %s: fetched %d series", cache_key, len(result))
        return result

    return _eia_cache.get_or_fetch(cache_key, _fetch)


def _make_eia_collector(
    route: str, facet_key: str, facet_value: str,
    data_col: str, frequency: str,
    name: str, display_name: str,
    domain: str, category: str,
    extra_facets: dict[str, str] | None = None,
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
                extra_facets,
                timeout=self.config.request_timeout,
            )
            rows = group_data.get((facet_value, data_col), [])
            return build_timeseries_df(rows, f"EIA {name} ({facet_value})")

    _Collector.__name__ = f"EIA_{name}"
    _Collector.__qualname__ = f"EIA_{name}"
    return _Collector


def get_eia_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for spec in EIA_SERIES:
        route, fk, fv, dc, freq, name, display, domain, cat, extra = _unpack_series_spec(spec)
        collectors[name] = _make_eia_collector(
            route, fk, fv, dc, freq, name, display, domain, cat, extra,
        )
    return collectors
