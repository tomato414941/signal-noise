"""Eurostat collectors via Statistics JSON API v1.

No API key required.  Docs: https://ec.europa.eu/eurostat/api/
Uses the compact Statistics API (v1) instead of the SDMX endpoint
for faster responses and smaller payloads.
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._utils import build_timeseries_df

_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# (dataset_code, filter_params, collector_name, display_name, frequency, domain, category)
EUROSTAT_SERIES: list[tuple[str, str, str, str, str, str, str]] = [
    # ── GDP ────────────────────────────────────────────────────
    ("namq_10_gdp", "na_item=B1GQ&unit=CLV10_MEUR&geo=EU27_2020&s_adj=SCA&freq=Q",
     "eu_gdp_real", "Eurostat EU GDP (Real, SA)", "quarterly", "economy", "economic"),
    ("namq_10_gdp", "na_item=B1GQ&unit=CLV10_MEUR&geo=EA20&s_adj=SCA&freq=Q",
     "eu_gdp_ea", "Eurostat Euro Area GDP (Real)", "quarterly", "economy", "economic"),
    ("namq_10_gdp", "na_item=B1GQ&unit=CLV10_MEUR&geo=DE&s_adj=SCA&freq=Q",
     "eu_gdp_de", "Eurostat Germany GDP (Real)", "quarterly", "economy", "economic"),
    ("namq_10_gdp", "na_item=B1GQ&unit=CLV10_MEUR&geo=FR&s_adj=SCA&freq=Q",
     "eu_gdp_fr", "Eurostat France GDP (Real)", "quarterly", "economy", "economic"),
    ("namq_10_gdp", "na_item=B1GQ&unit=CLV10_MEUR&geo=IT&s_adj=SCA&freq=Q",
     "eu_gdp_it", "Eurostat Italy GDP (Real)", "quarterly", "economy", "economic"),
    ("namq_10_gdp", "na_item=B1GQ&unit=CLV10_MEUR&geo=ES&s_adj=SCA&freq=Q",
     "eu_gdp_es", "Eurostat Spain GDP (Real)", "quarterly", "economy", "economic"),

    # ── HICP (Harmonized CPI) ─────────────────────────────────
    ("prc_hicp_midx", "coicop=CP00&unit=I15&geo=EU27_2020",
     "eu_hicp_all", "Eurostat EU HICP All Items", "monthly", "economy", "inflation"),
    ("prc_hicp_midx", "coicop=CP00&unit=I15&geo=EA20",
     "eu_hicp_ea", "Eurostat Euro Area HICP", "monthly", "economy", "inflation"),
    ("prc_hicp_midx", "coicop=TOT_X_NRG_FOOD&unit=I15&geo=EU27_2020",
     "eu_hicp_core", "Eurostat EU Core HICP", "monthly", "economy", "inflation"),
    ("prc_hicp_midx", "coicop=CP00&unit=I15&geo=DE",
     "eu_hicp_de", "Eurostat Germany HICP", "monthly", "economy", "inflation"),
    ("prc_hicp_midx", "coicop=CP00&unit=I15&geo=FR",
     "eu_hicp_fr", "Eurostat France HICP", "monthly", "economy", "inflation"),

    # ── Unemployment ───────────────────────────────────────────
    ("une_rt_m", "age=TOTAL&sex=T&unit=PC_ACT&s_adj=SA&geo=EU27_2020",
     "eu_unemp_total", "Eurostat EU Unemployment Rate", "monthly", "economy", "labor"),
    ("une_rt_m", "age=TOTAL&sex=T&unit=PC_ACT&s_adj=SA&geo=EA20",
     "eu_unemp_ea", "Eurostat Euro Area Unemployment", "monthly", "economy", "labor"),
    ("une_rt_m", "age=TOTAL&sex=T&unit=PC_ACT&s_adj=SA&geo=DE",
     "eu_unemp_de", "Eurostat Germany Unemployment", "monthly", "economy", "labor"),
    ("une_rt_m", "age=TOTAL&sex=T&unit=PC_ACT&s_adj=SA&geo=FR",
     "eu_unemp_fr", "Eurostat France Unemployment", "monthly", "economy", "labor"),
    ("une_rt_m", "age=TOTAL&sex=T&unit=PC_ACT&s_adj=SA&geo=ES",
     "eu_unemp_es", "Eurostat Spain Unemployment", "monthly", "economy", "labor"),
    ("une_rt_m", "age=Y_LT25&sex=T&unit=PC_ACT&s_adj=SA&geo=EU27_2020",
     "eu_unemp_youth", "Eurostat EU Youth Unemployment", "monthly", "economy", "labor"),

    # ── Industrial production ─────────────────────────────────
    ("sts_inpr_m", "nace_r2=B-D&unit=I15&s_adj=SCA&geo=EU27_2020",
     "eu_indprod", "Eurostat EU Industrial Production", "monthly", "economy", "economic"),
    ("sts_inpr_m", "nace_r2=B-D&unit=I15&s_adj=SCA&geo=DE",
     "eu_indprod_de", "Eurostat Germany Industrial Production", "monthly", "economy", "economic"),
    ("sts_inpr_m", "nace_r2=C&unit=I15&s_adj=SCA&geo=EU27_2020",
     "eu_mfg_prod", "Eurostat EU Manufacturing Production", "monthly", "economy", "economic"),

    # ── Retail trade ───────────────────────────────────────────
    ("sts_trtu_m", "nace_r2=G47&unit=I15&s_adj=SCA&geo=EU27_2020",
     "eu_retail", "Eurostat EU Retail Trade Volume", "monthly", "economy", "economic"),
    ("sts_trtu_m", "nace_r2=G47&unit=I15&s_adj=SCA&geo=EA20",
     "eu_retail_ea", "Eurostat Euro Area Retail Trade", "monthly", "economy", "economic"),

    # ── Trade balance (monthly, ratio) ────────────────────────
    ("ext_st_eu27_2020sitc", "partner=EXT_EU27_2020&sitc06=TOTAL&stk_flow=BAL_RT",
     "eu_trade_balance", "Eurostat EU Trade Balance Ratio", "monthly", "economy", "trade"),

    # ── Construction ───────────────────────────────────────────
    ("sts_copr_m", "nace_r2=F&unit=I15&s_adj=SCA&geo=EU27_2020",
     "eu_construction", "Eurostat EU Construction Output", "monthly", "economy", "economic"),

    # ── Business confidence ────────────────────────────────────
    ("ei_bsin_m_r2", "indic=BS-ICI&s_adj=SA&geo=EU27_2020",
     "eu_business_conf", "Eurostat EU Business Confidence", "monthly", "sentiment", "sentiment"),
    ("ei_bsco_m", "indic=BS-CSMCI&s_adj=SA&geo=EU27_2020",
     "eu_consumer_conf", "Eurostat EU Consumer Confidence", "monthly", "sentiment", "sentiment"),

    # ── Energy ─────────────────────────────────────────────────
    ("nrg_cb_oilm", "siec=O4100_TOT&nrg_bal=IMP&geo=EU27_2020",
     "eu_oil_supply", "Eurostat EU Crude Oil Imports", "monthly", "economy", "economic"),
    ("nrg_cb_gasm", "siec=G3000&nrg_bal=IC_OBS&unit=TJ_GCV&geo=EU27_2020",
     "eu_natgas_supply", "Eurostat EU Natural Gas Consumption", "monthly", "economy", "economic"),

    # ── Government debt ────────────────────────────────────────
    ("gov_10q_ggdebt", "sector=S13&unit=PC_GDP&na_item=GD&geo=EU27_2020&freq=Q",
     "eu_govt_debt", "Eurostat EU Govt Debt/GDP", "quarterly", "economy", "fiscal"),
    ("gov_10q_ggdebt", "sector=S13&unit=PC_GDP&na_item=GD&geo=EA20&freq=Q",
     "eu_govt_debt_ea", "Eurostat Euro Area Govt Debt/GDP", "quarterly", "economy", "fiscal"),
    ("gov_10q_ggdebt", "sector=S13&unit=PC_GDP&na_item=GD&geo=DE&freq=Q",
     "eu_govt_debt_de", "Eurostat Germany Govt Debt/GDP", "quarterly", "economy", "fiscal"),
    ("gov_10q_ggdebt", "sector=S13&unit=PC_GDP&na_item=GD&geo=FR&freq=Q",
     "eu_govt_debt_fr", "Eurostat France Govt Debt/GDP", "quarterly", "economy", "fiscal"),
    ("gov_10q_ggdebt", "sector=S13&unit=PC_GDP&na_item=GD&geo=IT&freq=Q",
     "eu_govt_debt_it", "Eurostat Italy Govt Debt/GDP", "quarterly", "economy", "fiscal"),

    # ── Exchange rate ──────────────────────────────────────────
    ("ert_bil_eur_m", "currency=USD&statinfo=AVG",
     "eu_eurusd", "Eurostat EUR/USD Rate", "monthly", "markets", "forex"),
    ("ert_bil_eur_m", "currency=GBP&statinfo=AVG",
     "eu_eurgbp", "Eurostat EUR/GBP Rate", "monthly", "markets", "forex"),
    ("ert_bil_eur_m", "currency=JPY&statinfo=AVG",
     "eu_eurjpy", "Eurostat EUR/JPY Rate", "monthly", "markets", "forex"),
    ("ert_bil_eur_m", "currency=CHF&statinfo=AVG",
     "eu_eurchf", "Eurostat EUR/CHF Rate", "monthly", "markets", "forex"),
]


def _parse_period(period: str) -> str | None:
    """Convert Eurostat period string to ISO date."""
    # Monthly: "2024-01"
    if len(period) == 7 and period[4] == "-" and period[5:].isdigit():
        return f"{period}-01"
    # Quarterly: "2024Q1" or "2024-Q1"
    if len(period) == 6 and period[4] == "Q":
        q = int(period[5])
        m = (q - 1) * 3 + 1
        return f"{period[:4]}-{m:02d}-01"
    if len(period) == 7 and period[4:6] == "-Q":
        q = int(period[6])
        m = (q - 1) * 3 + 1
        return f"{period[:4]}-{m:02d}-01"
    # Annual: "2024"
    if len(period) == 4:
        return f"{period}-01-01"
    return None


def _make_eurostat_collector(
    dataset: str, params: str,
    name: str, display_name: str,
    frequency: str, domain: str, category: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://ec.europa.eu/eurostat/api/",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            url = f"{_BASE_URL}/{dataset}?{params}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            # Eurostat JSON format: dimension indices + values
            dims = data.get("dimension", {})
            time_dim = dims.get("time", {}).get("category", {}).get("index", {})
            values = data.get("value", {})

            if not time_dim or not values:
                raise RuntimeError(f"No Eurostat data for {name}")

            # Build index-to-period mapping
            sorted_periods = sorted(time_dim.items(), key=lambda x: x[1])

            rows = []
            for period, idx in sorted_periods:
                val = values.get(str(idx))
                if val is None:
                    continue
                dt = _parse_period(period)
                if dt is None:
                    continue
                try:
                    rows.append({
                        "date": pd.to_datetime(dt, utc=True),
                        "value": float(val),
                    })
                except (ValueError, TypeError):
                    continue

            return build_timeseries_df(rows, f"Eurostat {name}")

    _Collector.__name__ = f"Eurostat_{name}"
    _Collector.__qualname__ = f"Eurostat_{name}"
    return _Collector


def get_eurostat_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for dataset, params, name, display, freq, domain, cat in EUROSTAT_SERIES:
        collectors[name] = _make_eurostat_collector(
            dataset, params, name, display, freq, domain, cat,
        )
    return collectors
