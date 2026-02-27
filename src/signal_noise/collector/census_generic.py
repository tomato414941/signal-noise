"""US Census Bureau economic indicators.

No API key required for basic access.
Docs: https://www.census.gov/data/developers/data-sets.html

The Census EITS API requires all standard fields in the `get` parameter
and does not support server-side filtering via query predicates (returns
204 No Content).  We fetch all rows for each endpoint and filter in Python.
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://api.census.gov/data/timeseries"
_GET_FIELDS = "data_type_code,time_slot_id,seasonally_adj,category_code,cell_value,error_data"

# (endpoint, data_type_code, category_code, sa_filter, collector_name, display_name, frequency, domain, category)
CENSUS_SERIES: list[tuple[str, str, str, str, str, str, str, str, str]] = [
    # ── Housing ────────────────────────────────────────────────
    ("/eits/resconst", "TOTAL", "APERMITS", "yes",
     "census_building_permits", "Census Building Permits", "monthly", "macro", "economic"),
    ("/eits/resconst", "TOTAL", "ASTARTS", "yes",
     "census_housing_starts", "Census Housing Starts", "monthly", "macro", "economic"),
    ("/eits/resconst", "TOTAL", "ACOMPLETIONS", "yes",
     "census_housing_completions", "Census Housing Completions", "monthly", "macro", "economic"),

    # ── New home sales ─────────────────────────────────────────
    ("/eits/ressales", "TOTAL", "ASOLD", "yes",
     "census_new_home_sales", "Census New Home Sales", "monthly", "macro", "economic"),
    ("/eits/ressales", "TOTAL", "FORSALE", "yes",
     "census_homes_for_sale", "Census New Homes For Sale", "monthly", "macro", "economic"),
    ("/eits/ressales", "AVERAG", "SOLD", "no",
     "census_avg_home_price", "Census Avg New Home Price", "monthly", "real_estate", "real_estate"),
    ("/eits/ressales", "MEDIAN", "SOLD", "no",
     "census_median_home_price", "Census Median New Home Price", "monthly", "real_estate", "real_estate"),

    # ── Retail & food services ─────────────────────────────────
    ("/eits/marts", "SM", "44X72", "yes",
     "census_retail_total", "Census Retail & Food Sales", "monthly", "macro", "economic"),
    ("/eits/marts", "SM", "441", "yes",
     "census_auto_sales", "Census Motor Vehicle Sales", "monthly", "macro", "economic"),
    ("/eits/marts", "SM", "454", "yes",
     "census_ecommerce", "Census E-Commerce Sales", "monthly", "macro", "economic"),
    ("/eits/marts", "SM", "722", "yes",
     "census_food_services", "Census Food Services Sales", "monthly", "macro", "economic"),
    ("/eits/marts", "SM", "452", "yes",
     "census_general_merch", "Census General Merchandise Sales", "monthly", "macro", "economic"),

    # ── Trade ──────────────────────────────────────────────────
    ("/eits/ftd", "IMP", "BOPGS", "yes",
     "census_imports_total", "Census Total Imports", "monthly", "macro", "trade"),
    ("/eits/ftd", "EXP", "BOPGS", "yes",
     "census_exports_total", "Census Total Exports", "monthly", "macro", "trade"),

    # ── Construction spending ──────────────────────────────────
    ("/eits/vip", "T", "AXXXX", "yes",
     "census_construction_spend", "Census Construction Spending", "monthly", "macro", "economic"),
    ("/eits/vip", "P", "AXXXX", "yes",
     "census_private_construction", "Census Private Construction", "monthly", "macro", "economic"),
    ("/eits/vip", "V", "AXXXX", "yes",
     "census_public_construction", "Census Public Construction", "monthly", "macro", "economic"),

    # ── Business inventories ───────────────────────────────────
    ("/eits/mtis", "IM", "MNFCTR", "yes",
     "census_mfg_inventories", "Census Manufacturing Inventories", "monthly", "macro", "economic"),
    ("/eits/mtis", "IM", "WHLSLR", "yes",
     "census_wholesale_inventories", "Census Wholesale Inventories", "monthly", "macro", "economic"),
    ("/eits/mtis", "IM", "RETAIL", "yes",
     "census_retail_inventories", "Census Retail Inventories", "monthly", "macro", "economic"),

    # ── Wholesale trade ────────────────────────────────────────
    ("/eits/mwts", "SM", "42", "yes",
     "census_wholesale_sales", "Census Wholesale Sales", "monthly", "macro", "economic"),
]


def _make_census_collector(
    endpoint: str, dt_code: str, cat_code: str, sa_filter: str,
    name: str, display_name: str,
    frequency: str, domain: str, category: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://www.census.gov/data/developers/data-sets.html",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            url = f"{_BASE_URL}{endpoint}?get={_GET_FIELDS}&time=from+2015"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            if not data or len(data) < 2:
                raise RuntimeError(f"No Census data for {name}")

            header = data[0]
            dt_idx = header.index("data_type_code")
            cat_idx = header.index("category_code")
            sa_idx = header.index("seasonally_adj")
            val_idx = header.index("cell_value")
            time_idx = header.index("time")

            rows = []
            for row in data[1:]:
                if row[dt_idx] != dt_code or row[cat_idx] != cat_code:
                    continue
                if row[sa_idx] != sa_filter:
                    continue
                try:
                    val = float(row[val_idx])
                    period = str(row[time_idx])
                    if "-" in period and len(period) == 7:
                        dt_str = f"{period}-01"
                    elif len(period) == 6:
                        dt_str = f"{period[:4]}-{period[4:]}-01"
                    elif len(period) == 4:
                        dt_str = f"{period}-01-01"
                    else:
                        continue
                    rows.append({
                        "date": pd.to_datetime(dt_str, utc=True),
                        "value": val,
                    })
                except (ValueError, TypeError, IndexError):
                    continue

            if not rows:
                raise RuntimeError(f"No Census data for {name}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Census_{name}"
    _Collector.__qualname__ = f"Census_{name}"
    return _Collector


def get_census_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for endpoint, dt_code, cat_code, sa, name, display, freq, domain, cat in CENSUS_SERIES:
        collectors[name] = _make_census_collector(
            endpoint, dt_code, cat_code, sa, name, display, freq, domain, cat,
        )
    return collectors
