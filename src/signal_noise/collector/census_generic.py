"""US Census Bureau economic indicators.

No API key required for basic access.
Docs: https://www.census.gov/data/developers/data-sets.html
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://api.census.gov/data/timeseries"

# (endpoint, params, time_param, value_key, collector_name, display_name, frequency, domain, category)
CENSUS_SERIES: list[tuple[str, str, str, str, str, str, str, str, str]] = [
    # ── Housing ────────────────────────────────────────────────
    ("/eits/resconst", "category_code=TOTAL&data_type_code=PERMITS",
     "time", "cell_value", "census_building_permits", "Census Building Permits", "monthly", "macro", "economic"),
    ("/eits/resconst", "category_code=TOTAL&data_type_code=STARTS",
     "time", "cell_value", "census_housing_starts", "Census Housing Starts", "monthly", "macro", "economic"),
    ("/eits/resconst", "category_code=TOTAL&data_type_code=COMPLETIONS",
     "time", "cell_value", "census_housing_completions", "Census Housing Completions", "monthly", "macro", "economic"),

    # ── New home sales ─────────────────────────────────────────
    ("/eits/ressales", "category_code=SOLD&data_type_code=TOTAL",
     "time", "cell_value", "census_new_home_sales", "Census New Home Sales", "monthly", "macro", "economic"),
    ("/eits/ressales", "category_code=FORSALE&data_type_code=TOTAL",
     "time", "cell_value", "census_homes_for_sale", "Census New Homes For Sale", "monthly", "macro", "economic"),
    ("/eits/ressales", "category_code=AVGPRICE&data_type_code=TOTAL",
     "time", "cell_value", "census_avg_home_price", "Census Avg New Home Price", "monthly", "real_estate", "real_estate"),
    ("/eits/ressales", "category_code=MEDPRICE&data_type_code=TOTAL",
     "time", "cell_value", "census_median_home_price", "Census Median New Home Price", "monthly", "real_estate", "real_estate"),

    # ── Retail & food services ─────────────────────────────────
    ("/eits/marts", "category_code=44X72&data_type_code=SM",
     "time", "cell_value", "census_retail_total", "Census Retail & Food Sales", "monthly", "macro", "economic"),
    ("/eits/marts", "category_code=441&data_type_code=SM",
     "time", "cell_value", "census_auto_sales", "Census Motor Vehicle Sales", "monthly", "macro", "economic"),
    ("/eits/marts", "category_code=4541&data_type_code=SM",
     "time", "cell_value", "census_ecommerce", "Census E-Commerce Sales", "monthly", "macro", "economic"),
    ("/eits/marts", "category_code=722&data_type_code=SM",
     "time", "cell_value", "census_food_services", "Census Food Services Sales", "monthly", "macro", "economic"),
    ("/eits/marts", "category_code=452&data_type_code=SM",
     "time", "cell_value", "census_general_merch", "Census General Merchandise Sales", "monthly", "macro", "economic"),

    # ── Manufacturing ──────────────────────────────────────────
    ("/eits/advm", "category_code=MTM&data_type_code=MPCNO",
     "time", "cell_value", "census_durable_orders", "Census Durable Goods Orders", "monthly", "macro", "economic"),
    ("/eits/advm", "category_code=MTM&data_type_code=MPCNOXD",
     "time", "cell_value", "census_durable_ex_defense", "Census Durable Orders ex Defense", "monthly", "macro", "economic"),
    ("/eits/advm", "category_code=MTM&data_type_code=MPCNOXDT",
     "time", "cell_value", "census_durable_ex_transport", "Census Durable Orders ex Transport", "monthly", "macro", "economic"),
    ("/eits/advm", "category_code=MTM&data_type_code=MPCVNO",
     "time", "cell_value", "census_factory_orders", "Census Factory Orders", "monthly", "macro", "economic"),

    # ── Trade ──────────────────────────────────────────────────
    ("/eits/ftd", "COMM_LVL=ALL&CTY_CODE=0000&time=from+2015",
     "time", "IMP_VAL", "census_imports_total", "Census Total Imports", "monthly", "macro", "trade"),
    ("/eits/ftd", "COMM_LVL=ALL&CTY_CODE=0000&time=from+2015",
     "time", "EXP_VAL", "census_exports_total", "Census Total Exports", "monthly", "macro", "trade"),

    # ── Construction spending ──────────────────────────────────
    ("/eits/vip", "category_code=TOTAL&data_type_code=TOTAL",
     "time", "cell_value", "census_construction_spend", "Census Construction Spending", "monthly", "macro", "economic"),
    ("/eits/vip", "category_code=PRIVATE&data_type_code=TOTAL",
     "time", "cell_value", "census_private_construction", "Census Private Construction", "monthly", "macro", "economic"),
    ("/eits/vip", "category_code=PUBLIC&data_type_code=TOTAL",
     "time", "cell_value", "census_public_construction", "Census Public Construction", "monthly", "macro", "economic"),

    # ── Business inventories ───────────────────────────────────
    ("/eits/mtis", "category_code=MTM&data_type_code=SM",
     "time", "cell_value", "census_mfg_inventories", "Census Manufacturing Inventories", "monthly", "macro", "economic"),
    ("/eits/mtis", "category_code=WTM&data_type_code=SM",
     "time", "cell_value", "census_wholesale_inventories", "Census Wholesale Inventories", "monthly", "macro", "economic"),
    ("/eits/mtis", "category_code=RTM&data_type_code=SM",
     "time", "cell_value", "census_retail_inventories", "Census Retail Inventories", "monthly", "macro", "economic"),

    # ── Wholesale trade ────────────────────────────────────────
    ("/eits/mwts", "category_code=42TW&data_type_code=SM",
     "time", "cell_value", "census_wholesale_sales", "Census Wholesale Sales", "monthly", "macro", "economic"),
]


def _make_census_collector(
    endpoint: str, params: str, time_param: str, value_key: str,
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
            url = f"{_BASE_URL}{endpoint}?get={value_key},{time_param}&{params}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            if not data or len(data) < 2:
                raise RuntimeError(f"No Census data for {name}")

            rows = []
            header = data[0]
            val_idx = header.index(value_key) if value_key in header else 0
            time_idx = header.index(time_param) if time_param in header else 1

            for row in data[1:]:
                try:
                    val = float(row[val_idx])
                    period = str(row[time_idx])
                    # period format: YYYY-MM or YYYYMM
                    if "-" in period and len(period) == 7:
                        dt = f"{period}-01"
                    elif len(period) == 6:
                        dt = f"{period[:4]}-{period[4:]}-01"
                    elif len(period) == 4:
                        dt = f"{period}-01-01"
                    else:
                        continue
                    rows.append({
                        "date": pd.to_datetime(dt, utc=True),
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
    for endpoint, params, tp, vk, name, display, freq, domain, cat in CENSUS_SERIES:
        collectors[name] = _make_census_collector(
            endpoint, params, tp, vk, name, display, freq, domain, cat,
        )
    return collectors
