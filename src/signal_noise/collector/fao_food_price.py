"""FAO Food Price Index collectors.

Monthly indices covering food, cereals, meat, dairy, oils, sugar.
Data from FAO STAT via CSV download. No authentication required.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_FAO_CSV_URL = (
    "https://www.fao.org/fileadmin/templates/worldfood/Reports_and_docs"
    "/Food_price_indices_data_nov.csv"
)

_SERIES = [
    ("Food Price Index", "fao_food_price_index", "FAO Food Price Index"),
    ("Meat", "fao_meat_price", "FAO Meat Price Index"),
    ("Dairy", "fao_dairy_price", "FAO Dairy Price Index"),
    ("Cereals", "fao_cereal_price", "FAO Cereals Price Index"),
    ("Oils", "fao_oils_price", "FAO Oils Price Index"),
    ("Sugar", "fao_sugar_price", "FAO Sugar Price Index"),
]


def _make_fao_collector(
    column: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="monthly",
            api_docs_url="https://www.fao.org/worldfoodsituation/foodpricesindex/en/",
            domain="food",
            category="food_price",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(_FAO_CSV_URL, timeout=60)
            resp.raise_for_status()
            from io import StringIO
            raw = pd.read_csv(StringIO(resp.text), skiprows=2)
            raw.columns = raw.columns.str.strip()
            if "Date" not in raw.columns:
                first_col = raw.columns[0]
                raw = raw.rename(columns={first_col: "Date"})
            target_col = None
            for c in raw.columns:
                if column.lower() in c.lower():
                    target_col = c
                    break
            if target_col is None:
                raise RuntimeError(f"Column '{column}' not found in FAO CSV")
            df = raw[["Date", target_col]].dropna().copy()
            df.columns = ["date", "value"]
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna()
            df["date"] = pd.to_datetime(df["date"], utc=True)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"FAO_{name}"
    _Collector.__qualname__ = f"FAO_{name}"
    return _Collector


def get_fao_food_price_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_fao_collector(column, name, display)
        for column, name, display in _SERIES
    }
