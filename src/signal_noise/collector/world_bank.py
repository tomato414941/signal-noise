from __future__ import annotations

import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector.worldbank_generic import _fetch_worldbank_json

# (indicator_code, collector_name, display_name)
# WLD = World aggregate
WB_INDICATORS: list[tuple[str, str, str]] = [
    ("NY.GDP.MKTP.KD.ZG", "wb_gdp_growth", "World Bank: Global GDP Growth %"),
    ("SP.POP.GROW", "wb_population_growth", "World Bank: Global Population Growth %"),
    ("IT.NET.USER.ZS", "wb_internet_users_pct", "World Bank: Internet Users %"),
    ("SE.ADT.LITR.ZS", "wb_literacy_rate", "World Bank: Adult Literacy Rate %"),
    ("SL.UEM.TOTL.ZS", "wb_unemployment_rate", "World Bank: Global Unemployment %"),
    ("FP.CPI.TOTL.ZG", "wb_inflation_cpi", "World Bank: Global CPI Inflation %"),
    ("EG.USE.ELEC.KH.PC", "wb_electricity_per_capita", "World Bank: Electricity Use per Capita (kWh)"),
]


def _make_wb_collector(
    indicator: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url="https://datahelpdesk.worldbank.org/knowledgebase/articles/889392",
            domain="economy",
            category="economic",
        )

        def fetch(self) -> pd.DataFrame:
            url = (
                f"https://api.worldbank.org/v2/country/WLD/indicator/{indicator}"
                f"?date=2000:2025&format=json&per_page=100"
            )
            result = _fetch_worldbank_json(url, timeout=self.config.request_timeout)
            if not isinstance(result, list) or len(result) < 2:
                raise RuntimeError(f"Unexpected WB response for {indicator}")
            entries = result[1]
            rows = []
            for entry in entries:
                if entry.get("value") is None:
                    continue
                try:
                    year = int(entry["date"])
                    val = float(entry["value"])
                    date = pd.Timestamp(year=year, month=7, day=1, tz="UTC")
                    rows.append({"date": date, "value": val})
                except (ValueError, TypeError, KeyError):
                    continue
            if not rows:
                raise RuntimeError(f"No World Bank data for {indicator}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"WB_{name}"
    _Collector.__qualname__ = f"WB_{name}"
    return _Collector


def get_wb_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_wb_collector(*t) for t in WB_INDICATORS}
