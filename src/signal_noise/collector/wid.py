from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://wid.world/api/"

# WID variable codes:
# sptinc_p99p100_z = pre-tax national income, top 1% share (%)
# sptinc_p90p100_z = pre-tax national income, top 10% share (%)
# sptinc_p0p50_z = pre-tax national income, bottom 50% share (%)
# shweal_p99p100_z = net personal wealth, top 1% share (%)

# (indicator, percentile, area_codes, name_prefix, display_prefix, domain, category)
_WID_SERIES: list[tuple[str, dict[str, str], str, str]] = [
    # Top 1% income share
    (
        "sptinc_p99p100_z",
        {"US": "us", "CN": "cn", "GB": "gb", "FR": "fr",
         "DE": "de", "JP": "jp", "IN": "in", "BR": "br"},
        "wid_top1_income",
        "WID Top 1% Income Share",
    ),
    # Bottom 50% income share
    (
        "sptinc_p0p50_z",
        {"US": "us", "CN": "cn", "GB": "gb", "FR": "fr",
         "DE": "de", "JP": "jp", "IN": "in", "BR": "br"},
        "wid_bottom50_income",
        "WID Bottom 50% Income Share",
    ),
    # Top 1% wealth share
    (
        "shweal_p99p100_z",
        {"US": "us", "GB": "gb", "FR": "fr", "DE": "de"},
        "wid_top1_wealth",
        "WID Top 1% Wealth Share",
    ),
]


def _make_wid_collector(
    indicator: str, area: str, suffix: str,
    name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url="https://wid.world/data/",
            domain="economy",
            category="inequality",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                _API_URL,
                params={
                    "indicators": indicator,
                    "areas": area,
                    "years": "all",
                },
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data:
                raise RuntimeError(f"No WID data for {indicator}/{area}")

            # WID returns: {area: {indicator: {year: value, ...}}}
            rows = []
            area_data = data.get(area, {})
            indicator_data = area_data.get(indicator, {})

            if isinstance(indicator_data, dict):
                for year_str, val in indicator_data.items():
                    if val is not None:
                        rows.append({
                            "date": pd.to_datetime(f"{year_str}-01-01", utc=True),
                            "value": float(val),
                        })
            elif isinstance(data, dict):
                # Fallback: try flat structure
                for key, val_dict in data.items():
                    if isinstance(val_dict, dict):
                        for sub_key, val in val_dict.items():
                            if isinstance(val, dict):
                                for year_str, v in val.items():
                                    if v is not None:
                                        rows.append({
                                            "date": pd.to_datetime(
                                                f"{year_str}-01-01", utc=True,
                                            ),
                                            "value": float(v),
                                        })

            if not rows:
                raise RuntimeError(f"No WID data for {indicator}/{area}")

            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"WID_{name}"
    _Collector.__qualname__ = f"WID_{name}"
    return _Collector


def get_wid_collectors() -> dict[str, type[BaseCollector]]:
    collectors = {}
    for indicator, countries, name_prefix, display_prefix in _WID_SERIES:
        for area, suffix in countries.items():
            name = f"{name_prefix}_{suffix}"
            display = f"{display_prefix}: {area}"
            cls = _make_wid_collector(indicator, area, suffix, name, display)
            collectors[name] = cls
    return collectors
