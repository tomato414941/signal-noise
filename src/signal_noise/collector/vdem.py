from __future__ import annotations

from io import StringIO

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

# V-Dem data via GitHub (vdeminstitute publishes CSV on their data portal).
# We use the Our World in Data (OWID) GitHub-hosted CSVs for specific indices,
# which are smaller and more reliable than downloading the full V-Dem dataset.
#
# OWID catalog endpoint for V-Dem electoral democracy index (v2x_polyarchy):
# https://ourworldindata.org/grapher/electoral-democracy-index

_OWID_BASE = "https://ourworldindata.org/grapher"

# (slug, variable, countries, name_prefix, display_prefix, description)
_VDEM_INDICATORS = [
    (
        "electoral-democracy-index",
        "electdem_vdem_owid",
        {"USA": "us", "CHN": "cn", "JPN": "jp", "RUS": "ru",
         "BRA": "br", "IND": "in", "DEU": "de", "GBR": "gb"},
        "vdem_democracy",
        "V-Dem Electoral Democracy",
    ),
    (
        "human-rights-index-vdem",
        "human_rights_score",
        {"USA": "us", "CHN": "cn", "JPN": "jp", "RUS": "ru",
         "BRA": "br", "IND": "in"},
        "vdem_human_rights",
        "V-Dem Human Rights Score",
    ),
]


def _fetch_owid_csv(slug: str) -> pd.DataFrame:
    url = f"{_OWID_BASE}/{slug}.csv"
    resp = requests.get(
        url,
        headers={"Accept": "text/csv", "User-Agent": "signal-noise/0.1"},
        timeout=60,
    )
    resp.raise_for_status()
    text = resp.text
    if text.strip().startswith("<!") or text.strip().startswith("<html"):
        raise RuntimeError(f"OWID returned HTML instead of CSV for {slug}")
    return pd.read_csv(StringIO(text))


def _make_vdem_collector(
    slug: str, variable: str, country_code: str, country_suffix: str,
    name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url=f"https://ourworldindata.org/grapher/{slug}",
            domain="society",
            category="governance",
        )

        def fetch(self) -> pd.DataFrame:
            raw = _fetch_owid_csv(slug)
            # OWID CSVs have columns: Entity, Code, Year, <variable>
            # Filter for target country
            mask = raw["Code"] == country_code
            filtered = raw.loc[mask].copy()
            if filtered.empty:
                raise RuntimeError(f"No V-Dem data for {country_code}")

            # Find the value column (last column that's not Entity/Code/Year)
            value_cols = [
                c for c in filtered.columns
                if c not in ("Entity", "Code", "Year", "Day")
            ]
            if not value_cols:
                raise RuntimeError(f"No value column in OWID data for {slug}")
            val_col = value_cols[0]

            rows = []
            for _, row in filtered.iterrows():
                val = row[val_col]
                if pd.notna(val):
                    rows.append({
                        "date": pd.to_datetime(f"{int(row['Year'])}-01-01", utc=True),
                        "value": float(val),
                    })

            if not rows:
                raise RuntimeError(f"No V-Dem data for {country_code}")

            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"VDEM_{name}"
    _Collector.__qualname__ = f"VDEM_{name}"
    return _Collector


def get_vdem_collectors() -> dict[str, type[BaseCollector]]:
    collectors = {}
    for slug, variable, countries, name_prefix, display_prefix in _VDEM_INDICATORS:
        for country_code, suffix in countries.items():
            name = f"{name_prefix}_{suffix}"
            display = f"{display_prefix}: {country_code}"
            cls = _make_vdem_collector(
                slug, variable, country_code, suffix, name, display,
            )
            collectors[name] = cls
    return collectors
