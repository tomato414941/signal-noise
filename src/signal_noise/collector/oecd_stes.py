"""OECD Short-Term Economic Statistics (STES) collectors.

CLI (Composite Leading Indicator) by country.
Uses the OECD SDMX API (CSV format).
No API key required.
"""
from __future__ import annotations

import io

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.SDD.STES,DSD_STES@DF_CLI,4.1"
)
_HEADERS = {"Accept": "application/vnd.sdmx.data+csv"}

# (country_code, collector_name, display_name)
OECD_CLI_SERIES: list[tuple[str, str, str]] = [
    ("USA", "oecd_cli_us", "OECD CLI: US"),
    ("JPN", "oecd_cli_jp", "OECD CLI: Japan"),
    ("DEU", "oecd_cli_de", "OECD CLI: Germany"),
    ("GBR", "oecd_cli_gb", "OECD CLI: UK"),
    ("FRA", "oecd_cli_fr", "OECD CLI: France"),
    ("ITA", "oecd_cli_it", "OECD CLI: Italy"),
    ("CAN", "oecd_cli_ca", "OECD CLI: Canada"),
    ("AUS", "oecd_cli_au", "OECD CLI: Australia"),
    ("KOR", "oecd_cli_kr", "OECD CLI: S.Korea"),
    ("CHN", "oecd_cli_cn", "OECD CLI: China"),
    ("IND", "oecd_cli_in", "OECD CLI: India"),
    ("BRA", "oecd_cli_br", "OECD CLI: Brazil"),
]


def _make_oecd_cli_collector(
    country: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="monthly",
            api_docs_url="https://data.oecd.org/leadind/composite-leading-indicator-cli.htm",
            domain="macro",
            category="economic",
        )

        def fetch(self) -> pd.DataFrame:
            url = (
                f"{_BASE_URL}/{country}.M.LI.IX._Z.AA.IX._Z.H"
                f"?startPeriod=2000-01"
            )
            resp = requests.get(url, headers=_HEADERS, timeout=60)
            resp.raise_for_status()
            raw = pd.read_csv(io.StringIO(resp.text))
            if raw.empty:
                raise RuntimeError(f"No OECD CLI data for {country}")

            rows = []
            for _, row in raw.iterrows():
                try:
                    period = str(row["TIME_PERIOD"])
                    val = float(row["OBS_VALUE"])
                    dt = pd.Timestamp(period + "-01", tz="UTC")
                    rows.append({"date": dt, "value": val})
                except (KeyError, ValueError, TypeError):
                    continue

            if not rows:
                raise RuntimeError(f"No parseable OECD CLI data for {country}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"OECD_CLI_{name}"
    _Collector.__qualname__ = f"OECD_CLI_{name}"
    return _Collector


def get_oecd_stes_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for country, name, display in OECD_CLI_SERIES:
        collectors[name] = _make_oecd_cli_collector(country, name, display)
    return collectors
