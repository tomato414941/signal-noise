from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (country_code, measure, collector_name, display_name, domain, category)
# Measures: HPI = nominal, RHP = real, HPI_YDH = price-to-income, HPI_RPI = price-to-rent
OECD_HP_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── Nominal HPI ──
    ("USA", "HPI", "oecd_hpi_us", "OECD Nominal HPI: US", "economy", "real_estate"),
    ("JPN", "HPI", "oecd_hpi_jp", "OECD Nominal HPI: Japan", "economy", "real_estate"),
    ("GBR", "HPI", "oecd_hpi_gb", "OECD Nominal HPI: UK", "economy", "real_estate"),
    ("DEU", "HPI", "oecd_hpi_de", "OECD Nominal HPI: Germany", "economy", "real_estate"),
    ("FRA", "HPI", "oecd_hpi_fr", "OECD Nominal HPI: France", "economy", "real_estate"),
    ("CAN", "HPI", "oecd_hpi_ca", "OECD Nominal HPI: Canada", "economy", "real_estate"),
    ("AUS", "HPI", "oecd_hpi_au", "OECD Nominal HPI: Australia", "economy", "real_estate"),
    ("KOR", "HPI", "oecd_hpi_kr", "OECD Nominal HPI: Korea", "economy", "real_estate"),
    ("CHN", "HPI", "oecd_hpi_cn", "OECD Nominal HPI: China", "economy", "real_estate"),
    ("ITA", "HPI", "oecd_hpi_it", "OECD Nominal HPI: Italy", "economy", "real_estate"),
    ("ESP", "HPI", "oecd_hpi_es", "OECD Nominal HPI: Spain", "economy", "real_estate"),
    ("NLD", "HPI", "oecd_hpi_nl", "OECD Nominal HPI: Netherlands", "economy", "real_estate"),
    ("CHE", "HPI", "oecd_hpi_ch", "OECD Nominal HPI: Switzerland", "economy", "real_estate"),
    ("SWE", "HPI", "oecd_hpi_se", "OECD Nominal HPI: Sweden", "economy", "real_estate"),
    ("NOR", "HPI", "oecd_hpi_no", "OECD Nominal HPI: Norway", "economy", "real_estate"),
    ("NZL", "HPI", "oecd_hpi_nz", "OECD Nominal HPI: New Zealand", "economy", "real_estate"),
    ("IRL", "HPI", "oecd_hpi_ie", "OECD Nominal HPI: Ireland", "economy", "real_estate"),
    ("ISR", "HPI", "oecd_hpi_il", "OECD Nominal HPI: Israel", "economy", "real_estate"),
    # ── Real HPI ──
    ("USA", "RHP", "oecd_rhpi_us", "OECD Real HPI: US", "economy", "real_estate"),
    ("JPN", "RHP", "oecd_rhpi_jp", "OECD Real HPI: Japan", "economy", "real_estate"),
    ("GBR", "RHP", "oecd_rhpi_gb", "OECD Real HPI: UK", "economy", "real_estate"),
    ("DEU", "RHP", "oecd_rhpi_de", "OECD Real HPI: Germany", "economy", "real_estate"),
    ("CHN", "RHP", "oecd_rhpi_cn", "OECD Real HPI: China", "economy", "real_estate"),
    ("AUS", "RHP", "oecd_rhpi_au", "OECD Real HPI: Australia", "economy", "real_estate"),
    ("CAN", "RHP", "oecd_rhpi_ca", "OECD Real HPI: Canada", "economy", "real_estate"),
    ("KOR", "RHP", "oecd_rhpi_kr", "OECD Real HPI: Korea", "economy", "real_estate"),
    # ── Price-to-income ratio ──
    ("USA", "HPI_YDH", "oecd_pti_us", "OECD Price-to-Income: US", "economy", "real_estate"),
    ("JPN", "HPI_YDH", "oecd_pti_jp", "OECD Price-to-Income: Japan", "economy", "real_estate"),
    ("GBR", "HPI_YDH", "oecd_pti_gb", "OECD Price-to-Income: UK", "economy", "real_estate"),
    ("DEU", "HPI_YDH", "oecd_pti_de", "OECD Price-to-Income: Germany", "economy", "real_estate"),
    ("AUS", "HPI_YDH", "oecd_pti_au", "OECD Price-to-Income: Australia", "economy", "real_estate"),
    ("CAN", "HPI_YDH", "oecd_pti_ca", "OECD Price-to-Income: Canada", "economy", "real_estate"),
    ("KOR", "HPI_YDH", "oecd_pti_kr", "OECD Price-to-Income: Korea", "economy", "real_estate"),
    # ── Price-to-rent ratio ──
    ("USA", "HPI_RPI", "oecd_ptr_us", "OECD Price-to-Rent: US", "economy", "real_estate"),
    ("JPN", "HPI_RPI", "oecd_ptr_jp", "OECD Price-to-Rent: Japan", "economy", "real_estate"),
    ("GBR", "HPI_RPI", "oecd_ptr_gb", "OECD Price-to-Rent: UK", "economy", "real_estate"),
    ("DEU", "HPI_RPI", "oecd_ptr_de", "OECD Price-to-Rent: Germany", "economy", "real_estate"),
    ("AUS", "HPI_RPI", "oecd_ptr_au", "OECD Price-to-Rent: Australia", "economy", "real_estate"),
    ("CAN", "HPI_RPI", "oecd_ptr_ca", "OECD Price-to-Rent: Canada", "economy", "real_estate"),
]

_BASE_URL = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.ECO.MPD,DSD_AN_HOUSE_PRICES@DF_HOUSE_PRICES,/"
    "{country}.Q.{measure}.IX"
    "?format=csvfilewithlabels&startPeriod={start}"
)

_OECD_HEADERS = {"User-Agent": "signal-noise/0.1 (research)"}


def _fetch_oecd_csv(url: str, *, timeout: int) -> str:
    timeout_tuple = (10, max(timeout, 60))
    last_error: Exception | None = None

    for _ in range(2):
        try:
            resp = requests.get(url, headers=_OECD_HEADERS, timeout=timeout_tuple)
            resp.raise_for_status()
            return resp.text
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    raise RuntimeError("Unexpected OECD request failure")


def _make_oecd_hp_collector(
    country: str, measure: str, name: str, display_name: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="quarterly",
            api_docs_url="https://data-explorer.oecd.org/vis?df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_AN_HOUSE_PRICES%40DF_HOUSE_PRICES",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            url = _BASE_URL.format(
                country=country,
                measure=measure,
                start="2015-Q1",
            )
            df_raw = pd.read_csv(
                io.StringIO(_fetch_oecd_csv(url, timeout=self.config.request_timeout))
            )
            time_col = "TIME_PERIOD"
            val_col = "OBS_VALUE"

            if time_col not in df_raw.columns or val_col not in df_raw.columns:
                raise RuntimeError(f"Unexpected OECD CSV columns: {list(df_raw.columns)}")

            rows = []
            for _, row in df_raw.iterrows():
                tp = str(row[time_col])
                val = row[val_col]
                if pd.isna(val):
                    continue
                # Convert "2024-Q1" -> date
                year, q = tp.split("-Q")
                month = (int(q) - 1) * 3 + 1
                dt = pd.Timestamp(year=int(year), month=month, day=1, tz="UTC")
                rows.append({"date": dt, "value": float(val)})

            if not rows:
                raise RuntimeError(f"No data for OECD {country}/{measure}")

            result = pd.DataFrame(rows)
            return result.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"OECD_{name}"
    _Collector.__qualname__ = f"OECD_{name}"
    return _Collector


def get_oecd_hp_collectors() -> dict[str, type[BaseCollector]]:
    return {t[2]: _make_oecd_hp_collector(*t) for t in OECD_HP_SERIES}
