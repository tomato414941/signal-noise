from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (country_code, measure, collector_name, display_name, domain, category)
# Measures: HPI = nominal, RHP = real, HPI_YDH = price-to-income, HPI_RPI = price-to-rent
OECD_HP_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── Nominal HPI ──
    ("USA", "HPI", "oecd_hpi_us", "OECD Nominal HPI: US", "real_estate", "real_estate"),
    ("JPN", "HPI", "oecd_hpi_jp", "OECD Nominal HPI: Japan", "real_estate", "real_estate"),
    ("GBR", "HPI", "oecd_hpi_gb", "OECD Nominal HPI: UK", "real_estate", "real_estate"),
    ("DEU", "HPI", "oecd_hpi_de", "OECD Nominal HPI: Germany", "real_estate", "real_estate"),
    ("FRA", "HPI", "oecd_hpi_fr", "OECD Nominal HPI: France", "real_estate", "real_estate"),
    ("CAN", "HPI", "oecd_hpi_ca", "OECD Nominal HPI: Canada", "real_estate", "real_estate"),
    ("AUS", "HPI", "oecd_hpi_au", "OECD Nominal HPI: Australia", "real_estate", "real_estate"),
    ("KOR", "HPI", "oecd_hpi_kr", "OECD Nominal HPI: Korea", "real_estate", "real_estate"),
    ("CHN", "HPI", "oecd_hpi_cn", "OECD Nominal HPI: China", "real_estate", "real_estate"),
    ("ITA", "HPI", "oecd_hpi_it", "OECD Nominal HPI: Italy", "real_estate", "real_estate"),
    ("ESP", "HPI", "oecd_hpi_es", "OECD Nominal HPI: Spain", "real_estate", "real_estate"),
    ("NLD", "HPI", "oecd_hpi_nl", "OECD Nominal HPI: Netherlands", "real_estate", "real_estate"),
    ("CHE", "HPI", "oecd_hpi_ch", "OECD Nominal HPI: Switzerland", "real_estate", "real_estate"),
    ("SWE", "HPI", "oecd_hpi_se", "OECD Nominal HPI: Sweden", "real_estate", "real_estate"),
    ("NOR", "HPI", "oecd_hpi_no", "OECD Nominal HPI: Norway", "real_estate", "real_estate"),
    ("NZL", "HPI", "oecd_hpi_nz", "OECD Nominal HPI: New Zealand", "real_estate", "real_estate"),
    ("IRL", "HPI", "oecd_hpi_ie", "OECD Nominal HPI: Ireland", "real_estate", "real_estate"),
    ("ISR", "HPI", "oecd_hpi_il", "OECD Nominal HPI: Israel", "real_estate", "real_estate"),
    # ── Real HPI ──
    ("USA", "RHP", "oecd_rhpi_us", "OECD Real HPI: US", "real_estate", "real_estate"),
    ("JPN", "RHP", "oecd_rhpi_jp", "OECD Real HPI: Japan", "real_estate", "real_estate"),
    ("GBR", "RHP", "oecd_rhpi_gb", "OECD Real HPI: UK", "real_estate", "real_estate"),
    ("DEU", "RHP", "oecd_rhpi_de", "OECD Real HPI: Germany", "real_estate", "real_estate"),
    ("CHN", "RHP", "oecd_rhpi_cn", "OECD Real HPI: China", "real_estate", "real_estate"),
    ("AUS", "RHP", "oecd_rhpi_au", "OECD Real HPI: Australia", "real_estate", "real_estate"),
    ("CAN", "RHP", "oecd_rhpi_ca", "OECD Real HPI: Canada", "real_estate", "real_estate"),
    ("KOR", "RHP", "oecd_rhpi_kr", "OECD Real HPI: Korea", "real_estate", "real_estate"),
    # ── Price-to-income ratio ──
    ("USA", "HPI_YDH", "oecd_pti_us", "OECD Price-to-Income: US", "real_estate", "real_estate"),
    ("JPN", "HPI_YDH", "oecd_pti_jp", "OECD Price-to-Income: Japan", "real_estate", "real_estate"),
    ("GBR", "HPI_YDH", "oecd_pti_gb", "OECD Price-to-Income: UK", "real_estate", "real_estate"),
    ("DEU", "HPI_YDH", "oecd_pti_de", "OECD Price-to-Income: Germany", "real_estate", "real_estate"),
    ("CHN", "HPI_YDH", "oecd_pti_cn", "OECD Price-to-Income: China", "real_estate", "real_estate"),
    ("AUS", "HPI_YDH", "oecd_pti_au", "OECD Price-to-Income: Australia", "real_estate", "real_estate"),
    ("CAN", "HPI_YDH", "oecd_pti_ca", "OECD Price-to-Income: Canada", "real_estate", "real_estate"),
    ("KOR", "HPI_YDH", "oecd_pti_kr", "OECD Price-to-Income: Korea", "real_estate", "real_estate"),
    # ── Price-to-rent ratio ──
    ("USA", "HPI_RPI", "oecd_ptr_us", "OECD Price-to-Rent: US", "real_estate", "real_estate"),
    ("JPN", "HPI_RPI", "oecd_ptr_jp", "OECD Price-to-Rent: Japan", "real_estate", "real_estate"),
    ("GBR", "HPI_RPI", "oecd_ptr_gb", "OECD Price-to-Rent: UK", "real_estate", "real_estate"),
    ("DEU", "HPI_RPI", "oecd_ptr_de", "OECD Price-to-Rent: Germany", "real_estate", "real_estate"),
    ("AUS", "HPI_RPI", "oecd_ptr_au", "OECD Price-to-Rent: Australia", "real_estate", "real_estate"),
    ("CAN", "HPI_RPI", "oecd_ptr_ca", "OECD Price-to-Rent: Canada", "real_estate", "real_estate"),
]

_BASE_URL = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.ECO.MPD,DSD_AN_HOUSE_PRICES@DF_HOUSE_PRICES,/"
    "{country}.Q.{measure}.IX"
    "?format=csvfilewithlabels&startPeriod={start}"
)


def _make_oecd_hp_collector(
    country: str, measure: str, name: str, display_name: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
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
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()

            df_raw = pd.read_csv(io.StringIO(resp.text))
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
