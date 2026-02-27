from __future__ import annotations

from io import StringIO

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (flow_key, collector_name, display_name, frequency, domain, category)
# flow_key = "{dataflow}/{series_key}" for ECB SDMX API
ECB_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── Exchange rates (daily) ──
    ("EXR/D.USD.EUR.SP00.A", "ecb_eur_usd", "ECB EUR/USD Rate", "daily", "financial", "forex"),
    ("EXR/D.GBP.EUR.SP00.A", "ecb_eur_gbp", "ECB EUR/GBP Rate", "daily", "financial", "forex"),
    ("EXR/D.JPY.EUR.SP00.A", "ecb_eur_jpy", "ECB EUR/JPY Rate", "daily", "financial", "forex"),
    ("EXR/D.CHF.EUR.SP00.A", "ecb_eur_chf", "ECB EUR/CHF Rate", "daily", "financial", "forex"),
    ("EXR/D.CNY.EUR.SP00.A", "ecb_eur_cny", "ECB EUR/CNY Rate", "daily", "financial", "forex"),
    ("EXR/D.TRY.EUR.SP00.A", "ecb_eur_try", "ECB EUR/TRY Rate", "daily", "financial", "forex"),
    ("EXR/D.BRL.EUR.SP00.A", "ecb_eur_brl", "ECB EUR/BRL Rate", "daily", "financial", "forex"),
    ("EXR/D.INR.EUR.SP00.A", "ecb_eur_inr", "ECB EUR/INR Rate", "daily", "financial", "forex"),
    ("EXR/D.KRW.EUR.SP00.A", "ecb_eur_krw", "ECB EUR/KRW Rate", "daily", "financial", "forex"),
    ("EXR/D.AUD.EUR.SP00.A", "ecb_eur_aud", "ECB EUR/AUD Rate", "daily", "financial", "forex"),
    # ── Key interest rates (daily — FM dataset only serves daily for these) ──
    ("FM/D.U2.EUR.4F.KR.MRR_FR.LEV", "ecb_main_refi_rate", "ECB Main Refinancing Rate", "daily", "financial", "rates"),
    ("FM/D.U2.EUR.4F.KR.DFR.LEV", "ecb_deposit_rate", "ECB Deposit Facility Rate", "daily", "financial", "rates"),
    ("FM/D.U2.EUR.4F.KR.MLFR.LEV", "ecb_marginal_rate", "ECB Marginal Lending Rate", "daily", "financial", "rates"),
    # ── Money market rates (Euribor via Refinitiv provider) ──
    ("FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", "ecb_euribor_3m", "Euribor 3-Month", "monthly", "financial", "rates"),
    ("FM/M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA", "ecb_euribor_6m", "Euribor 6-Month", "monthly", "financial", "rates"),
    # ── ESTER (replaced EONIA in 2022) ──
    ("EST/B.EU000A2X2A25.WT", "ecb_ester", "ECB ESTER Rate", "daily", "financial", "rates"),
    # ── Government bond yields (monthly, Euro Area aggregate only) ──
    ("FM/M.U2.EUR.4F.BB.U2_10Y.YLD", "ecb_ea_10y_yield", "Euro Area 10Y Yield", "monthly", "financial", "rates"),
    # ── Money supply ──
    ("BSI/M.U2.Y.V.M10.X.1.U2.2300.Z01.E", "ecb_m1", "Euro Area M1", "monthly", "financial", "rates"),
    ("BSI/M.U2.Y.V.M20.X.1.U2.2300.Z01.E", "ecb_m2", "Euro Area M2", "monthly", "financial", "rates"),
    ("BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E", "ecb_m3", "Euro Area M3", "monthly", "financial", "rates"),
    # ── HICP Inflation ──
    ("ICP/M.U2.N.000000.4.ANR", "ecb_hicp_ea", "Euro Area HICP Inflation", "monthly", "macro", "inflation"),
    ("ICP/M.DE.N.000000.4.ANR", "ecb_hicp_de", "Germany HICP Inflation", "monthly", "macro", "inflation"),
    ("ICP/M.FR.N.000000.4.ANR", "ecb_hicp_fr", "France HICP Inflation", "monthly", "macro", "inflation"),
    ("ICP/M.IT.N.000000.4.ANR", "ecb_hicp_it", "Italy HICP Inflation", "monthly", "macro", "inflation"),
]


def _make_ecb_collector(
    flow_key: str, name: str, display_name: str, frequency: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://data-api.ecb.europa.eu/",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            url = (
                f"https://data-api.ecb.europa.eu/service/data/{flow_key}"
                f"?format=csvdata&startPeriod=2015-01-01"
            )
            headers = {"Accept": "text/csv"}
            resp = requests.get(url, headers=headers, timeout=self.config.request_timeout)
            resp.raise_for_status()

            df = pd.read_csv(StringIO(resp.text))

            if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
                raise RuntimeError(f"Unexpected ECB CSV format for {flow_key}")

            rows = []
            for _, row in df.iterrows():
                try:
                    val = float(row["OBS_VALUE"])
                except (ValueError, TypeError):
                    continue
                rows.append({
                    "date": pd.to_datetime(row["TIME_PERIOD"], utc=True),
                    "value": val,
                })

            if not rows:
                raise RuntimeError(f"No data for ECB {flow_key}")

            result = pd.DataFrame(rows)
            return result.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"ECB_{name}"
    _Collector.__qualname__ = f"ECB_{name}"
    return _Collector


def get_ecb_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_ecb_collector(*t) for t in ECB_SERIES}
