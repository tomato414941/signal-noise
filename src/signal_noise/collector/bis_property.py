from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (country_code, value_type, unit, collector_name, display_name, domain, category)
# value_type: N=Nominal, R=Real
# unit: 628=Index(2010=100), 771=YoY % change
BIS_PROPERTY_SERIES: list[tuple[str, str, str, str, str, str, str]] = [
    # ── Nominal index (2010=100) ──
    ("US", "N", "628", "bis_pp_us", "BIS Property Prices: US (Nominal)", "real_estate", "real_estate"),
    ("JP", "N", "628", "bis_pp_jp", "BIS Property Prices: Japan (Nominal)", "real_estate", "real_estate"),
    ("GB", "N", "628", "bis_pp_gb", "BIS Property Prices: UK (Nominal)", "real_estate", "real_estate"),
    ("DE", "N", "628", "bis_pp_de", "BIS Property Prices: Germany (Nominal)", "real_estate", "real_estate"),
    ("FR", "N", "628", "bis_pp_fr", "BIS Property Prices: France (Nominal)", "real_estate", "real_estate"),
    ("CN", "N", "628", "bis_pp_cn", "BIS Property Prices: China (Nominal)", "real_estate", "real_estate"),
    ("CA", "N", "628", "bis_pp_ca", "BIS Property Prices: Canada (Nominal)", "real_estate", "real_estate"),
    ("AU", "N", "628", "bis_pp_au", "BIS Property Prices: Australia (Nominal)", "real_estate", "real_estate"),
    ("KR", "N", "628", "bis_pp_kr", "BIS Property Prices: Korea (Nominal)", "real_estate", "real_estate"),
    ("IT", "N", "628", "bis_pp_it", "BIS Property Prices: Italy (Nominal)", "real_estate", "real_estate"),
    ("ES", "N", "628", "bis_pp_es", "BIS Property Prices: Spain (Nominal)", "real_estate", "real_estate"),
    ("NL", "N", "628", "bis_pp_nl", "BIS Property Prices: Netherlands (Nominal)", "real_estate", "real_estate"),
    ("SE", "N", "628", "bis_pp_se", "BIS Property Prices: Sweden (Nominal)", "real_estate", "real_estate"),
    ("CH", "N", "628", "bis_pp_ch", "BIS Property Prices: Switzerland (Nominal)", "real_estate", "real_estate"),
    ("NZ", "N", "628", "bis_pp_nz", "BIS Property Prices: New Zealand (Nominal)", "real_estate", "real_estate"),
    ("IN", "N", "628", "bis_pp_in", "BIS Property Prices: India (Nominal)", "real_estate", "real_estate"),
    ("BR", "N", "628", "bis_pp_br", "BIS Property Prices: Brazil (Nominal)", "real_estate", "real_estate"),
    ("ZA", "N", "628", "bis_pp_za", "BIS Property Prices: South Africa (Nominal)", "real_estate", "real_estate"),
    ("TR", "N", "628", "bis_pp_tr", "BIS Property Prices: Turkey (Nominal)", "real_estate", "real_estate"),
    ("MX", "N", "628", "bis_pp_mx", "BIS Property Prices: Mexico (Nominal)", "real_estate", "real_estate"),
    # ── Real index (2010=100) ──
    ("US", "R", "628", "bis_rpp_us", "BIS Real Property Prices: US", "real_estate", "real_estate"),
    ("JP", "R", "628", "bis_rpp_jp", "BIS Real Property Prices: Japan", "real_estate", "real_estate"),
    ("GB", "R", "628", "bis_rpp_gb", "BIS Real Property Prices: UK", "real_estate", "real_estate"),
    ("DE", "R", "628", "bis_rpp_de", "BIS Real Property Prices: Germany", "real_estate", "real_estate"),
    ("CN", "R", "628", "bis_rpp_cn", "BIS Real Property Prices: China", "real_estate", "real_estate"),
    ("AU", "R", "628", "bis_rpp_au", "BIS Real Property Prices: Australia", "real_estate", "real_estate"),
    ("CA", "R", "628", "bis_rpp_ca", "BIS Real Property Prices: Canada", "real_estate", "real_estate"),
    ("KR", "R", "628", "bis_rpp_kr", "BIS Real Property Prices: Korea", "real_estate", "real_estate"),
    # ── YoY % change (nominal) ──
    ("US", "N", "771", "bis_pp_yoy_us", "BIS Property Prices YoY: US", "real_estate", "real_estate"),
    ("JP", "N", "771", "bis_pp_yoy_jp", "BIS Property Prices YoY: Japan", "real_estate", "real_estate"),
    ("GB", "N", "771", "bis_pp_yoy_gb", "BIS Property Prices YoY: UK", "real_estate", "real_estate"),
    ("CN", "N", "771", "bis_pp_yoy_cn", "BIS Property Prices YoY: China", "real_estate", "real_estate"),
]

_BASE_URL = (
    "https://data.bis.org/topics/RPP/BIS,WS_SPP,1.0/"
    "Q.{country}.{value_type}.{unit}"
    "?file_format=csv&format=long&include=code,label"
)

# BIS CSV has metadata rows before the actual data header.
# The data header row starts with "DATAFLOW_ID" or contains "OBS_VALUE".
_DATA_HEADER_MARKER = "OBS_VALUE"


def _make_bis_pp_collector(
    country: str, value_type: str, unit: str,
    name: str, display_name: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="quarterly",
            api_docs_url=f"https://data.bis.org/topics/RPP/BIS,WS_SPP,1.0/Q.{country}.{value_type}.{unit}",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            url = _BASE_URL.format(
                country=country,
                value_type=value_type,
                unit=unit,
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()

            # Find the data header row (skip metadata rows)
            lines = resp.text.splitlines()
            header_idx = None
            for i, line in enumerate(lines):
                if _DATA_HEADER_MARKER in line:
                    header_idx = i
                    break

            if header_idx is None:
                raise RuntimeError(f"Cannot find data header in BIS response for {country}/{value_type}/{unit}")

            csv_text = "\n".join(lines[header_idx:])
            df_raw = pd.read_csv(io.StringIO(csv_text))

            # Find the time and value columns
            time_col = [c for c in df_raw.columns if "TIME_PERIOD" in c]
            val_col = [c for c in df_raw.columns if "OBS_VALUE" in c]

            if not time_col or not val_col:
                raise RuntimeError(f"Missing columns in BIS CSV: {list(df_raw.columns)}")

            rows = []
            for _, row in df_raw.iterrows():
                tp = str(row[time_col[0]])
                val = row[val_col[0]]
                if pd.isna(val) or str(val).strip() == "":
                    continue
                # BIS dates are like "2024-03-31"
                dt = pd.to_datetime(tp, utc=True)
                rows.append({"date": dt, "value": float(val)})

            if not rows:
                raise RuntimeError(f"No data for BIS {country}/{value_type}/{unit}")

            result = pd.DataFrame(rows)
            return result.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"BIS_{name}"
    _Collector.__qualname__ = f"BIS_{name}"
    return _Collector


def get_bis_pp_collectors() -> dict[str, type[BaseCollector]]:
    return {t[3]: _make_bis_pp_collector(*t) for t in BIS_PROPERTY_SERIES}
