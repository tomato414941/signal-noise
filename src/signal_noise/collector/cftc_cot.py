"""CFTC Commitments of Traders (COT) collectors.

Weekly futures positioning data for key commodities.
Three metrics per commodity: open_interest, net_noncommercial, net_commercial.
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (commodity_name_filter, collector_prefix, display_prefix, domain, category)
CFTC_COMMODITIES: list[tuple[str, str, str, str, str]] = [
    # Crypto
    ("BITCOIN", "cot_btc", "COT Bitcoin", "financial", "crypto"),
    ("ETHER CASH SETTLED", "cot_eth", "COT Ether", "financial", "crypto"),
    # Metals
    ("GOLD", "cot_gold", "COT Gold", "financial", "commodity"),
    ("SILVER", "cot_silver", "COT Silver", "financial", "commodity"),
    ("COPPER-GRADE #1", "cot_copper", "COT Copper", "financial", "commodity"),
    ("PLATINUM", "cot_platinum", "COT Platinum", "financial", "commodity"),
    # Energy
    ("CRUDE OIL, LIGHT SWEET", "cot_wti", "COT WTI Crude", "financial", "commodity"),
    ("NATURAL GAS OF HENRY HUB", "cot_natgas", "COT Natural Gas", "financial", "commodity"),
    ("BRENT LAST DAY", "cot_brent", "COT Brent Crude", "financial", "commodity"),
    # Agriculture
    ("CORN", "cot_corn", "COT Corn", "financial", "commodity"),
    ("WHEAT", "cot_wheat", "COT Wheat", "financial", "commodity"),
    ("SOYBEANS", "cot_soy", "COT Soybeans", "financial", "commodity"),
    ("COFFEE C", "cot_coffee", "COT Coffee", "financial", "commodity"),
    ("SUGAR NO. 11", "cot_sugar", "COT Sugar", "financial", "commodity"),
    ("COTTON NO. 2", "cot_cotton", "COT Cotton", "financial", "commodity"),
    # Currencies
    ("EURO FX", "cot_eur", "COT Euro FX", "financial", "forex"),
    ("JAPANESE YEN", "cot_jpy", "COT Japanese Yen", "financial", "forex"),
    ("BRITISH POUND", "cot_gbp", "COT British Pound", "financial", "forex"),
    ("SWISS FRANC", "cot_chf", "COT Swiss Franc", "financial", "forex"),
    ("CANADIAN DOLLAR", "cot_cad", "COT Canadian Dollar", "financial", "forex"),
    ("AUSTRALIAN DOLLAR", "cot_aud", "COT Australian Dollar", "financial", "forex"),
    ("MEXICAN PESO", "cot_mxn", "COT Mexican Peso", "financial", "forex"),
    ("U.S. DOLLAR INDEX", "cot_dxy", "COT US Dollar Index", "financial", "forex"),
    # Indices & Rates
    ("S&P 500 STOCK INDEX", "cot_sp500", "COT S&P 500", "financial", "equity"),
    ("E-MINI S&P 500", "cot_es", "COT E-mini S&P 500", "financial", "equity"),
    ("NASDAQ-100 STOCK INDEX", "cot_nq", "COT Nasdaq 100", "financial", "equity"),
    ("VIX FUTURES", "cot_vix", "COT VIX", "financial", "equity"),
    ("2-YEAR U.S. TREASURY NOTES", "cot_2y", "COT 2Y Treasury", "financial", "rates"),
    ("10-YEAR U.S. TREASURY NOTES", "cot_10y", "COT 10Y Treasury", "financial", "rates"),
    ("U.S. TREASURY BONDS", "cot_30y", "COT 30Y Treasury", "financial", "rates"),
]

# Three metrics per commodity
_METRICS = [
    ("oi", "Open Interest", "open_interest_all"),
    ("net_nc", "Net Non-Commercial", None),  # computed: long - short
    ("net_c", "Net Commercial", None),  # computed: long - short
]

_BASE_URL = "https://publicreporting.cftc.gov/resource/jun7-fc8e.json"


def _make_cot_collector(
    commodity_filter: str, prefix: str, display_prefix: str,
    metric_key: str, metric_label: str, field: str | None,
    domain: str, category: str,
) -> type[BaseCollector]:
    name = f"{prefix}_{metric_key}"
    display = f"{display_prefix}: {metric_label}"

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display,
            update_frequency="weekly",
            api_docs_url="https://publicreporting.cftc.gov/",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            params = {
                "$where": f"commodity_name='{commodity_filter}'",
                "$order": "report_date_as_yyyy_mm_dd DESC",
                "$limit": "5000",
            }
            resp = requests.get(_BASE_URL, params=params, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            rows = []
            for r in data:
                dt = r.get("report_date_as_yyyy_mm_dd")
                if not dt:
                    continue
                try:
                    if field:
                        val = float(r[field])
                    elif metric_key == "net_nc":
                        val = float(r["noncomm_positions_long_all"]) - float(r["noncomm_positions_short_all"])
                    elif metric_key == "net_c":
                        val = float(r["comm_positions_long_all"]) - float(r["comm_positions_short_all"])
                    else:
                        continue
                except (ValueError, TypeError, KeyError):
                    continue
                rows.append({
                    "date": pd.to_datetime(dt[:10], utc=True),
                    "value": val,
                })

            if not rows:
                raise RuntimeError(f"No COT data for {commodity_filter}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"COT_{name}"
    _Collector.__qualname__ = f"COT_{name}"
    return _Collector


def get_cot_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for commodity_filter, prefix, display_prefix, domain, category in CFTC_COMMODITIES:
        for metric_key, metric_label, field in _METRICS:
            name = f"{prefix}_{metric_key}"
            collectors[name] = _make_cot_collector(
                commodity_filter, prefix, display_prefix,
                metric_key, metric_label, field,
                domain, category,
            )
    return collectors
