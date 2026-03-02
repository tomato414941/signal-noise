"""CFTC Commitments of Traders (COT) collectors.

Weekly futures positioning data for key commodities.
Three metrics per commodity: open_interest, net_noncommercial, net_commercial.
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (contract_market_name, collector_prefix, display_prefix, domain, category)
# Filter on contract_market_name (not commodity_name) for precise matching.
CFTC_COMMODITIES: list[tuple[str, str, str, str, str]] = [
    # Crypto
    ("BITCOIN", "cot_btc", "COT Bitcoin", "markets", "crypto"),
    ("ETHER CASH SETTLED", "cot_eth", "COT Ether", "markets", "crypto"),
    # Metals
    ("GOLD", "cot_gold", "COT Gold", "markets", "commodity"),
    ("SILVER", "cot_silver", "COT Silver", "markets", "commodity"),
    ("COPPER- #1", "cot_copper", "COT Copper", "markets", "commodity"),
    ("PLATINUM", "cot_platinum", "COT Platinum", "markets", "commodity"),
    # Energy
    ("WTI-PHYSICAL", "cot_wti", "COT WTI Crude", "markets", "commodity"),
    ("NAT GAS NYME", "cot_natgas", "COT Natural Gas", "markets", "commodity"),
    ("BRENT LAST DAY", "cot_brent", "COT Brent Crude", "markets", "commodity"),
    # Agriculture
    ("CORN", "cot_corn", "COT Corn", "markets", "commodity"),
    ("WHEAT-SRW", "cot_wheat", "COT Wheat", "markets", "commodity"),
    ("SOYBEANS", "cot_soy", "COT Soybeans", "markets", "commodity"),
    ("COFFEE C", "cot_coffee", "COT Coffee", "markets", "commodity"),
    ("SUGAR NO. 11", "cot_sugar", "COT Sugar", "markets", "commodity"),
    ("COTTON NO. 2", "cot_cotton", "COT Cotton", "markets", "commodity"),
    # Currencies
    ("EURO FX", "cot_eur", "COT Euro FX", "markets", "forex"),
    ("JAPANESE YEN", "cot_jpy", "COT Japanese Yen", "markets", "forex"),
    ("BRITISH POUND", "cot_gbp", "COT British Pound", "markets", "forex"),
    ("SWISS FRANC", "cot_chf", "COT Swiss Franc", "markets", "forex"),
    ("CANADIAN DOLLAR", "cot_cad", "COT Canadian Dollar", "markets", "forex"),
    ("AUSTRALIAN DOLLAR", "cot_aud", "COT Australian Dollar", "markets", "forex"),
    ("MEXICAN PESO", "cot_mxn", "COT Mexican Peso", "markets", "forex"),
    ("USD INDEX", "cot_dxy", "COT US Dollar Index", "markets", "forex"),
    # Indices & Rates
    ("E-MINI S&P 500", "cot_es", "COT E-mini S&P 500", "markets", "equity"),
    ("NASDAQ MINI", "cot_nq", "COT Nasdaq 100", "markets", "equity"),
    ("VIX FUTURES", "cot_vix", "COT VIX", "markets", "equity"),
    ("UST 2Y NOTE", "cot_2y", "COT 2Y Treasury", "markets", "rates"),
    ("UST 10Y NOTE", "cot_10y", "COT 10Y Treasury", "markets", "rates"),
    ("UST BOND", "cot_30y", "COT 30Y Treasury", "markets", "rates"),
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
                "$where": f"contract_market_name='{commodity_filter}'",
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
