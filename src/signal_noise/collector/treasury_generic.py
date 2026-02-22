from __future__ import annotations

from io import StringIO

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_TREASURY_YIELD_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates"
    "/daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve"
    "&page&_format=csv"
)

# Yield curve maturities to extract as separate collectors
TREASURY_YIELD_MATURITIES: list[tuple[str, str]] = [
    ("1 Mo", "tsy_yield_1m", "Treasury Yield 1-Month"),
    ("3 Mo", "tsy_yield_3m", "Treasury Yield 3-Month"),
    ("6 Mo", "tsy_yield_6m", "Treasury Yield 6-Month"),
    ("1 Yr", "tsy_yield_1y", "Treasury Yield 1-Year"),
    ("2 Yr", "tsy_yield_2y", "Treasury Yield 2-Year"),
    ("5 Yr", "tsy_yield_5y", "Treasury Yield 5-Year"),
    ("10 Yr", "tsy_yield_10y", "Treasury Yield 10-Year"),
    ("20 Yr", "tsy_yield_20y", "Treasury Yield 20-Year"),
    ("30 Yr", "tsy_yield_30y", "Treasury Yield 30-Year"),
]

# (endpoint_path, value_field, collector_name, display_name, domain, category)
TREASURY_FISCAL_SERIES: list[tuple[str, str, str, str, str, str]] = [
    (
        "v2/accounting/od/debt_to_penny",
        "tot_pub_debt_out_amt",
        "tsy_total_debt",
        "US Total Public Debt",
        "macro",
        "fiscal",
    ),
    (
        "v2/accounting/od/debt_to_penny",
        "debt_held_public_amt",
        "tsy_debt_public",
        "US Debt Held by Public",
        "macro",
        "fiscal",
    ),
    (
        "v2/accounting/od/avg_interest_rates?filter=security_desc:eq:Treasury Bills",
        "avg_interest_rate_amt",
        "tsy_avg_rate_bills",
        "Avg Interest Rate: T-Bills",
        "financial",
        "rates",
    ),
    (
        "v2/accounting/od/avg_interest_rates?filter=security_desc:eq:Treasury Notes",
        "avg_interest_rate_amt",
        "tsy_avg_rate_notes",
        "Avg Interest Rate: T-Notes",
        "financial",
        "rates",
    ),
    (
        "v2/accounting/od/avg_interest_rates?filter=security_desc:eq:Treasury Bonds",
        "avg_interest_rate_amt",
        "tsy_avg_rate_bonds",
        "Avg Interest Rate: T-Bonds",
        "financial",
        "rates",
    ),
]


def _make_yield_collector(
    col_name: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://home.treasury.gov/resource-center/data-chart-center/interest-rates",
            domain="financial",
            category="rates",
        )

        def fetch(self) -> pd.DataFrame:
            from datetime import UTC, datetime
            current_year = datetime.now(UTC).year
            all_rows = []
            for year in range(current_year - 4, current_year + 1):
                url = _TREASURY_YIELD_URL.format(year=year)
                try:
                    resp = requests.get(url, timeout=self.config.request_timeout)
                    resp.raise_for_status()
                    df = pd.read_csv(StringIO(resp.text))
                    if "Date" not in df.columns or col_name not in df.columns:
                        continue
                    for _, row in df.iterrows():
                        try:
                            val = float(row[col_name])
                        except (ValueError, TypeError):
                            continue
                        all_rows.append({
                            "date": pd.to_datetime(row["Date"], utc=True),
                            "value": val,
                        })
                except Exception:
                    continue

            if not all_rows:
                raise RuntimeError(f"No Treasury yield data for {col_name}")

            result = pd.DataFrame(all_rows)
            return result.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"TSY_{name}"
    _Collector.__qualname__ = f"TSY_{name}"
    return _Collector


def _make_fiscal_collector(
    endpoint: str, value_field: str, name: str, display_name: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://fiscaldata.treasury.gov/",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            base = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
            sep = "&" if "?" in endpoint else "?"
            url = (
                f"{base}/{endpoint}{sep}"
                f"sort=-record_date&page%5Bsize%5D=1000"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json().get("data", [])

            rows = []
            for obs in data:
                try:
                    val = float(obs[value_field])
                except (ValueError, TypeError, KeyError):
                    continue
                rows.append({
                    "date": pd.to_datetime(obs["record_date"], utc=True),
                    "value": val,
                })

            if not rows:
                raise RuntimeError(f"No data for Treasury {endpoint}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"TSY_{name}"
    _Collector.__qualname__ = f"TSY_{name}"
    return _Collector


def get_treasury_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for col_name, name, display_name in TREASURY_YIELD_MATURITIES:
        collectors[name] = _make_yield_collector(col_name, name, display_name)
    for endpoint, value_field, name, display_name, domain, category in TREASURY_FISCAL_SERIES:
        collectors[name] = _make_fiscal_collector(
            endpoint, value_field, name, display_name, domain, category
        )
    return collectors
