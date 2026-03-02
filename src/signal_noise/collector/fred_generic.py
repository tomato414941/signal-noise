from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# FRED API key: set FRED_API_KEY env var or place in ~/.secrets/fred
_FRED_API_KEY: str | None = None


def _get_fred_key() -> str:
    global _FRED_API_KEY
    if _FRED_API_KEY:
        return _FRED_API_KEY

    key = os.environ.get("FRED_API_KEY")
    if not key:
        secret_path = os.path.expanduser("~/.secrets/fred")
        if os.path.exists(secret_path):
            with open(secret_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("export FRED_API_KEY="):
                        key = line.split("=", 1)[1].strip().strip("'\"")
                        break
    if not key:
        raise RuntimeError(
            "FRED_API_KEY not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
        )
    _FRED_API_KEY = key
    return key


# (series_id, collector_name, display_name, frequency, domain, category)
FRED_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── Employment / labor ──
    ("ICSA", "fred_jobless_claims", "Initial Jobless Claims", "weekly", "economy", "labor"),
    ("UNRATE", "fred_unemployment", "US Unemployment Rate", "monthly", "economy", "labor"),
    ("PAYEMS", "fred_nonfarm_payrolls", "Nonfarm Payrolls", "monthly", "economy", "labor"),
    # ── Inflation / prices ──
    ("CPIAUCSL", "fred_cpi", "Consumer Price Index", "monthly", "economy", "inflation"),
    ("CPILFESL", "fred_core_cpi", "Core CPI (ex Food & Energy)", "monthly", "economy", "inflation"),
    ("PCEPI", "fred_pce", "PCE Price Index", "monthly", "economy", "inflation"),
    ("T10YIE", "fred_breakeven_10y", "10Y Breakeven Inflation", "daily", "economy", "inflation"),
    # ── Interest rates / monetary ──
    ("FEDFUNDS", "fred_fed_funds", "Federal Funds Rate", "monthly", "markets", "rates"),
    ("DFF", "fred_daily_fed_funds", "Daily Fed Funds Rate", "daily", "markets", "rates"),
    ("T10Y2Y", "fred_yield_spread", "10Y-2Y Yield Spread", "daily", "markets", "rates"),
    ("T10Y3M", "fred_yield_spread_3m", "10Y-3M Yield Spread", "daily", "markets", "rates"),
    # ── Money supply ──
    ("M2SL", "fred_m2", "M2 Money Supply", "monthly", "markets", "rates"),
    ("WALCL", "fred_fed_balance", "Fed Balance Sheet Total", "weekly", "markets", "rates"),
    # ── GDP / economic activity ──
    ("GDP", "fred_gdp", "US GDP", "quarterly", "economy", "economic"),
    ("GDPC1", "fred_real_gdp", "US Real GDP", "quarterly", "economy", "economic"),
    # ── Consumer / housing ──
    ("UMCSENT", "fred_consumer_sentiment", "U of Michigan Consumer Sentiment", "monthly", "sentiment", "sentiment"),
    ("HOUST", "fred_housing_starts", "Housing Starts", "monthly", "economy", "real_estate"),
    ("CSUSHPISA", "fred_case_shiller", "Case-Shiller Home Price Index", "monthly", "economy", "real_estate"),
    # ── Financial stress / volatility ──
    ("STLFSI2", "fred_financial_stress", "St. Louis Financial Stress Index", "weekly", "markets", "rates"),
    ("BAMLH0A0HYM2", "fred_high_yield_spread", "ICE BofA High Yield Spread", "daily", "markets", "rates"),
    # ── Trade / global ──
    ("DTWEXBGS", "fred_trade_weighted_usd", "Trade Weighted USD Index", "daily", "markets", "forex"),
    ("BOPGSTB", "fred_trade_balance", "US Trade Balance", "monthly", "economy", "trade"),
    # ── Commodities (via FRED) ──
    ("DCOILWTICO", "fred_wti_daily", "WTI Crude Oil (FRED daily)", "daily", "markets", "commodity"),
    # GOLDAMGBD228NLBM discontinued by FRED (May 2025)
    # ── Transportation / logistics ──
    ("TSIFRGHT", "fred_freight_index", "Transportation Services Index: Freight", "monthly", "technology", "logistics"),
    # ── Housing / real estate (expanded) ──
    ("HPIPONM226S", "fred_fhfa_hpi", "FHFA Purchase-Only HPI (Monthly SA)", "monthly", "economy", "real_estate"),
    ("SPCS20RSA", "fred_case_shiller_20", "Case-Shiller 20-City Composite", "monthly", "economy", "real_estate"),
    ("COMREPUSQ159N", "fred_commercial_re", "Commercial Real Estate Prices US", "quarterly", "economy", "real_estate"),
    ("MORTGAGE30US", "fred_mortgage_30y", "30-Year Fixed Mortgage Rate", "weekly", "economy", "real_estate"),
    ("MORTGAGE15US", "fred_mortgage_15y", "15-Year Fixed Mortgage Rate", "weekly", "economy", "real_estate"),
    ("PERMIT", "fred_building_permits", "New Housing Permits", "monthly", "economy", "real_estate"),
    ("EXHOSLUSM495S", "fred_existing_home_sales", "Existing Home Sales", "monthly", "economy", "real_estate"),
    ("HSN1F", "fred_new_home_sales", "New One Family Houses Sold", "monthly", "economy", "real_estate"),
    ("FIXHAI", "fred_affordability", "Housing Affordability Index", "monthly", "economy", "real_estate"),
    # BIS property prices via FRED
    ("QUSR628BIS", "fred_bis_hpi_us", "BIS Real Property Prices: US", "quarterly", "economy", "real_estate"),
    ("QJPR628BIS", "fred_bis_hpi_jp", "BIS Real Property Prices: Japan", "quarterly", "economy", "real_estate"),
    ("QGBR628BIS", "fred_bis_hpi_gb", "BIS Real Property Prices: UK", "quarterly", "economy", "real_estate"),
    ("QDER628BIS", "fred_bis_hpi_de", "BIS Real Property Prices: Germany", "quarterly", "economy", "real_estate"),
    ("QCNR628BIS", "fred_bis_hpi_cn", "BIS Real Property Prices: China", "quarterly", "economy", "real_estate"),
    ("QAUR628BIS", "fred_bis_hpi_au", "BIS Real Property Prices: Australia", "quarterly", "economy", "real_estate"),
    ("QCAR628BIS", "fred_bis_hpi_ca", "BIS Real Property Prices: Canada", "quarterly", "economy", "real_estate"),
    # ── Additional interest rates / credit ──
    ("DGS2", "fred_treasury_2y", "2-Year Treasury Yield", "daily", "markets", "rates"),
    ("DGS5", "fred_treasury_5y", "5-Year Treasury Yield", "daily", "markets", "rates"),
    ("DGS10", "fred_treasury_10y", "10-Year Treasury Yield", "daily", "markets", "rates"),
    ("DGS30", "fred_treasury_30y", "30-Year Treasury Yield", "daily", "markets", "rates"),
    ("DFII10", "fred_tips_10y", "10-Year TIPS Yield", "daily", "markets", "rates"),
    ("BAMLC0A0CM", "fred_ig_spread", "ICE BofA IG Corporate Spread", "daily", "markets", "rates"),
    ("BAMLH0A0HYM2EY", "fred_hy_yield", "ICE BofA HY Effective Yield", "daily", "markets", "rates"),
    ("TEDRATE", "fred_ted_spread", "TED Spread (3M LIBOR - T-Bill)", "daily", "markets", "rates"),
    ("DPRIME", "fred_prime_rate", "Bank Prime Loan Rate", "daily", "markets", "rates"),
    ("AAA", "fred_aaa_yield", "Moody's AAA Corporate Yield", "daily", "markets", "rates"),
    ("BAA", "fred_baa_yield", "Moody's BAA Corporate Yield", "daily", "markets", "rates"),
    ("BAA10Y", "fred_baa_spread", "BAA-10Y Treasury Spread", "daily", "markets", "rates"),
    # ── ISM / PMI / business surveys ──
    ("MANEMP", "fred_mfg_employment", "Manufacturing Employment", "monthly", "economy", "labor"),
    ("INDPRO", "fred_industrial_production", "Industrial Production Index", "monthly", "economy", "economic"),
    ("TCU", "fred_capacity_utilization", "Capacity Utilization", "monthly", "economy", "economic"),
    ("RSAFS", "fred_retail_sales", "Advance Retail Sales", "monthly", "economy", "economic"),
    ("PCEC96", "fred_real_pce", "Real PCE", "monthly", "economy", "economic"),
    ("PI", "fred_personal_income", "Personal Income", "monthly", "economy", "economic"),
    ("PSAVERT", "fred_savings_rate", "Personal Savings Rate", "monthly", "economy", "economic"),
    ("BOGZ1FL073164003Q", "fred_household_debt", "Household Debt Service Ratio", "quarterly", "economy", "economic"),
    ("USSLIND", "fred_leading_index", "Leading Economic Index", "monthly", "economy", "economic"),
    # ── Commodities / market ──
    ("DCOILBRENTEU", "fred_brent_daily", "Brent Crude Oil (FRED daily)", "daily", "markets", "commodity"),
    ("DHHNGSP", "fred_henryhub_daily", "Henry Hub Natural Gas Spot", "daily", "markets", "commodity"),
    ("DEXUSEU", "fred_eurusd", "EUR/USD Exchange Rate", "daily", "markets", "forex"),
    ("DEXJPUS", "fred_usdjpy", "USD/JPY Exchange Rate", "daily", "markets", "forex"),
    ("DEXUSUK", "fred_gbpusd", "GBP/USD Exchange Rate", "daily", "markets", "forex"),
    ("DEXCHUS", "fred_usdcny", "USD/CNY Exchange Rate", "daily", "markets", "forex"),
    ("CBBTCUSD", "fred_btc_price", "Bitcoin Price (Coinbase/FRED)", "daily", "markets", "crypto"),
    ("CBETHUSD", "fred_eth_price", "Ethereum Price (Coinbase/FRED)", "daily", "markets", "crypto"),
    # ── Labor market (additional) ──
    ("JTSJOL", "fred_jolts_openings", "JOLTS Job Openings", "monthly", "economy", "labor"),
    ("JTSQUR", "fred_jolts_quits_rate", "JOLTS Quits Rate", "monthly", "economy", "labor"),
    ("EMRATIO", "fred_employment_pop_ratio", "Employment-Population Ratio", "monthly", "economy", "labor"),
    ("U6RATE", "fred_u6_rate", "U-6 Unemployment Rate", "monthly", "economy", "labor"),
    ("CES0500000003", "fred_avg_hourly_earnings", "Avg Hourly Earnings (Private)", "monthly", "economy", "labor"),
    # ── Financial conditions ──
    ("NFCI", "fred_chicago_nfci", "Chicago Fed NFCI", "weekly", "markets", "rates"),
    ("ANFCI", "fred_adjusted_nfci", "Adjusted NFCI", "weekly", "markets", "rates"),
    ("VIXCLS", "fred_vix", "CBOE VIX (FRED)", "daily", "markets", "equity"),
    ("SP500", "fred_sp500", "S&P 500 (FRED)", "daily", "markets", "equity"),
    ("NASDAQCOM", "fred_nasdaq", "NASDAQ Composite (FRED)", "daily", "markets", "equity"),
    # WILL5000INDFC discontinued by FRED
]


def _make_fred_collector(
    series_id: str, name: str, display_name: str, frequency: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url=f"https://fred.stlouisfed.org/series/{series_id}",
            requires_key=True,
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            api_key = _get_fred_key()
            end = datetime.now(UTC)
            start = end - timedelta(days=365 * 5)
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series_id}"
                f"&api_key={api_key}"
                f"&file_type=json"
                f"&observation_start={start.strftime('%Y-%m-%d')}"
                f"&observation_end={end.strftime('%Y-%m-%d')}"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            observations = resp.json().get("observations", [])

            rows = []
            for obs in observations:
                val = obs.get("value", ".")
                if val == ".":
                    continue
                rows.append({
                    "date": pd.to_datetime(obs["date"], utc=True),
                    "value": float(val),
                })

            if not rows:
                raise RuntimeError(f"No data for FRED series {series_id}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"FRED_{name}"
    _Collector.__qualname__ = f"FRED_{name}"
    return _Collector


def get_fred_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_fred_collector(*t) for t in FRED_SERIES}
