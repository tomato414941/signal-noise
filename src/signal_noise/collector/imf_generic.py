from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# ISO3 country codes for IMF API
_COUNTRIES = {
    "USA": "US", "JPN": "Japan", "DEU": "Germany", "GBR": "UK",
    "CHN": "China", "IND": "India", "BRA": "Brazil", "KOR": "S.Korea",
    "TUR": "Turkey", "MEX": "Mexico", "RUS": "Russia", "ZAF": "S.Africa",
}

# (indicator, country_iso3, collector_name, display_name, domain, category)
IMF_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── Real GDP growth ──
    ("NGDP_RPCH", "USA", "imf_gdp_growth_us", "IMF GDP Growth: US", "economy", "economic"),
    ("NGDP_RPCH", "CHN", "imf_gdp_growth_cn", "IMF GDP Growth: China", "economy", "economic"),
    ("NGDP_RPCH", "JPN", "imf_gdp_growth_jp", "IMF GDP Growth: Japan", "economy", "economic"),
    ("NGDP_RPCH", "DEU", "imf_gdp_growth_de", "IMF GDP Growth: Germany", "economy", "economic"),
    ("NGDP_RPCH", "GBR", "imf_gdp_growth_gb", "IMF GDP Growth: UK", "economy", "economic"),
    ("NGDP_RPCH", "IND", "imf_gdp_growth_in", "IMF GDP Growth: India", "economy", "economic"),
    # ── Inflation (avg consumer prices) ──
    ("PCPIPCH", "USA", "imf_inflation_us", "IMF Inflation: US", "economy", "inflation"),
    ("PCPIPCH", "CHN", "imf_inflation_cn", "IMF Inflation: China", "economy", "inflation"),
    ("PCPIPCH", "JPN", "imf_inflation_jp", "IMF Inflation: Japan", "economy", "inflation"),
    ("PCPIPCH", "DEU", "imf_inflation_de", "IMF Inflation: Germany", "economy", "inflation"),
    ("PCPIPCH", "GBR", "imf_inflation_gb", "IMF Inflation: UK", "economy", "inflation"),
    ("PCPIPCH", "TUR", "imf_inflation_tr", "IMF Inflation: Turkey", "economy", "inflation"),
    # ── Current account balance (% GDP) ──
    ("BCA_NGDPD", "USA", "imf_current_acct_us", "IMF Current Account: US", "economy", "trade"),
    ("BCA_NGDPD", "CHN", "imf_current_acct_cn", "IMF Current Account: China", "economy", "trade"),
    ("BCA_NGDPD", "DEU", "imf_current_acct_de", "IMF Current Account: Germany", "economy", "trade"),
    ("BCA_NGDPD", "JPN", "imf_current_acct_jp", "IMF Current Account: Japan", "economy", "trade"),
    # ── Government debt (% GDP) ──
    ("GGXWDG_NGDP", "USA", "imf_gov_debt_us", "IMF Gov Debt/GDP: US", "economy", "fiscal"),
    ("GGXWDG_NGDP", "JPN", "imf_gov_debt_jp", "IMF Gov Debt/GDP: Japan", "economy", "fiscal"),
    ("GGXWDG_NGDP", "GBR", "imf_gov_debt_gb", "IMF Gov Debt/GDP: UK", "economy", "fiscal"),
    ("GGXWDG_NGDP", "CHN", "imf_gov_debt_cn", "IMF Gov Debt/GDP: China", "economy", "fiscal"),
    # ── Unemployment rate ──
    ("LUR", "USA", "imf_unemp_us", "IMF Unemployment: US", "economy", "labor"),
    ("LUR", "DEU", "imf_unemp_de", "IMF Unemployment: Germany", "economy", "labor"),
    ("LUR", "GBR", "imf_unemp_gb", "IMF Unemployment: UK", "economy", "labor"),
    ("LUR", "JPN", "imf_unemp_jp", "IMF Unemployment: Japan", "economy", "labor"),
]


def _make_imf_collector(
    indicator: str, country: str, name: str, display_name: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url=f"https://www.imf.org/external/datamapper/{indicator}",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://www.imf.org/external/datamapper/api/v1/{indicator}/{country}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            values = data.get("values", {}).get(indicator, {}).get(country, {})
            if not values:
                raise RuntimeError(f"No IMF data for {indicator}/{country}")

            rows = []
            for year_str, val in values.items():
                try:
                    rows.append({
                        "date": pd.to_datetime(f"{year_str}-01-01", utc=True),
                        "value": float(val),
                    })
                except (ValueError, TypeError):
                    continue

            if not rows:
                raise RuntimeError(f"No valid IMF data for {indicator}/{country}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"IMF_{name}"
    _Collector.__qualname__ = f"IMF_{name}"
    return _Collector


def get_imf_collectors() -> dict[str, type[BaseCollector]]:
    return {t[2]: _make_imf_collector(*t) for t in IMF_SERIES}
