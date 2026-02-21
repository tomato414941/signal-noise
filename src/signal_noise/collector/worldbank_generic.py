from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (indicator_id, country_code, collector_name, display_name, data_type)
WORLDBANK_SERIES: list[tuple[str, str, str, str, str]] = [
    # ── GDP growth (annual %) ──
    ("NY.GDP.MKTP.KD.ZG", "US", "wb_gdp_growth_us", "GDP Growth: US", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "CN", "wb_gdp_growth_cn", "GDP Growth: China", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "JP", "wb_gdp_growth_jp", "GDP Growth: Japan", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "DE", "wb_gdp_growth_de", "GDP Growth: Germany", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "GB", "wb_gdp_growth_gb", "GDP Growth: UK", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "IN", "wb_gdp_growth_in", "GDP Growth: India", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "BR", "wb_gdp_growth_br", "GDP Growth: Brazil", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "KR", "wb_gdp_growth_kr", "GDP Growth: S.Korea", "economic"),
    # ── Inflation (CPI annual %) ──
    ("FP.CPI.TOTL.ZG", "US", "wb_inflation_us", "Inflation: US", "inflation"),
    ("FP.CPI.TOTL.ZG", "CN", "wb_inflation_cn", "Inflation: China", "inflation"),
    ("FP.CPI.TOTL.ZG", "JP", "wb_inflation_jp", "Inflation: Japan", "inflation"),
    ("FP.CPI.TOTL.ZG", "DE", "wb_inflation_de", "Inflation: Germany", "inflation"),
    ("FP.CPI.TOTL.ZG", "GB", "wb_inflation_gb", "Inflation: UK", "inflation"),
    ("FP.CPI.TOTL.ZG", "IN", "wb_inflation_in", "Inflation: India", "inflation"),
    ("FP.CPI.TOTL.ZG", "BR", "wb_inflation_br", "Inflation: Brazil", "inflation"),
    ("FP.CPI.TOTL.ZG", "TR", "wb_inflation_tr", "Inflation: Turkey", "inflation"),
    # ── Trade (% of GDP) ──
    ("NE.TRD.GNFS.ZS", "US", "wb_trade_us", "Trade % GDP: US", "trade"),
    ("NE.TRD.GNFS.ZS", "CN", "wb_trade_cn", "Trade % GDP: China", "trade"),
    ("NE.TRD.GNFS.ZS", "DE", "wb_trade_de", "Trade % GDP: Germany", "trade"),
    ("NE.TRD.GNFS.ZS", "JP", "wb_trade_jp", "Trade % GDP: Japan", "trade"),
    # ── Foreign reserves (total, USD) ──
    ("FI.RES.TOTL.CD", "CN", "wb_reserves_cn", "Foreign Reserves: China", "monetary"),
    ("FI.RES.TOTL.CD", "JP", "wb_reserves_jp", "Foreign Reserves: Japan", "monetary"),
    ("FI.RES.TOTL.CD", "IN", "wb_reserves_in", "Foreign Reserves: India", "monetary"),
    ("FI.RES.TOTL.CD", "KR", "wb_reserves_kr", "Foreign Reserves: S.Korea", "monetary"),
    # ── Unemployment (% of labor force) ──
    ("SL.UEM.TOTL.ZS", "US", "wb_unemp_us", "Unemployment: US", "labor"),
    ("SL.UEM.TOTL.ZS", "GB", "wb_unemp_gb", "Unemployment: UK", "labor"),
    ("SL.UEM.TOTL.ZS", "DE", "wb_unemp_de", "Unemployment: Germany", "labor"),
    ("SL.UEM.TOTL.ZS", "JP", "wb_unemp_jp", "Unemployment: Japan", "labor"),
    ("SL.UEM.TOTL.ZS", "BR", "wb_unemp_br", "Unemployment: Brazil", "labor"),
    # ── Current account balance (% of GDP) ──
    ("BN.CAB.XOKA.GD.ZS", "US", "wb_current_acct_us", "Current Account: US", "trade"),
    ("BN.CAB.XOKA.GD.ZS", "CN", "wb_current_acct_cn", "Current Account: China", "trade"),
    ("BN.CAB.XOKA.GD.ZS", "DE", "wb_current_acct_de", "Current Account: Germany", "trade"),
    ("BN.CAB.XOKA.GD.ZS", "JP", "wb_current_acct_jp", "Current Account: Japan", "trade"),
    # ── Real interest rate ──
    ("FR.INR.RINR", "US", "wb_real_rate_us", "Real Interest Rate: US", "monetary"),
    ("FR.INR.RINR", "GB", "wb_real_rate_gb", "Real Interest Rate: UK", "monetary"),
    ("FR.INR.RINR", "JP", "wb_real_rate_jp", "Real Interest Rate: Japan", "monetary"),
    ("FR.INR.RINR", "BR", "wb_real_rate_br", "Real Interest Rate: Brazil", "monetary"),
    # ── Broad money (% of GDP) ──
    ("FM.LBL.BMNY.GD.ZS", "US", "wb_broad_money_us", "Broad Money % GDP: US", "monetary"),
    ("FM.LBL.BMNY.GD.ZS", "CN", "wb_broad_money_cn", "Broad Money % GDP: China", "monetary"),
    ("FM.LBL.BMNY.GD.ZS", "JP", "wb_broad_money_jp", "Broad Money % GDP: Japan", "monetary"),
]


def _make_wb_collector(
    indicator: str, country: str, name: str, display_name: str, data_type: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            data_type=data_type,
            api_docs_url=f"https://data.worldbank.org/indicator/{indicator}",
        )

        def fetch(self) -> pd.DataFrame:
            url = (
                f"https://api.worldbank.org/v2/country/{country}"
                f"/indicator/{indicator}?format=json&per_page=50"
                f"&date=2000:2026"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, list) or len(data) < 2:
                raise RuntimeError(f"Unexpected WB response for {indicator}/{country}")

            rows = []
            for obs in data[1] or []:
                val = obs.get("value")
                if val is None:
                    continue
                rows.append({
                    "date": pd.to_datetime(f"{obs['date']}-01-01", utc=True),
                    "value": float(val),
                })

            if not rows:
                raise RuntimeError(f"No data for WB {indicator}/{country}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"WB_{name}"
    _Collector.__qualname__ = f"WB_{name}"
    return _Collector


def get_wb_collectors() -> dict[str, type[BaseCollector]]:
    return {t[2]: _make_wb_collector(*t) for t in WORLDBANK_SERIES}
