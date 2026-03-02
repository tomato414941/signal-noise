from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (indicator_id, country_code, collector_name, display_name, domain, category)
WORLDBANK_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── GDP growth (annual %) ──
    ("NY.GDP.MKTP.KD.ZG", "US", "wb_gdp_growth_us", "GDP Growth: US", "economy", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "CN", "wb_gdp_growth_cn", "GDP Growth: China", "economy", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "JP", "wb_gdp_growth_jp", "GDP Growth: Japan", "economy", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "DE", "wb_gdp_growth_de", "GDP Growth: Germany", "economy", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "GB", "wb_gdp_growth_gb", "GDP Growth: UK", "economy", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "IN", "wb_gdp_growth_in", "GDP Growth: India", "economy", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "BR", "wb_gdp_growth_br", "GDP Growth: Brazil", "economy", "economic"),
    ("NY.GDP.MKTP.KD.ZG", "KR", "wb_gdp_growth_kr", "GDP Growth: S.Korea", "economy", "economic"),
    # ── Inflation (CPI annual %) ──
    ("FP.CPI.TOTL.ZG", "US", "wb_inflation_us", "Inflation: US", "economy", "inflation"),
    ("FP.CPI.TOTL.ZG", "CN", "wb_inflation_cn", "Inflation: China", "economy", "inflation"),
    ("FP.CPI.TOTL.ZG", "JP", "wb_inflation_jp", "Inflation: Japan", "economy", "inflation"),
    ("FP.CPI.TOTL.ZG", "DE", "wb_inflation_de", "Inflation: Germany", "economy", "inflation"),
    ("FP.CPI.TOTL.ZG", "GB", "wb_inflation_gb", "Inflation: UK", "economy", "inflation"),
    ("FP.CPI.TOTL.ZG", "IN", "wb_inflation_in", "Inflation: India", "economy", "inflation"),
    ("FP.CPI.TOTL.ZG", "BR", "wb_inflation_br", "Inflation: Brazil", "economy", "inflation"),
    ("FP.CPI.TOTL.ZG", "TR", "wb_inflation_tr", "Inflation: Turkey", "economy", "inflation"),
    # ── Trade (% of GDP) ──
    ("NE.TRD.GNFS.ZS", "US", "wb_trade_us", "Trade % GDP: US", "economy", "trade"),
    ("NE.TRD.GNFS.ZS", "CN", "wb_trade_cn", "Trade % GDP: China", "economy", "trade"),
    ("NE.TRD.GNFS.ZS", "DE", "wb_trade_de", "Trade % GDP: Germany", "economy", "trade"),
    ("NE.TRD.GNFS.ZS", "JP", "wb_trade_jp", "Trade % GDP: Japan", "economy", "trade"),
    # ── Foreign reserves (total, USD) ──
    ("FI.RES.TOTL.CD", "CN", "wb_reserves_cn", "Foreign Reserves: China", "markets", "rates"),
    ("FI.RES.TOTL.CD", "JP", "wb_reserves_jp", "Foreign Reserves: Japan", "markets", "rates"),
    ("FI.RES.TOTL.CD", "IN", "wb_reserves_in", "Foreign Reserves: India", "markets", "rates"),
    ("FI.RES.TOTL.CD", "KR", "wb_reserves_kr", "Foreign Reserves: S.Korea", "markets", "rates"),
    # ── Unemployment (% of labor force) ──
    ("SL.UEM.TOTL.ZS", "US", "wb_unemp_us", "Unemployment: US", "economy", "labor"),
    ("SL.UEM.TOTL.ZS", "GB", "wb_unemp_gb", "Unemployment: UK", "economy", "labor"),
    ("SL.UEM.TOTL.ZS", "DE", "wb_unemp_de", "Unemployment: Germany", "economy", "labor"),
    ("SL.UEM.TOTL.ZS", "JP", "wb_unemp_jp", "Unemployment: Japan", "economy", "labor"),
    ("SL.UEM.TOTL.ZS", "BR", "wb_unemp_br", "Unemployment: Brazil", "economy", "labor"),
    # ── Current account balance (% of GDP) ──
    ("BN.CAB.XOKA.GD.ZS", "US", "wb_current_acct_us", "Current Account: US", "economy", "trade"),
    ("BN.CAB.XOKA.GD.ZS", "CN", "wb_current_acct_cn", "Current Account: China", "economy", "trade"),
    ("BN.CAB.XOKA.GD.ZS", "DE", "wb_current_acct_de", "Current Account: Germany", "economy", "trade"),
    ("BN.CAB.XOKA.GD.ZS", "JP", "wb_current_acct_jp", "Current Account: Japan", "economy", "trade"),
    # ── Real interest rate ──
    ("FR.INR.RINR", "US", "wb_real_rate_us", "Real Interest Rate: US", "markets", "rates"),
    ("FR.INR.RINR", "GB", "wb_real_rate_gb", "Real Interest Rate: UK", "markets", "rates"),
    ("FR.INR.RINR", "JP", "wb_real_rate_jp", "Real Interest Rate: Japan", "markets", "rates"),
    ("FR.INR.RINR", "BR", "wb_real_rate_br", "Real Interest Rate: Brazil", "markets", "rates"),
    # ── Broad money (% of GDP) ──
    ("FM.LBL.BMNY.GD.ZS", "US", "wb_broad_money_us", "Broad Money % GDP: US", "markets", "rates"),
    ("FM.LBL.BMNY.GD.ZS", "CN", "wb_broad_money_cn", "Broad Money % GDP: China", "markets", "rates"),
    ("FM.LBL.BMNY.GD.ZS", "JP", "wb_broad_money_jp", "Broad Money % GDP: Japan", "markets", "rates"),
    # ── GDP per capita PPP (current intl $) ──
    ("NY.GDP.PCAP.PP.CD", "US", "wb_gdppc_us", "GDP per Capita PPP: US", "economy", "economic"),
    ("NY.GDP.PCAP.PP.CD", "CN", "wb_gdppc_cn", "GDP per Capita PPP: China", "economy", "economic"),
    ("NY.GDP.PCAP.PP.CD", "JP", "wb_gdppc_jp", "GDP per Capita PPP: Japan", "economy", "economic"),
    ("NY.GDP.PCAP.PP.CD", "DE", "wb_gdppc_de", "GDP per Capita PPP: Germany", "economy", "economic"),
    ("NY.GDP.PCAP.PP.CD", "IN", "wb_gdppc_in", "GDP per Capita PPP: India", "economy", "economic"),
    # ── Gross savings (% of GDP) ──
    ("NY.GNS.ICTR.ZS", "US", "wb_savings_us", "Gross Savings % GDP: US", "economy", "economic"),
    ("NY.GNS.ICTR.ZS", "CN", "wb_savings_cn", "Gross Savings % GDP: China", "economy", "economic"),
    ("NY.GNS.ICTR.ZS", "JP", "wb_savings_jp", "Gross Savings % GDP: Japan", "economy", "economic"),
    ("NY.GNS.ICTR.ZS", "DE", "wb_savings_de", "Gross Savings % GDP: Germany", "economy", "economic"),
    # ── FDI net inflows (% of GDP) ──
    ("BX.KLT.DINV.WD.GD.ZS", "US", "wb_fdi_us", "FDI Inflows % GDP: US", "markets", "trade"),
    ("BX.KLT.DINV.WD.GD.ZS", "CN", "wb_fdi_cn", "FDI Inflows % GDP: China", "markets", "trade"),
    ("BX.KLT.DINV.WD.GD.ZS", "IN", "wb_fdi_in", "FDI Inflows % GDP: India", "markets", "trade"),
    ("BX.KLT.DINV.WD.GD.ZS", "BR", "wb_fdi_br", "FDI Inflows % GDP: Brazil", "markets", "trade"),
    # ── Domestic credit to private sector (% of GDP) ──
    ("FS.AST.DOMS.GD.ZS", "US", "wb_credit_us", "Private Credit % GDP: US", "markets", "rates"),
    ("FS.AST.DOMS.GD.ZS", "CN", "wb_credit_cn", "Private Credit % GDP: China", "markets", "rates"),
    ("FS.AST.DOMS.GD.ZS", "JP", "wb_credit_jp", "Private Credit % GDP: Japan", "markets", "rates"),
    ("FS.AST.DOMS.GD.ZS", "DE", "wb_credit_de", "Private Credit % GDP: Germany", "markets", "rates"),
    # ── Stock market capitalization (% of GDP) ──
    ("CM.MKT.LCAP.GD.ZS", "US", "wb_mktcap_us", "Market Cap % GDP: US", "markets", "equity"),
    ("CM.MKT.LCAP.GD.ZS", "JP", "wb_mktcap_jp", "Market Cap % GDP: Japan", "markets", "equity"),
    ("CM.MKT.LCAP.GD.ZS", "GB", "wb_mktcap_gb", "Market Cap % GDP: UK", "markets", "equity"),
    ("CM.MKT.LCAP.GD.ZS", "KR", "wb_mktcap_kr", "Market Cap % GDP: S.Korea", "markets", "equity"),
    # ── Government debt (% of GDP) ──
    ("GC.DOD.TOTL.GD.ZS", "US", "wb_govdebt_us", "Govt Debt % GDP: US", "economy", "fiscal"),
    ("GC.DOD.TOTL.GD.ZS", "JP", "wb_govdebt_jp", "Govt Debt % GDP: Japan", "economy", "fiscal"),
    ("GC.DOD.TOTL.GD.ZS", "GB", "wb_govdebt_gb", "Govt Debt % GDP: UK", "economy", "fiscal"),
    ("GC.DOD.TOTL.GD.ZS", "BR", "wb_govdebt_br", "Govt Debt % GDP: Brazil", "economy", "fiscal"),
    # ── Population growth (annual %) ──
    ("SP.POP.GROW", "US", "wb_popgrow_us", "Population Growth: US", "economy", "economic"),
    ("SP.POP.GROW", "CN", "wb_popgrow_cn", "Population Growth: China", "economy", "economic"),
    ("SP.POP.GROW", "IN", "wb_popgrow_in", "Population Growth: India", "economy", "economic"),
    ("SP.POP.GROW", "JP", "wb_popgrow_jp", "Population Growth: Japan", "economy", "economic"),
    # ── Energy use per capita (kg oil equivalent) ──
    ("EG.USE.PCAP.KG.OE", "US", "wb_energy_us", "Energy Use per Capita: US", "economy", "economic"),
    ("EG.USE.PCAP.KG.OE", "CN", "wb_energy_cn", "Energy Use per Capita: China", "economy", "economic"),
    ("EG.USE.PCAP.KG.OE", "JP", "wb_energy_jp", "Energy Use per Capita: Japan", "economy", "economic"),
    ("EG.USE.PCAP.KG.OE", "DE", "wb_energy_de", "Energy Use per Capita: Germany", "economy", "economic"),
    # ── Internet users (% of population) ──
    ("IT.NET.USER.ZS", "US", "wb_internet_us", "Internet Users %: US", "technology", "internet"),
    ("IT.NET.USER.ZS", "CN", "wb_internet_cn", "Internet Users %: China", "technology", "internet"),
    ("IT.NET.USER.ZS", "IN", "wb_internet_in", "Internet Users %: India", "technology", "internet"),
    ("IT.NET.USER.ZS", "BR", "wb_internet_br", "Internet Users %: Brazil", "technology", "internet"),
    # ── Life expectancy at birth ──
    ("SP.DYN.LE00.IN", "US", "wb_lifeexp_us", "Life Expectancy: US", "society", "public_health"),
    ("SP.DYN.LE00.IN", "CN", "wb_lifeexp_cn", "Life Expectancy: China", "society", "public_health"),
    ("SP.DYN.LE00.IN", "JP", "wb_lifeexp_jp", "Life Expectancy: Japan", "society", "public_health"),
]


def _make_wb_collector(
    indicator: str, country: str, name: str, display_name: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="yearly",
            api_docs_url=f"https://data.worldbank.org/indicator/{indicator}",
            domain=domain,
            category=category,
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
