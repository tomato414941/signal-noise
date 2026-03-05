from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._utils import build_timeseries_df

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
    ("FS.AST.DOMS.GD.ZS", "JP", "wb_credit_jp", "Private Credit % GDP: Japan", "markets", "rates"),
    # ── Stock market capitalization (% of GDP) ──
    ("CM.MKT.LCAP.GD.ZS", "US", "wb_mktcap_us", "Market Cap % GDP: US", "markets", "equity"),
    ("CM.MKT.LCAP.GD.ZS", "JP", "wb_mktcap_jp", "Market Cap % GDP: Japan", "markets", "equity"),
    ("CM.MKT.LCAP.GD.ZS", "GB", "wb_mktcap_gb", "Market Cap % GDP: UK", "markets", "equity"),
    ("CM.MKT.LCAP.GD.ZS", "KR", "wb_mktcap_kr", "Market Cap % GDP: S.Korea", "markets", "equity"),
    # ── Government debt (% of GDP) ──
    ("GC.DOD.TOTL.GD.ZS", "US", "wb_govdebt_us", "Govt Debt % GDP: US", "economy", "fiscal"),
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
    # ── Gini index (income inequality) ──
    ("SI.POV.GINI", "US", "wb_gini_us", "Gini Index: US", "economy", "inequality"),
    ("SI.POV.GINI", "CN", "wb_gini_cn", "Gini Index: China", "economy", "inequality"),
    ("SI.POV.GINI", "GB", "wb_gini_gb", "Gini Index: UK", "economy", "inequality"),
    ("SI.POV.GINI", "BR", "wb_gini_br", "Gini Index: Brazil", "economy", "inequality"),
    ("SI.POV.GINI", "ZA", "wb_gini_za", "Gini Index: South Africa", "economy", "inequality"),
    ("SI.POV.GINI", "DE", "wb_gini_de", "Gini Index: Germany", "economy", "inequality"),
    ("SI.POV.GINI", "IN", "wb_gini_in", "Gini Index: India", "economy", "inequality"),
    ("SI.POV.GINI", "MX", "wb_gini_mx", "Gini Index: Mexico", "economy", "inequality"),
    # ── Birth rate (per 1,000 people) ──
    ("SP.DYN.CBRT.IN", "US", "wb_birthrate_us", "Birth Rate: US", "society", "demographics"),
    ("SP.DYN.CBRT.IN", "CN", "wb_birthrate_cn", "Birth Rate: China", "society", "demographics"),
    ("SP.DYN.CBRT.IN", "JP", "wb_birthrate_jp", "Birth Rate: Japan", "society", "demographics"),
    ("SP.DYN.CBRT.IN", "IN", "wb_birthrate_in", "Birth Rate: India", "society", "demographics"),
    ("SP.DYN.CBRT.IN", "NG", "wb_birthrate_ng", "Birth Rate: Nigeria", "society", "demographics"),
    ("SP.DYN.CBRT.IN", "DE", "wb_birthrate_de", "Birth Rate: Germany", "society", "demographics"),
    # ── Death rate (per 1,000 people) ──
    ("SP.DYN.CDRT.IN", "US", "wb_deathrate_us", "Death Rate: US", "society", "demographics"),
    ("SP.DYN.CDRT.IN", "CN", "wb_deathrate_cn", "Death Rate: China", "society", "demographics"),
    ("SP.DYN.CDRT.IN", "JP", "wb_deathrate_jp", "Death Rate: Japan", "society", "demographics"),
    ("SP.DYN.CDRT.IN", "IN", "wb_deathrate_in", "Death Rate: India", "society", "demographics"),
    ("SP.DYN.CDRT.IN", "RU", "wb_deathrate_ru", "Death Rate: Russia", "society", "demographics"),
    ("SP.DYN.CDRT.IN", "DE", "wb_deathrate_de", "Death Rate: Germany", "society", "demographics"),
    # ── International tourism arrivals ──
    ("ST.INT.ARVL", "US", "wb_tourism_us", "Tourism Arrivals: US", "economy", "tourism"),
    ("ST.INT.ARVL", "FR", "wb_tourism_fr", "Tourism Arrivals: France", "economy", "tourism"),
    ("ST.INT.ARVL", "ES", "wb_tourism_es", "Tourism Arrivals: Spain", "economy", "tourism"),
    ("ST.INT.ARVL", "IT", "wb_tourism_it", "Tourism Arrivals: Italy", "economy", "tourism"),
    ("ST.INT.ARVL", "JP", "wb_tourism_jp", "Tourism Arrivals: Japan", "economy", "tourism"),
    ("ST.INT.ARVL", "TH", "wb_tourism_th", "Tourism Arrivals: Thailand", "economy", "tourism"),
    # ── School enrollment, primary (% gross) ──
    ("SE.PRM.ENRR", "US", "wb_enrollment_us", "Primary Enrollment: US", "society", "education"),
    ("SE.PRM.ENRR", "CN", "wb_enrollment_cn", "Primary Enrollment: China", "society", "education"),
    ("SE.PRM.ENRR", "IN", "wb_enrollment_in", "Primary Enrollment: India", "society", "education"),
    ("SE.PRM.ENRR", "BR", "wb_enrollment_br", "Primary Enrollment: Brazil", "society", "education"),
    ("SE.PRM.ENRR", "NG", "wb_enrollment_ng", "Primary Enrollment: Nigeria", "society", "education"),
    # ── School enrollment, secondary (% gross) ──
    ("SE.SEC.ENRR", "US", "wb_sec_enrollment_us", "Secondary Enrollment: US", "society", "education"),
    ("SE.SEC.ENRR", "CN", "wb_sec_enrollment_cn", "Secondary Enrollment: China", "society", "education"),
    ("SE.SEC.ENRR", "IN", "wb_sec_enrollment_in", "Secondary Enrollment: India", "society", "education"),
    ("SE.SEC.ENRR", "BR", "wb_sec_enrollment_br", "Secondary Enrollment: Brazil", "society", "education"),
    ("SE.SEC.ENRR", "NG", "wb_sec_enrollment_ng", "Secondary Enrollment: Nigeria", "society", "education"),
    # ── School enrollment, tertiary (% gross) ──
    ("SE.TER.ENRR", "US", "wb_ter_enrollment_us", "Tertiary Enrollment: US", "society", "education"),
    ("SE.TER.ENRR", "CN", "wb_ter_enrollment_cn", "Tertiary Enrollment: China", "society", "education"),
    ("SE.TER.ENRR", "IN", "wb_ter_enrollment_in", "Tertiary Enrollment: India", "society", "education"),
    ("SE.TER.ENRR", "BR", "wb_ter_enrollment_br", "Tertiary Enrollment: Brazil", "society", "education"),
    # ── Literacy rate, adult total (% ages 15+) ──
    ("SE.ADT.LITR.ZS", "CN", "wb_literacy_cn", "Adult Literacy Rate: China", "society", "education"),
    ("SE.ADT.LITR.ZS", "IN", "wb_literacy_in", "Adult Literacy Rate: India", "society", "education"),
    ("SE.ADT.LITR.ZS", "BR", "wb_literacy_br", "Adult Literacy Rate: Brazil", "society", "education"),
    # ── Control of Corruption (governance, -2.5 to 2.5) ──
    ("CC.EST", "US", "wb_corruption_us", "Control of Corruption: US", "society", "governance"),
    ("CC.EST", "CN", "wb_corruption_cn", "Control of Corruption: China", "society", "governance"),
    ("CC.EST", "JP", "wb_corruption_jp", "Control of Corruption: Japan", "society", "governance"),
    ("CC.EST", "RU", "wb_corruption_ru", "Control of Corruption: Russia", "society", "governance"),
    ("CC.EST", "BR", "wb_corruption_br", "Control of Corruption: Brazil", "society", "governance"),
    ("CC.EST", "IN", "wb_corruption_in", "Control of Corruption: India", "society", "governance"),
    # ── Voice and Accountability (governance, -2.5 to 2.5) ──
    ("VA.EST", "US", "wb_voice_us", "Voice & Accountability: US", "society", "governance"),
    ("VA.EST", "CN", "wb_voice_cn", "Voice & Accountability: China", "society", "governance"),
    ("VA.EST", "RU", "wb_voice_ru", "Voice & Accountability: Russia", "society", "governance"),
    ("VA.EST", "IN", "wb_voice_in", "Voice & Accountability: India", "society", "governance"),
    ("VA.EST", "BR", "wb_voice_br", "Voice & Accountability: Brazil", "society", "governance"),
    # ── Rule of Law (governance, -2.5 to 2.5) ──
    ("RL.EST", "US", "wb_ruleoflaw_us", "Rule of Law: US", "society", "governance"),
    ("RL.EST", "CN", "wb_ruleoflaw_cn", "Rule of Law: China", "society", "governance"),
    ("RL.EST", "JP", "wb_ruleoflaw_jp", "Rule of Law: Japan", "society", "governance"),
    ("RL.EST", "RU", "wb_ruleoflaw_ru", "Rule of Law: Russia", "society", "governance"),
    ("RL.EST", "BR", "wb_ruleoflaw_br", "Rule of Law: Brazil", "society", "governance"),
    # ── Prevalence of undernourishment (% of population) ──
    ("SN.ITK.DEFC.ZS", "WLD", "wb_undernourish_world", "Undernourishment: World", "society", "food_security"),
    ("SN.ITK.DEFC.ZS", "IN", "wb_undernourish_in", "Undernourishment: India", "society", "food_security"),
    ("SN.ITK.DEFC.ZS", "NG", "wb_undernourish_ng", "Undernourishment: Nigeria", "society", "food_security"),
    ("SN.ITK.DEFC.ZS", "BD", "wb_undernourish_bd", "Undernourishment: Bangladesh", "society", "food_security"),
    ("SN.ITK.DEFC.ZS", "ET", "wb_undernourish_et", "Undernourishment: Ethiopia", "society", "food_security"),
    # ── Prevalence of severe food insecurity (% of population) ──
    # ── Displacement / forced movement ──
    ("SM.POP.FDIP", "WLD", "wb_forced_displaced_world", "Forcibly Displaced People: World", "society", "displacement"),
    ("SM.POP.FDIP", "US", "wb_forced_displaced_us", "Forcibly Displaced People: US", "society", "displacement"),
    ("SM.POP.FDIP", "DE", "wb_forced_displaced_de", "Forcibly Displaced People: Germany", "society", "displacement"),
    ("SM.POP.IDPC", "WLD", "wb_idp_world", "Internally Displaced People: World", "society", "displacement"),
    ("SM.POP.ASYS.EA", "WLD", "wb_asylum_world", "Asylum Seekers: World", "society", "displacement"),
    # ── Cause-of-death structure ──
    ("SP.DYN.AMRT.FE", "WLD", "wb_adult_mort_female_world", "Adult Mortality Female: World", "society", "cause_of_death"),
    ("SP.DYN.AMRT.MA", "WLD", "wb_adult_mort_male_world", "Adult Mortality Male: World", "society", "cause_of_death"),
    ("SH.DTH.NCOM.ZS", "US", "wb_death_ncd_share_us", "Deaths by NCD (%): US", "society", "cause_of_death"),
    ("SH.DTH.COMM.ZS", "US", "wb_death_comm_share_us", "Deaths by Communicable Diseases (%): US", "society", "cause_of_death"),
    ("SH.DTH.INJR.ZS", "US", "wb_death_injury_share_us", "Deaths by Injury (%): US", "society", "cause_of_death"),
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

            return build_timeseries_df(rows, f"WB {indicator}/{country}")

    _Collector.__name__ = f"WB_{name}"
    _Collector.__qualname__ = f"WB_{name}"
    return _Collector


def get_wb_collectors() -> dict[str, type[BaseCollector]]:
    return {t[2]: _make_wb_collector(*t) for t in WORLDBANK_SERIES}
