from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (article_title, collector_name, display_name, data_type)
# Article title must match the exact Wikipedia URL slug (case-sensitive)
WIKIPEDIA_PAGES: list[tuple[str, str, str, str]] = [
    # ── Fear / crisis keywords ──
    ("Recession", "wiki_recession", "Wikipedia: Recession", "fear"),
    ("Inflation", "wiki_inflation", "Wikipedia: Inflation", "fear"),
    ("Bank_run", "wiki_bank_run", "Wikipedia: Bank Run", "fear"),
    ("Stock_market_crash", "wiki_stock_crash", "Wikipedia: Stock Market Crash", "fear"),
    ("Financial_crisis", "wiki_financial_crisis", "Wikipedia: Financial Crisis", "fear"),
    ("Bankruptcy", "wiki_bankruptcy", "Wikipedia: Bankruptcy", "fear"),
    ("Unemployment", "wiki_unemployment", "Wikipedia: Unemployment", "fear"),
    ("Hyperinflation", "wiki_hyperinflation", "Wikipedia: Hyperinflation", "fear"),
    ("Debt_ceiling", "wiki_debt_ceiling", "Wikipedia: Debt Ceiling", "fear"),
    ("Quantitative_easing", "wiki_qe", "Wikipedia: Quantitative Easing", "fear"),
    # ── Safe haven / flight keywords ──
    ("Gold", "wiki_gold", "Wikipedia: Gold", "safe_haven"),
    ("United_States_Treasury_security", "wiki_treasury", "Wikipedia: US Treasury", "safe_haven"),
    ("Swiss_franc", "wiki_chf", "Wikipedia: Swiss Franc", "safe_haven"),
    # ── Crypto attention ──
    ("Ethereum", "wiki_ethereum", "Wikipedia: Ethereum", "crypto_attention"),
    ("Cryptocurrency", "wiki_cryptocurrency", "Wikipedia: Cryptocurrency", "crypto_attention"),
    ("Decentralized_finance", "wiki_defi", "Wikipedia: DeFi", "crypto_attention"),
    ("Non-fungible_token", "wiki_nft", "Wikipedia: NFT", "crypto_attention"),
    # ── Geopolitical / macro ──
    ("War", "wiki_war", "Wikipedia: War", "geopolitical"),
    ("Pandemic", "wiki_pandemic", "Wikipedia: Pandemic", "geopolitical"),
    ("Sanctions_(law)", "wiki_sanctions", "Wikipedia: Sanctions", "geopolitical"),
    ("Tariff", "wiki_tariff", "Wikipedia: Tariff", "geopolitical"),
    # ── Greed / euphoria keywords ──
    ("Bull_market", "wiki_bull_market", "Wikipedia: Bull Market", "greed"),
    ("Initial_public_offering", "wiki_ipo", "Wikipedia: IPO", "greed"),
    ("Speculation", "wiki_speculation", "Wikipedia: Speculation", "greed"),
    ("Bubble_(economics)", "wiki_bubble", "Wikipedia: Economic Bubble", "greed"),
    # ── Entertainment / attention diversion ──
    ("Super_Bowl", "wiki_super_bowl", "Wikipedia: Super Bowl", "entertainment"),
    ("FIFA_World_Cup", "wiki_world_cup", "Wikipedia: FIFA World Cup", "entertainment"),
    ("Olympic_Games", "wiki_olympics", "Wikipedia: Olympic Games", "entertainment"),
    ("UEFA_Champions_League", "wiki_ucl", "Wikipedia: Champions League", "entertainment"),
    ("Netflix", "wiki_netflix", "Wikipedia: Netflix", "entertainment"),
    ("Taylor_Swift", "wiki_taylor_swift", "Wikipedia: Taylor Swift", "entertainment"),
    ("Elon_Musk", "wiki_elon_musk", "Wikipedia: Elon Musk", "attention"),
    ("Donald_Trump", "wiki_trump", "Wikipedia: Donald Trump", "attention"),
    ("Artificial_intelligence", "wiki_ai", "Wikipedia: AI", "tech_attention"),
    ("ChatGPT", "wiki_chatgpt", "Wikipedia: ChatGPT", "tech_attention"),
    # ── Consumer behavior (Whopper inspiration) ──
    ("Whopper", "wiki_whopper", "Wikipedia: Whopper", "consumer"),
    ("Big_Mac_Index", "wiki_big_mac_index", "Wikipedia: Big Mac Index", "consumer"),
    ("Comfort_food", "wiki_comfort_food", "Wikipedia: Comfort Food", "consumer"),
    # ── Labor / regulation / central bank ──
    ("Strike_action", "wiki_strike", "Wikipedia: Strike Action", "labor"),
    ("Trade_union", "wiki_trade_union", "Wikipedia: Trade Union", "labor"),
    ("Minimum_wage", "wiki_minimum_wage", "Wikipedia: Minimum Wage", "labor"),
    ("Lawsuit", "wiki_lawsuit", "Wikipedia: Lawsuit", "regulation"),
    ("Financial_regulation", "wiki_fin_regulation", "Wikipedia: Financial Regulation", "regulation"),
    ("Federal_Reserve", "wiki_fed", "Wikipedia: Federal Reserve", "central_bank"),
    ("Interest_rate", "wiki_interest_rate", "Wikipedia: Interest Rate", "central_bank"),
    ("Central_bank", "wiki_central_bank", "Wikipedia: Central Bank", "central_bank"),
    # ── Climate / environment ──
    ("Climate_change", "wiki_climate_change", "Wikipedia: Climate Change", "climate"),
    ("Global_warming", "wiki_global_warming", "Wikipedia: Global Warming", "climate"),
    ("Carbon_dioxide_in_Earth%27s_atmosphere", "wiki_co2", "Wikipedia: CO2 Atmosphere", "climate"),
    ("Drought", "wiki_drought", "Wikipedia: Drought", "climate"),
    ("Flood", "wiki_flood", "Wikipedia: Flood", "climate"),
    ("Heat_wave", "wiki_heatwave", "Wikipedia: Heat Wave", "climate"),
    ("Tropical_cyclone", "wiki_cyclone", "Wikipedia: Tropical Cyclone", "climate"),
    ("El_Ni%C3%B1o%E2%80%93Southern_Oscillation", "wiki_enso_page", "Wikipedia: ENSO", "climate"),
    ("Renewable_energy", "wiki_renewable", "Wikipedia: Renewable Energy", "climate"),
    ("Fossil_fuel", "wiki_fossil_fuel", "Wikipedia: Fossil Fuel", "climate"),
    # ── Real estate / housing ──
    ("Real-estate_bubble", "wiki_re_bubble", "Wikipedia: Real Estate Bubble", "real_estate"),
    ("Japanese_asset_price_bubble", "wiki_jp_bubble", "Wikipedia: Japanese Asset Bubble", "real_estate"),
    ("Subprime_mortgage_crisis", "wiki_subprime", "Wikipedia: Subprime Crisis", "real_estate"),
    ("Real_estate_investment_trust", "wiki_reit", "Wikipedia: REIT", "real_estate"),
    ("Housing_affordability_index", "wiki_housing_afford", "Wikipedia: Housing Affordability", "real_estate"),
    ("Case%E2%80%93Shiller_index", "wiki_case_shiller", "Wikipedia: Case-Shiller Index", "real_estate"),
    ("Land_value_tax", "wiki_land_value_tax", "Wikipedia: Land Value Tax", "real_estate"),
    ("Affordable_housing", "wiki_affordable_housing", "Wikipedia: Affordable Housing", "real_estate"),
    # ── Logistics / transportation ──
    ("Supply_chain", "wiki_supply_chain", "Wikipedia: Supply Chain", "logistics"),
    ("Containerization", "wiki_containerization", "Wikipedia: Containerization", "logistics"),
    ("Suez_Canal", "wiki_suez_canal", "Wikipedia: Suez Canal", "logistics"),
    ("Panama_Canal", "wiki_panama_canal", "Wikipedia: Panama Canal", "logistics"),
    ("Baltic_Exchange", "wiki_baltic_exchange", "Wikipedia: Baltic Exchange", "logistics"),
    ("Aviation", "wiki_aviation", "Wikipedia: Aviation", "logistics"),
    ("Freight_transport", "wiki_freight", "Wikipedia: Freight Transport", "logistics"),
    ("Shipping_container", "wiki_shipping_container", "Wikipedia: Shipping Container", "logistics"),
]


def _make_wiki_collector(
    article: str, name: str, display_name: str, data_type: str
) -> type[BaseCollector]:
    base_url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        f"en.wikipedia/all-access/all-agents/{article}/daily/{{start}}/{{end}}"
    )

    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            data_type=data_type,
            api_docs_url="https://wikimedia.org/api/rest_v1/",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=730)
            url = base_url.format(
                start=start.strftime("%Y%m%d00"),
                end=end.strftime("%Y%m%d00"),
            )
            headers = {"User-Agent": "signal-noise/0.1 (https://github.com/tomato414941/signal-noise)"}
            resp = requests.get(url, headers=headers, timeout=self.config.request_timeout)
            resp.raise_for_status()
            items = resp.json()["items"]
            rows = [
                {
                    "date": pd.to_datetime(item["timestamp"], format="%Y%m%d00", utc=True),
                    "value": float(item["views"]),
                }
                for item in items
            ]
            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Wiki_{name}"
    _Collector.__qualname__ = f"Wiki_{name}"
    return _Collector


def get_wiki_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_wiki_collector(*t) for t in WIKIPEDIA_PAGES}
