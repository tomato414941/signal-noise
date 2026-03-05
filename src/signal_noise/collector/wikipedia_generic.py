from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (article_title, collector_name, display_name, domain, category)
# Article title must match the exact Wikipedia URL slug (case-sensitive)
WIKIPEDIA_PAGES: list[tuple[str, str, str, str, str]] = [
    # ── Fear / crisis keywords ──
    ("Recession", "wiki_recession", "Wikipedia: Recession", "sentiment", "sentiment"),
    ("Inflation", "wiki_inflation", "Wikipedia: Inflation", "sentiment", "sentiment"),
    ("Bank_run", "wiki_bank_run", "Wikipedia: Bank Run", "sentiment", "sentiment"),
    ("Stock_market_crash", "wiki_stock_crash", "Wikipedia: Stock Market Crash", "sentiment", "sentiment"),
    ("Financial_crisis", "wiki_financial_crisis", "Wikipedia: Financial Crisis", "sentiment", "sentiment"),
    ("Bankruptcy", "wiki_bankruptcy", "Wikipedia: Bankruptcy", "sentiment", "sentiment"),
    ("Unemployment", "wiki_unemployment", "Wikipedia: Unemployment", "sentiment", "sentiment"),
    ("Hyperinflation", "wiki_hyperinflation", "Wikipedia: Hyperinflation", "sentiment", "sentiment"),
    ("Debt_ceiling", "wiki_debt_ceiling", "Wikipedia: Debt Ceiling", "sentiment", "sentiment"),
    ("Quantitative_easing", "wiki_qe", "Wikipedia: Quantitative Easing", "sentiment", "sentiment"),
    # ── Safe haven / flight keywords ──
    ("Gold", "wiki_gold", "Wikipedia: Gold", "sentiment", "sentiment"),
    ("United_States_Treasury_security", "wiki_treasury", "Wikipedia: US Treasury", "sentiment", "sentiment"),
    ("Swiss_franc", "wiki_chf", "Wikipedia: Swiss Franc", "sentiment", "sentiment"),
    # ── Crypto attention ──
    ("Ethereum", "wiki_ethereum", "Wikipedia: Ethereum", "sentiment", "attention"),
    ("Cryptocurrency", "wiki_cryptocurrency", "Wikipedia: Cryptocurrency", "sentiment", "attention"),
    ("Decentralized_finance", "wiki_defi", "Wikipedia: DeFi", "sentiment", "attention"),
    ("Non-fungible_token", "wiki_nft", "Wikipedia: NFT", "sentiment", "attention"),
    # ── Geopolitical / macro ──
    ("War", "wiki_war", "Wikipedia: War", "sentiment", "sentiment"),
    ("Pandemic", "wiki_pandemic", "Wikipedia: Pandemic", "sentiment", "sentiment"),
    ("Sanctions_(law)", "wiki_sanctions", "Wikipedia: Sanctions", "sentiment", "sentiment"),
    ("Tariff", "wiki_tariff", "Wikipedia: Tariff", "sentiment", "sentiment"),
    # ── Greed / euphoria keywords ──
    ("Bull_market", "wiki_bull_market", "Wikipedia: Bull Market", "sentiment", "sentiment"),
    ("Initial_public_offering", "wiki_ipo", "Wikipedia: IPO", "sentiment", "sentiment"),
    ("Speculation", "wiki_speculation", "Wikipedia: Speculation", "sentiment", "sentiment"),
    ("Bubble_(economics)", "wiki_bubble", "Wikipedia: Economic Bubble", "sentiment", "sentiment"),
    # ── Entertainment / attention diversion ──
    ("Super_Bowl", "wiki_super_bowl", "Wikipedia: Super Bowl", "sentiment", "attention"),
    ("FIFA_World_Cup", "wiki_world_cup", "Wikipedia: FIFA World Cup", "sentiment", "attention"),
    ("Olympic_Games", "wiki_olympics", "Wikipedia: Olympic Games", "sentiment", "attention"),
    ("UEFA_Champions_League", "wiki_ucl", "Wikipedia: Champions League", "sentiment", "attention"),
    ("Netflix", "wiki_netflix", "Wikipedia: Netflix", "sentiment", "attention"),
    ("Taylor_Swift", "wiki_taylor_swift", "Wikipedia: Taylor Swift", "sentiment", "attention"),
    ("Elon_Musk", "wiki_elon_musk", "Wikipedia: Elon Musk", "sentiment", "attention"),
    ("Donald_Trump", "wiki_trump", "Wikipedia: Donald Trump", "sentiment", "attention"),
    ("Artificial_intelligence", "wiki_ai", "Wikipedia: AI", "sentiment", "attention"),
    ("ChatGPT", "wiki_chatgpt", "Wikipedia: ChatGPT", "sentiment", "attention"),
    # ── Consumer behavior (Whopper inspiration) ──
    ("Whopper", "wiki_whopper", "Wikipedia: Whopper", "sentiment", "attention"),
    ("Big_Mac_Index", "wiki_big_mac_index", "Wikipedia: Big Mac Index", "sentiment", "attention"),
    ("Comfort_food", "wiki_comfort_food", "Wikipedia: Comfort Food", "sentiment", "attention"),
    # ── Labor / regulation / central bank ──
    ("Strike_action", "wiki_strike", "Wikipedia: Strike Action", "economy", "labor"),
    ("Trade_union", "wiki_trade_union", "Wikipedia: Trade Union", "economy", "labor"),
    ("Minimum_wage", "wiki_minimum_wage", "Wikipedia: Minimum Wage", "economy", "labor"),
    ("Lawsuit", "wiki_lawsuit", "Wikipedia: Lawsuit", "sentiment", "sentiment"),
    ("Financial_regulation", "wiki_fin_regulation", "Wikipedia: Financial Regulation", "sentiment", "sentiment"),
    ("Federal_Reserve", "wiki_fed", "Wikipedia: Federal Reserve", "sentiment", "sentiment"),
    ("Interest_rate", "wiki_interest_rate", "Wikipedia: Interest Rate", "sentiment", "sentiment"),
    ("Central_bank", "wiki_central_bank", "Wikipedia: Central Bank", "sentiment", "sentiment"),
    # ── Remote sensing / natural disasters (NOTE: remote_sensing overridden to earth/satellite) ──
    ("Satellite_imagery", "wiki_sat_imagery", "Wikipedia: Satellite Imagery", "environment", "satellite"),
    ("Remote_sensing", "wiki_remote_sensing", "Wikipedia: Remote Sensing", "environment", "satellite"),
    ("Wildfire", "wiki_wildfire", "Wikipedia: Wildfire", "environment", "seismic"),
    ("Volcanic_eruption", "wiki_volcanic", "Wikipedia: Volcanic Eruption", "environment", "seismic"),
    ("Tsunami", "wiki_tsunami", "Wikipedia: Tsunami", "environment", "seismic"),
    ("Hurricane", "wiki_hurricane", "Wikipedia: Hurricane", "environment", "seismic"),
    ("Tornado", "wiki_tornado", "Wikipedia: Tornado", "environment", "seismic"),
    ("Landslide", "wiki_landslide", "Wikipedia: Landslide", "environment", "seismic"),
    # ── Climate / environment ──
    ("Climate_change", "wiki_climate_change", "Wikipedia: Climate Change", "environment", "climate"),
    ("Global_warming", "wiki_global_warming", "Wikipedia: Global Warming", "environment", "climate"),
    ("Carbon_dioxide_in_Earth%27s_atmosphere", "wiki_co2", "Wikipedia: CO2 Atmosphere", "environment", "climate"),
    ("Drought", "wiki_drought", "Wikipedia: Drought", "environment", "climate"),
    ("Flood", "wiki_flood", "Wikipedia: Flood", "environment", "climate"),
    ("Heat_wave", "wiki_heatwave", "Wikipedia: Heat Wave", "environment", "climate"),
    ("Tropical_cyclone", "wiki_cyclone", "Wikipedia: Tropical Cyclone", "environment", "climate"),
    ("El_Ni%C3%B1o%E2%80%93Southern_Oscillation", "wiki_enso_page", "Wikipedia: ENSO", "environment", "climate"),
    ("Renewable_energy", "wiki_renewable", "Wikipedia: Renewable Energy", "environment", "climate"),
    ("Fossil_fuel", "wiki_fossil_fuel", "Wikipedia: Fossil Fuel", "environment", "climate"),
    # ── Real estate / housing ──
    ("Real-estate_bubble", "wiki_re_bubble", "Wikipedia: Real Estate Bubble", "economy", "real_estate"),
    ("Japanese_asset_price_bubble", "wiki_jp_bubble", "Wikipedia: Japanese Asset Bubble", "economy", "real_estate"),
    ("Subprime_mortgage_crisis", "wiki_subprime", "Wikipedia: Subprime Crisis", "economy", "real_estate"),
    ("Real_estate_investment_trust", "wiki_reit", "Wikipedia: REIT", "economy", "real_estate"),
    ("Housing_affordability_index", "wiki_housing_afford", "Wikipedia: Housing Affordability", "economy", "real_estate"),
    ("Case%E2%80%93Shiller_index", "wiki_case_shiller", "Wikipedia: Case-Shiller Index", "economy", "real_estate"),
    ("Land_value_tax", "wiki_land_value_tax", "Wikipedia: Land Value Tax", "economy", "real_estate"),
    ("Affordable_housing", "wiki_affordable_housing", "Wikipedia: Affordable Housing", "economy", "real_estate"),
    # ── Logistics / transportation ──
    ("Supply_chain", "wiki_supply_chain", "Wikipedia: Supply Chain", "technology", "logistics"),
    ("Containerization", "wiki_containerization", "Wikipedia: Containerization", "technology", "logistics"),
    ("Suez_Canal", "wiki_suez_canal", "Wikipedia: Suez Canal", "technology", "logistics"),
    ("Panama_Canal", "wiki_panama_canal", "Wikipedia: Panama Canal", "technology", "logistics"),
    ("Baltic_Exchange", "wiki_baltic_exchange", "Wikipedia: Baltic Exchange", "technology", "logistics"),
    ("Aviation", "wiki_aviation", "Wikipedia: Aviation", "technology", "logistics"),
    ("Freight_transport", "wiki_freight", "Wikipedia: Freight Transport", "technology", "logistics"),
    ("Shipping_container", "wiki_shipping_container", "Wikipedia: Shipping Container", "technology", "logistics"),
    # ── Weird / anomaly attention ──
    ("Area_51", "wiki_area_51", "Wikipedia: Area 51", "sentiment", "attention"),
    ("Bermuda_Triangle", "wiki_bermuda_triangle", "Wikipedia: Bermuda Triangle", "sentiment", "attention"),
    ("Bigfoot", "wiki_bigfoot", "Wikipedia: Bigfoot", "sentiment", "attention"),
    ("Loch_Ness_Monster", "wiki_loch_ness", "Wikipedia: Loch Ness Monster", "sentiment", "attention"),
    ("Mothman", "wiki_mothman", "Wikipedia: Mothman", "sentiment", "attention"),
    ("Alien_abduction", "wiki_alien_abduction", "Wikipedia: Alien Abduction", "sentiment", "attention"),
    ("Conspiracy_theory", "wiki_conspiracy", "Wikipedia: Conspiracy Theory", "sentiment", "attention"),
    ("Mandela_Effect", "wiki_mandela_effect", "Wikipedia: Mandela Effect", "sentiment", "attention"),
    ("Simulation_hypothesis", "wiki_simulation_hypothesis", "Wikipedia: Simulation Hypothesis", "sentiment", "attention"),
    ("Fermi_paradox", "wiki_fermi_paradox", "Wikipedia: Fermi Paradox", "sentiment", "attention"),
    ("Doomsday_Clock", "wiki_doomsday_clock", "Wikipedia: Doomsday Clock", "sentiment", "attention"),
    ("Zombie", "wiki_zombie", "Wikipedia: Zombie", "sentiment", "attention"),
    ("Time_travel", "wiki_time_travel", "Wikipedia: Time Travel", "sentiment", "attention"),
    ("Internet_meme", "wiki_internet_meme", "Wikipedia: Internet Meme", "sentiment", "attention"),
    ("Creepypasta", "wiki_creepypasta", "Wikipedia: Creepypasta", "sentiment", "attention"),
    ("Backrooms", "wiki_backrooms", "Wikipedia: Backrooms", "sentiment", "attention"),
    ("Cicada_3301", "wiki_cicada_3301", "Wikipedia: Cicada 3301", "sentiment", "attention"),
    ("QAnon", "wiki_qanon", "Wikipedia: QAnon", "sentiment", "attention"),
    ("Flat_Earth", "wiki_flat_earth", "Wikipedia: Flat Earth", "sentiment", "attention"),
    ("Ghost", "wiki_ghost", "Wikipedia: Ghost", "sentiment", "attention"),
]


def _make_wiki_collector(
    article: str, name: str, display_name: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    base_url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        f"en.wikipedia/all-access/all-agents/{article}/daily/{{start}}/{{end}}"
    )

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://wikimedia.org/api/rest_v1/",
            domain=domain,
            category=category,
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
