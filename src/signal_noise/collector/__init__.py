from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector

log = logging.getLogger(__name__)


def _get_collectors() -> dict[str, type[BaseCollector]]:
    from signal_noise.collector.btc_dominance import BtcDominanceCollector
    from signal_noise.collector.btc_ohlcv import BtcOhlcvCollector
    from signal_noise.collector.ccxt_generic import get_ccxt_collectors
    from signal_noise.collector.climate_indices import (
        ArcticOscillationCollector,
        EnsoCollector,
        NaoCollector,
    )
    from signal_noise.collector.difficulty import DifficultyCollector
    from signal_noise.collector.earthquake import EarthquakeCountCollector
    from signal_noise.collector.electricity import ElectricityCollector
    from signal_noise.collector.eth_btc import EthBtcCollector
    from signal_noise.collector.events import (
        MajorSportsEventCollector,
        OlympicsCollector,
        SuperBowlCollector,
    )
    from signal_noise.collector.fear_greed import FearGreedCollector
    from signal_noise.collector.geomagnetic import GeomagneticCollector
    from signal_noise.collector.github_activity import (
        BitcoinCommitsCollector,
        EthereumCommitsCollector,
    )
    from signal_noise.collector.google_trends import GoogleTrendsCollector
    from signal_noise.collector.hashrate import HashrateCollector
    from signal_noise.collector.lightning import LightningCapacityCollector
    from signal_noise.collector.mempool import MempoolFeeCollector, MempoolSizeCollector
    from signal_noise.collector.moon import MoonPhaseCollector
    from signal_noise.collector.reddit import (
        RedditBitcoinCollector,
        RedditCryptoCollector,
        RedditWsbCollector,
    )
    from signal_noise.collector.solar import SolarXrayCollector
    from signal_noise.collector.steam import SteamPlayersCollector
    from signal_noise.collector.sunspot import SunspotCollector
    from signal_noise.collector.temporal import DayOfWeekCollector, HourOfDayCollector
    from signal_noise.collector.tor import TorUsersCollector
    from signal_noise.collector.weather import NYWeatherCollector
    from signal_noise.collector.wikipedia import WikipediaBtcCollector
    from signal_noise.collector.wikipedia_generic import get_wiki_collectors
    from signal_noise.collector.aviation import FR24TotalCollector, OpenSkyTotalCollector, OpenSkyUSCollector
    from signal_noise.collector.blockchain_charts import get_blockchain_collectors
    from signal_noise.collector.coingecko_global import (
        CG_ActiveCryptosCollector,
        CG_BtcDominanceCollector,
        CG_EthDominanceCollector,
        CG_MarketCapChangePct24hCollector,
        CG_MarketsCollector,
        CG_OngoingICOsCollector,
        CG_TotalMarketCapCollector,
        CG_TotalVolumeCollector,
    )
    from signal_noise.collector.ecb_generic import get_ecb_collectors
    from signal_noise.collector.fred_generic import get_fred_collectors
    from signal_noise.collector.github_events import get_gh_events_collectors
    from signal_noise.collector.hackernews import HNBestCollector, HNNewCollector, HNTopCollector
    from signal_noise.collector.imf_generic import get_imf_collectors
    from signal_noise.collector.npm_downloads import get_npm_collectors
    from signal_noise.collector.stackoverflow import get_so_collectors
    from signal_noise.collector.treasury_generic import get_treasury_collectors
    from signal_noise.collector.wayback import get_wayback_collectors
    from signal_noise.collector.worldbank_generic import get_wb_collectors
    from signal_noise.collector.yahoo_finance import DXYCollector, GoldCollector, SP500Collector
    from signal_noise.collector.yahoo_generic import get_yahoo_collectors
    from signal_noise.collector.oecd_house_prices import get_oecd_hp_collectors
    from signal_noise.collector.bis_property import get_bis_pp_collectors
    from signal_noise.collector.open_meteo_weather import get_weather_collectors
    from signal_noise.collector.open_meteo_marine import get_marine_collectors
    from signal_noise.collector.open_meteo_air import get_air_collectors
    from signal_noise.collector.noaa_climate import (
        GlobalTempAnomalyCollector,
        LandTempAnomalyCollector,
        CO2DailyCollector,
        NASAGlobalTempCollector,
    )
    from signal_noise.collector.nasa_eonet import (
        EONETWildfireCollector,
        EONETStormCollector,
        EONETVolcanoCollector,
        EONETTotalCollector,
    )
    from signal_noise.collector.nasa_power import get_power_collectors
    from signal_noise.collector.usgs_water import get_water_collectors
    from signal_noise.collector.defillama import get_defillama_collectors
    from signal_noise.collector.cftc_cot import get_cot_collectors
    from signal_noise.collector.eia_generic import get_eia_collectors
    from signal_noise.collector.bls_generic import get_bls_collectors
    from signal_noise.collector.eurostat_generic import get_eurostat_collectors
    from signal_noise.collector.mempool_extended import get_mempool_extended_collectors
    from signal_noise.collector.gdelt import get_gdelt_collectors
    from signal_noise.collector.census_generic import get_census_collectors
    # ── New providers (batch expansion) ──
    # Infrastructure / Internet
    from signal_noise.collector.cloudflare_radar import CloudflareRadarCollector
    from signal_noise.collector.ripe_stat import RIPEPeerCountCollector
    from signal_noise.collector.letsencrypt_stats import LetsEncryptCollector
    from signal_noise.collector.submarine_cable import SubmarineCableCollector
    from signal_noise.collector.wikimedia_total import WikimediaTotalCollector
    from signal_noise.collector.github_repo_count import GitHubRepoCountCollector
    # Geophysical / Space
    from signal_noise.collector.solar_wind import SolarWindCollector
    from signal_noise.collector.kp_index import KpIndexCollector
    from signal_noise.collector.cosmic_ray import CosmicRayCollector
    from signal_noise.collector.near_earth_objects import NearEarthObjectCollector
    from signal_noise.collector.iris_seismic import IRISSeismicCollector
    from signal_noise.collector.gvp_volcano import GVPVolcanoCollector
    from signal_noise.collector.dst_index import DstIndexCollector
    from signal_noise.collector.solar_flare import SolarFlareCollector
    # Earth / Environment
    from signal_noise.collector.openaq import OpenAQCollector
    from signal_noise.collector.epa_aqi import EPAAQICollector
    from signal_noise.collector.nsidc_sea_ice import NSIDCSeaIceCollector
    from signal_noise.collector.nasa_firms import NASAFIRMSCollector
    from signal_noise.collector.sea_level import SeaLevelCollector
    from signal_noise.collector.uv_index import UVIndexCollector
    from signal_noise.collector.river_discharge import RiverDischargeCollector
    from signal_noise.collector.co2_global import CO2GlobalCollector
    from signal_noise.collector.forest_watch import GlobalForestWatchCollector
    from signal_noise.collector.precipitation import GlobalPrecipitationCollector
    # Health / Pandemic
    from signal_noise.collector.who_gho import WHOLifeExpectancyCollector
    from signal_noise.collector.owid import OWIDCovidCollector
    from signal_noise.collector.cdc_flu import CDCFluCollector
    from signal_noise.collector.who_disease_outbreak import WHODiseaseOutbreakCollector
    from signal_noise.collector.ghe_mortality import GHEMortalityCollector
    from signal_noise.collector.healthmap import ProMEDAlertCollector
    # Sentiment / Alternative Data
    from signal_noise.collector.cboe_vix import CBOEVIXCollector
    from signal_noise.collector.aaii_sentiment import AAIISentimentCollector
    from signal_noise.collector.polymarket import PolymarketVolumeCollector
    from signal_noise.collector.bitfinex_longs_shorts import BitfinexLongShortCollector
    # Developer / Tech
    from signal_noise.collector.pypi_stats import PyPIDownloadsCollector
    from signal_noise.collector.crates_io import CratesIODownloadsCollector
    from signal_noise.collector.docker_hub import DockerHubPullsCollector
    from signal_noise.collector.homebrew_stats import HomebrewInstallsCollector
    from signal_noise.collector.huggingface_stats import HuggingFaceModelsCollector
    from signal_noise.collector.gitlab_stats import GitLabProjectsCollector
    from signal_noise.collector.libraries_io import LibrariesIOCollector
    # Financial / Crypto
    from signal_noise.collector.binance_oi import BinanceOpenInterestCollector
    from signal_noise.collector.deribit_skew import DeribitSkewCollector
    from signal_noise.collector.crypto_compare import CryptoCompareVolumeCollector
    from signal_noise.collector.etherscan_gas import EtherscanGasCollector
    from signal_noise.collector.glassnode_free import GlassnodeActiveAddressesCollector
    from signal_noise.collector.santiment_free import SantimentSocialVolumeCollector
    from signal_noise.collector.alternative_btc_dominance import CoinMarketCapDominanceCollector
    from signal_noise.collector.coin_dance import CoinDanceCollector
    from signal_noise.collector.coinmetrics_free import CoinMetricsCollector
    from signal_noise.collector.messari_free import MessariCollector
    # Demographics / Macro
    from signal_noise.collector.un_population import UNPopulationCollector
    from signal_noise.collector.ilo_stats import ILOUnemploymentCollector
    from signal_noise.collector.oecd_cli import OECDCLICollector
    from signal_noise.collector.numbeo import NumbeoCollector
    from signal_noise.collector.ecb_rates import ECBDepositRateCollector
    from signal_noise.collector.ycharts_free import FedBalanceSheetCollector
    from signal_noise.collector.boj_rates import BOJPolicyRateCollector
    from signal_noise.collector.pboc_rate import PBOCLPRCollector
    from signal_noise.collector.rba_rate import RBACashRateCollector
    # Transport / Logistics
    from signal_noise.collector.freightos_bdi import FreightosBDICollector
    from signal_noise.collector.tsa_checkpoint import TSACheckpointCollector
    from signal_noise.collector.port_la import PortOfLACollector
    from signal_noise.collector.marine_traffic import MarineTrafficCollector

    collectors: dict[str, type[BaseCollector]] = {
        "day_of_week": DayOfWeekCollector,
        "hour_of_day": HourOfDayCollector,
        "btc_ohlcv": BtcOhlcvCollector,
        "fear_greed": FearGreedCollector,
        "geomagnetic": GeomagneticCollector,
        "hashrate": HashrateCollector,
        "dxy": DXYCollector,
        "gold": GoldCollector,
        "sp500": SP500Collector,
        "eth_btc": EthBtcCollector,
        "btc_dominance": BtcDominanceCollector,
        "wikipedia_btc": WikipediaBtcCollector,
        "google_trends": GoogleTrendsCollector,
        "ny_weather": NYWeatherCollector,
        "electricity": ElectricityCollector,
        # Nature / physical phenomena
        "moon_phase": MoonPhaseCollector,
        "earthquake_count": EarthquakeCountCollector,
        "solar_xray": SolarXrayCollector,
        "sunspot": SunspotCollector,
        "enso": EnsoCollector,
        "arctic_oscillation": ArcticOscillationCollector,
        "nao": NaoCollector,
        # On-chain / crypto niche
        "mempool_size": MempoolSizeCollector,
        "mempool_fee": MempoolFeeCollector,
        "lightning_capacity": LightningCapacityCollector,
        "btc_difficulty": DifficultyCollector,
        # Internet / digital behavior
        "github_bitcoin": BitcoinCommitsCollector,
        "github_ethereum": EthereumCommitsCollector,
        "tor_users": TorUsersCollector,
        "steam_players": SteamPlayersCollector,
        # Entertainment / events
        "major_sports_event": MajorSportsEventCollector,
        "super_bowl": SuperBowlCollector,
        "olympics": OlympicsCollector,
        # Social media
        "reddit_crypto": RedditCryptoCollector,
        "reddit_wsb": RedditWsbCollector,
        "reddit_bitcoin": RedditBitcoinCollector,
        # Hacker News
        "hn_top": HNTopCollector,
        "hn_best": HNBestCollector,
        "hn_new": HNNewCollector,
        # CoinGecko global
        "cg_total_mcap": CG_TotalMarketCapCollector,
        "cg_total_volume": CG_TotalVolumeCollector,
        "cg_btc_dominance": CG_BtcDominanceCollector,
        "cg_eth_dominance": CG_EthDominanceCollector,
        "cg_active_cryptos": CG_ActiveCryptosCollector,
        "cg_ongoing_icos": CG_OngoingICOsCollector,
        "cg_markets": CG_MarketsCollector,
        "cg_mcap_change_24h": CG_MarketCapChangePct24hCollector,
        # Aviation
        "opensky_total": OpenSkyTotalCollector,
        "opensky_us": OpenSkyUSCollector,
        "fr24_total": FR24TotalCollector,
        # Climate / atmosphere
        "noaa_global_temp": GlobalTempAnomalyCollector,
        "noaa_land_temp": LandTempAnomalyCollector,
        "noaa_co2_daily": CO2DailyCollector,
        "nasa_giss_temp": NASAGlobalTempCollector,
        # Natural disasters (NASA EONET)
        "eonet_wildfires": EONETWildfireCollector,
        "eonet_storms": EONETStormCollector,
        "eonet_volcanoes": EONETVolcanoCollector,
        "eonet_total": EONETTotalCollector,
        # ── Infrastructure / Internet ──
        "cloudflare_http_human": CloudflareRadarCollector,
        "ripe_peer_count": RIPEPeerCountCollector,
        "ssl_cert_issuance": LetsEncryptCollector,
        "submarine_cable_count": SubmarineCableCollector,
        "wikimedia_pageview_total": WikimediaTotalCollector,
        "github_new_repos": GitHubRepoCountCollector,
        # ── Geophysical / Space ──
        "solar_wind_speed": SolarWindCollector,
        "kp_index": KpIndexCollector,
        "cosmic_ray_flux": CosmicRayCollector,
        "neo_close_approach": NearEarthObjectCollector,
        "iris_seismic_count": IRISSeismicCollector,
        "gvp_active_volcanoes": GVPVolcanoCollector,
        "dst_index": DstIndexCollector,
        "solar_flare_count": SolarFlareCollector,
        # ── Earth / Environment ──
        "openaq_pm25": OpenAQCollector,
        "epa_aqi_us": EPAAQICollector,
        "arctic_sea_ice_extent": NSIDCSeaIceCollector,
        "nasa_active_fires": NASAFIRMSCollector,
        "global_sea_level": SeaLevelCollector,
        "uv_index_nyc": UVIndexCollector,
        "mississippi_discharge": RiverDischargeCollector,
        "co2_monthly_global": CO2GlobalCollector,
        "glad_deforestation": GlobalForestWatchCollector,
        "global_precip_index": GlobalPrecipitationCollector,
        # ── Health / Pandemic ──
        "who_life_expectancy": WHOLifeExpectancyCollector,
        "owid_covid_cases": OWIDCovidCollector,
        "cdc_ili_rate": CDCFluCollector,
        "who_disease_outbreaks": WHODiseaseOutbreakCollector,
        "who_mortality_rate": GHEMortalityCollector,
        "health_alerts": ProMEDAlertCollector,
        # ── Sentiment / Alternative Data ──
        "vix_close": CBOEVIXCollector,
        "aaii_bull_ratio": AAIISentimentCollector,
        "polymarket_volume": PolymarketVolumeCollector,
        "bitfinex_btc_ls_ratio": BitfinexLongShortCollector,
        # ── Developer / Tech ──
        "pypi_numpy_downloads": PyPIDownloadsCollector,
        "crates_serde_downloads": CratesIODownloadsCollector,
        "dockerhub_nginx_pulls": DockerHubPullsCollector,
        "homebrew_wget_installs": HomebrewInstallsCollector,
        "hf_trending_downloads": HuggingFaceModelsCollector,
        "gitlab_new_projects": GitLabProjectsCollector,
        "librariesio_new_packages": LibrariesIOCollector,
        # ── Financial / Crypto ──
        "binance_btc_oi": BinanceOpenInterestCollector,
        "deribit_btc_skew": DeribitSkewCollector,
        "cryptocompare_volume": CryptoCompareVolumeCollector,
        "eth_gas_price": EtherscanGasCollector,
        "btc_active_addresses": GlassnodeActiveAddressesCollector,
        "santiment_social_btc": SantimentSocialVolumeCollector,
        "cg_btc_dom_30d": CoinMarketCapDominanceCollector,
        "btc_node_count": CoinDanceCollector,
        "coinmetrics_btc_txcount": CoinMetricsCollector,
        "messari_btc_volume": MessariCollector,
        # ── Demographics / Macro ──
        "un_world_population": UNPopulationCollector,
        "ilo_unemployment_rate": ILOUnemploymentCollector,
        "oecd_cli": OECDCLICollector,
        "numbeo_cost_of_living": NumbeoCollector,
        "ecb_deposit_rate": ECBDepositRateCollector,
        "fed_balance_sheet": FedBalanceSheetCollector,
        "boj_policy_rate": BOJPolicyRateCollector,
        "pboc_lpr_1y": PBOCLPRCollector,
        "rba_cash_rate": RBACashRateCollector,
        # ── Transport / Logistics ──
        "freightos_bdi": FreightosBDICollector,
        "tsa_traveler_count": TSACheckpointCollector,
        "port_la_teus": PortOfLACollector,
        "global_trade_value": MarineTrafficCollector,
    }
    collectors.update(get_yahoo_collectors())
    collectors.update(get_ccxt_collectors())
    collectors.update(get_wiki_collectors())
    collectors.update(get_fred_collectors())
    collectors.update(get_wb_collectors())
    collectors.update(get_ecb_collectors())
    collectors.update(get_treasury_collectors())
    collectors.update(get_imf_collectors())
    collectors.update(get_blockchain_collectors())
    collectors.update(get_gh_events_collectors())
    collectors.update(get_so_collectors())
    collectors.update(get_npm_collectors())
    collectors.update(get_wayback_collectors())
    collectors.update(get_oecd_hp_collectors())
    collectors.update(get_bis_pp_collectors())
    collectors.update(get_weather_collectors())
    collectors.update(get_marine_collectors())
    collectors.update(get_air_collectors())
    collectors.update(get_power_collectors())
    collectors.update(get_water_collectors())
    collectors.update(get_defillama_collectors())
    collectors.update(get_cot_collectors())
    collectors.update(get_eia_collectors())
    collectors.update(get_bls_collectors())
    collectors.update(get_eurostat_collectors())
    collectors.update(get_mempool_extended_collectors())
    collectors.update(get_gdelt_collectors())
    collectors.update(get_census_collectors())

    return collectors


COLLECTORS = _get_collectors()


def collect_all(
    collectors: list[str] | None = None,
    store: "SignalStore | None" = None,
) -> dict[str, pd.DataFrame]:
    targets = collectors or list(COLLECTORS.keys())
    results: dict[str, pd.DataFrame] = {}
    for name in targets:
        cls = COLLECTORS.get(name)
        if not cls:
            log.warning("Unknown collector: %s", name)
            continue
        try:
            collector = cls()
            results[name] = collector.collect(store=store)
        except Exception as e:
            log.warning("Failed to collect %s: %s", name, e)
    return results
