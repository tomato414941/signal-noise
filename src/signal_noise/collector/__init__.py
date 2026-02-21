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
    return collectors


COLLECTORS = _get_collectors()


def collect_all(sources: list[str] | None = None) -> dict[str, pd.DataFrame]:
    targets = sources or list(COLLECTORS.keys())
    results: dict[str, pd.DataFrame] = {}
    for name in targets:
        cls = COLLECTORS.get(name)
        if not cls:
            log.warning("Unknown source: %s", name)
            continue
        try:
            collector = cls()
            results[name] = collector.collect()
        except Exception as e:
            log.warning("Failed to collect %s: %s", name, e)
    return results
