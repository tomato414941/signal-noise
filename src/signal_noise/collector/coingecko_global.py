from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class _CoinGeckoGlobalCollector(BaseCollector):
    """Base for CoinGecko global market data.

    Uses the free /global endpoint (no API key required).
    Accumulates snapshots over time via parquet append.
    """

    _field_path: list[str] = []

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/global",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()["data"]

        val = data
        for key in self._field_path:
            val = val[key]

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({
            "timestamp": [ts],
            "value": [float(val)],
        })


class CG_TotalMarketCapCollector(_CoinGeckoGlobalCollector):
    _field_path = ["total_market_cap", "usd"]
    meta = SourceMeta(
        name="cg_total_mcap",
        display_name="CoinGecko Total Market Cap (USD)",
        update_frequency="hourly",
        data_type="crypto_market",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )


class CG_TotalVolumeCollector(_CoinGeckoGlobalCollector):
    _field_path = ["total_volume", "usd"]
    meta = SourceMeta(
        name="cg_total_volume",
        display_name="CoinGecko Total 24h Volume (USD)",
        update_frequency="hourly",
        data_type="crypto_market",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )


class CG_BtcDominanceCollector(_CoinGeckoGlobalCollector):
    _field_path = ["market_cap_percentage", "btc"]
    meta = SourceMeta(
        name="cg_btc_dominance",
        display_name="CoinGecko BTC Dominance %",
        update_frequency="hourly",
        data_type="crypto_market",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )


class CG_EthDominanceCollector(_CoinGeckoGlobalCollector):
    _field_path = ["market_cap_percentage", "eth"]
    meta = SourceMeta(
        name="cg_eth_dominance",
        display_name="CoinGecko ETH Dominance %",
        update_frequency="hourly",
        data_type="crypto_market",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )


class CG_ActiveCryptosCollector(_CoinGeckoGlobalCollector):
    _field_path = ["active_cryptocurrencies"]
    meta = SourceMeta(
        name="cg_active_cryptos",
        display_name="CoinGecko Active Cryptocurrencies",
        update_frequency="hourly",
        data_type="crypto_market",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )


class CG_OngoingICOsCollector(_CoinGeckoGlobalCollector):
    _field_path = ["ongoing_icos"]
    meta = SourceMeta(
        name="cg_ongoing_icos",
        display_name="CoinGecko Ongoing ICOs",
        update_frequency="hourly",
        data_type="crypto_market",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )


class CG_MarketsCollector(_CoinGeckoGlobalCollector):
    _field_path = ["markets"]
    meta = SourceMeta(
        name="cg_markets",
        display_name="CoinGecko Number of Markets",
        update_frequency="hourly",
        data_type="crypto_market",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )


class CG_MarketCapChangePct24hCollector(_CoinGeckoGlobalCollector):
    _field_path = ["market_cap_change_percentage_24h_usd"]
    meta = SourceMeta(
        name="cg_mcap_change_24h",
        display_name="CoinGecko Market Cap Change % 24h",
        update_frequency="hourly",
        data_type="crypto_market",
        api_docs_url="https://www.coingecko.com/en/api/documentation",
        domain="financial",
        category="crypto",
    )
