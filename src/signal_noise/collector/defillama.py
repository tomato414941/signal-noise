"""DeFi Llama collectors: TVL by chain, stablecoin market caps, DEX volume."""
from __future__ import annotations


import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

# ── Chain TVL ────────────────────────────────────────────────
# (chain_name, collector_name, display_name)
CHAIN_TVL_SERIES: list[tuple[str, str, str]] = [
    ("Ethereum", "defi_tvl_eth", "DeFi TVL: Ethereum"),
    ("Solana", "defi_tvl_sol", "DeFi TVL: Solana"),
    ("BSC", "defi_tvl_bsc", "DeFi TVL: BSC"),
    ("Bitcoin", "defi_tvl_btc", "DeFi TVL: Bitcoin"),
    ("Tron", "defi_tvl_tron", "DeFi TVL: Tron"),
    ("Base", "defi_tvl_base", "DeFi TVL: Base"),
    ("Arbitrum", "defi_tvl_arb", "DeFi TVL: Arbitrum"),
    ("Polygon", "defi_tvl_matic", "DeFi TVL: Polygon"),
    ("Avalanche", "defi_tvl_avax", "DeFi TVL: Avalanche"),
    ("Sui", "defi_tvl_sui", "DeFi TVL: Sui"),
    ("OP Mainnet", "defi_tvl_op", "DeFi TVL: OP Mainnet"),
    ("Hyperliquid L1", "defi_tvl_hyper", "DeFi TVL: Hyperliquid"),
    ("Mantle", "defi_tvl_mantle", "DeFi TVL: Mantle"),
    ("Aptos", "defi_tvl_apt", "DeFi TVL: Aptos"),
    ("TON", "defi_tvl_ton", "DeFi TVL: TON"),
    ("Cronos", "defi_tvl_cro", "DeFi TVL: Cronos"),
    ("Scroll", "defi_tvl_scroll", "DeFi TVL: Scroll"),
    ("zkSync Era", "defi_tvl_zksync", "DeFi TVL: zkSync Era"),
    ("Blast", "defi_tvl_blast", "DeFi TVL: Blast"),
    ("Linea", "defi_tvl_linea", "DeFi TVL: Linea"),
    ("Starknet", "defi_tvl_starknet", "DeFi TVL: Starknet"),
    ("Sei", "defi_tvl_sei", "DeFi TVL: Sei"),
    ("Manta", "defi_tvl_manta", "DeFi TVL: Manta"),
]


def _make_chain_tvl_collector(
    chain: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://defillama.com/docs/api",
            domain="markets",
            category="crypto",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://api.llama.fi/v2/historicalChainTvl/{chain}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            rows = [
                {
                    "date": pd.to_datetime(r["date"], unit="s", utc=True),
                    "value": float(r["tvl"]),
                }
                for r in data
                if r.get("tvl") is not None and float(r["tvl"]) > 0
            ]
            if not rows:
                raise RuntimeError(f"No TVL data for {chain}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"DeFiTVL_{name}"
    _Collector.__qualname__ = f"DeFiTVL_{name}"
    return _Collector


# ── Total TVL ────────────────────────────────────────────────

class DeFiTotalTVLCollector(BaseCollector):
    meta = CollectorMeta(
        name="defi_tvl_total",
        display_name="DeFi TVL: Total",
        update_frequency="daily",
        api_docs_url="https://defillama.com/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://api.llama.fi/v2/historicalChainTvl",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = [
            {
                "date": pd.to_datetime(r["date"], unit="s", utc=True),
                "value": float(r["tvl"]),
            }
            for r in data
            if r.get("tvl") is not None
        ]
        if not rows:
            raise RuntimeError("No total TVL data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# ── Stablecoin Market Caps ───────────────────────────────────
# (stablecoin_id, collector_name, display_name)
STABLECOIN_SERIES: list[tuple[int, str, str]] = [
    (1, "defi_sc_usdt", "Stablecoin: USDT"),
    (2, "defi_sc_usdc", "Stablecoin: USDC"),
    (5, "defi_sc_dai", "Stablecoin: DAI"),
    (6, "defi_sc_frax", "Stablecoin: FRAX"),
    (7, "defi_sc_tusd", "Stablecoin: TUSD"),
    (146, "defi_sc_usde", "Stablecoin: USDe"),
    (120, "defi_sc_pyusd", "Stablecoin: PYUSD"),
    (262, "defi_sc_usd1", "Stablecoin: WLFI USD1"),
    (209, "defi_sc_usds", "Stablecoin: USDS (Sky Dollar)"),
]


def _make_stablecoin_collector(
    sc_id: int, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://defillama.com/docs/api",
            domain="markets",
            category="crypto",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://stablecoins.llama.fi/stablecoincharts/all?stablecoin={sc_id}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            rows = []
            for r in data:
                circ = r.get("totalCirculatingUSD", {}).get("peggedUSD")
                if circ is None:
                    circ = r.get("totalCirculating", {}).get("peggedUSD")
                if circ is not None and float(circ) > 0:
                    rows.append({
                        "date": pd.to_datetime(int(r["date"]), unit="s", utc=True),
                        "value": float(circ),
                    })
            if not rows:
                raise RuntimeError(f"No stablecoin data for id={sc_id}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Stablecoin_{name}"
    _Collector.__qualname__ = f"Stablecoin_{name}"
    return _Collector


# ── DEX Total Volume ─────────────────────────────────────────

class DeFiDEXVolumeCollector(BaseCollector):
    meta = CollectorMeta(
        name="defi_dex_volume",
        display_name="DEX Daily Volume (Total)",
        update_frequency="daily",
        api_docs_url="https://defillama.com/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = (
            "https://api.llama.fi/overview/dexs"
            "?excludeTotalDataChart=false"
            "&excludeTotalDataChartBreakdown=true"
            "&dataType=dailyVolume"
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        chart = data.get("totalDataChart", [])

        rows = [
            {
                "date": pd.to_datetime(int(r[0]), unit="s", utc=True),
                "value": float(r[1]),
            }
            for r in chart
            if r[1] is not None and float(r[1]) > 0
        ]
        if not rows:
            raise RuntimeError("No DEX volume data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# ── Protocol TVL ─────────────────────────────────────────────
# (protocol_slug, collector_name, display_name)
PROTOCOL_TVL_SERIES: list[tuple[str, str, str]] = [
    ("lido", "defi_proto_lido", "Protocol TVL: Lido"),
    ("aave", "defi_proto_aave", "Protocol TVL: Aave"),
    ("eigenlayer", "defi_proto_eigen", "Protocol TVL: EigenLayer"),
    ("ether.fi-stake", "defi_proto_etherfi", "Protocol TVL: Ether.fi"),
    ("uniswap", "defi_proto_uni", "Protocol TVL: Uniswap"),
    ("sky-lending", "defi_proto_maker", "Protocol TVL: Sky (ex-Maker)"),
    ("ethena", "defi_proto_ethena", "Protocol TVL: Ethena"),
    ("jito", "defi_proto_jito", "Protocol TVL: Jito"),
    ("rocket-pool", "defi_proto_rpl", "Protocol TVL: Rocket Pool"),
    ("compound-finance", "defi_proto_comp", "Protocol TVL: Compound"),
    ("pendle", "defi_proto_pendle", "Protocol TVL: Pendle"),
    ("curve-dex", "defi_proto_crv", "Protocol TVL: Curve"),
    ("morpho", "defi_proto_morpho", "Protocol TVL: Morpho"),
    ("jupiter", "defi_proto_jup", "Protocol TVL: Jupiter"),
    ("raydium", "defi_proto_ray", "Protocol TVL: Raydium"),
    ("sparklend", "defi_proto_spark", "Protocol TVL: SparkLend"),
    ("kamino-lend", "defi_proto_kamino", "Protocol TVL: Kamino"),
    ("hyperliquid", "defi_proto_hyperliquid", "Protocol TVL: Hyperliquid"),
    ("venus", "defi_proto_venus", "Protocol TVL: Venus"),
    ("instadapp", "defi_proto_instadapp", "Protocol TVL: Instadapp"),
    ("benqi-lending", "defi_proto_benqi", "Protocol TVL: BENQI"),
    ("drift-protocol", "defi_proto_drift", "Protocol TVL: Drift"),
]


def _make_protocol_tvl_collector(
    slug: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://defillama.com/docs/api",
            domain="markets",
            category="crypto",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://api.llama.fi/protocol/{slug}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            tvl_data = data.get("tvl", [])

            rows = [
                {
                    "date": pd.to_datetime(r["date"], unit="s", utc=True),
                    "value": float(r["totalLiquidityUSD"]),
                }
                for r in tvl_data
                if r.get("totalLiquidityUSD") is not None
                and float(r["totalLiquidityUSD"]) > 0
            ]
            if not rows:
                raise RuntimeError(f"No protocol TVL for {slug}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"ProtoTVL_{name}"
    _Collector.__qualname__ = f"ProtoTVL_{name}"
    return _Collector


# ── Fees / Revenue ───────────────────────────────────────────

class DeFiTotalFeesCollector(BaseCollector):
    meta = CollectorMeta(
        name="defi_total_fees",
        display_name="DeFi Total Fees (Daily)",
        update_frequency="daily",
        api_docs_url="https://defillama.com/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = (
            "https://api.llama.fi/overview/fees"
            "?excludeTotalDataChart=false"
            "&excludeTotalDataChartBreakdown=true"
            "&dataType=dailyFees"
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        chart = data.get("totalDataChart", [])

        rows = [
            {
                "date": pd.to_datetime(int(r[0]), unit="s", utc=True),
                "value": float(r[1]),
            }
            for r in chart
            if r[1] is not None and float(r[1]) > 0
        ]
        if not rows:
            raise RuntimeError("No fees data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# ── Options Volume ───────────────────────────────────────────

class DeFiOptionsVolumeCollector(BaseCollector):
    meta = CollectorMeta(
        name="defi_options_volume",
        display_name="DeFi Options Volume (Daily)",
        update_frequency="daily",
        api_docs_url="https://defillama.com/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = (
            "https://api.llama.fi/overview/options"
            "?excludeTotalDataChart=false"
            "&excludeTotalDataChartBreakdown=true"
            "&dataType=dailyPremiumVolume"
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        chart = data.get("totalDataChart", [])

        rows = [
            {
                "date": pd.to_datetime(int(r[0]), unit="s", utc=True),
                "value": float(r[1]),
            }
            for r in chart
            if r[1] is not None and float(r[1]) > 0
        ]
        if not rows:
            raise RuntimeError("No options volume data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# ── Perps Volume ─────────────────────────────────────────────

class DeFiPerpsVolumeCollector(BaseCollector):
    meta = CollectorMeta(
        name="defi_perps_volume",
        display_name="DeFi Perps Volume (Daily)",
        update_frequency="daily",
        api_docs_url="https://defillama.com/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = (
            "https://api.llama.fi/overview/derivatives"
            "?excludeTotalDataChart=false"
            "&excludeTotalDataChartBreakdown=true"
            "&dataType=dailyVolume"
        )
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        chart = data.get("totalDataChart", [])

        rows = [
            {
                "date": pd.to_datetime(int(r[0]), unit="s", utc=True),
                "value": float(r[1]),
            }
            for r in chart
            if r[1] is not None and float(r[1]) > 0
        ]
        if not rows:
            raise RuntimeError("No perps volume data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# ── Yields (Average APY) ────────────────────────────────────

class DeFiAvgYieldCollector(BaseCollector):
    meta = CollectorMeta(
        name="defi_avg_yield",
        display_name="DeFi Avg Yield (Median APY)",
        update_frequency="daily",
        api_docs_url="https://defillama.com/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://yields.llama.fi/median"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        raw = resp.json()
        data = raw if isinstance(raw, list) else raw.get("data", [])

        rows = [
            {
                "date": pd.to_datetime(r["timestamp"][:10], utc=True),
                "value": float(r["medianAPY"]),
            }
            for r in data
            if r.get("medianAPY") is not None
        ]
        if not rows:
            raise RuntimeError("No yield data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# ── DefiLlama Chain TVL (defi category) ──────────────────────
# "total" is special: uses historicalChainTvl without chain suffix
# (chain_or_total, collector_name, display_name)
_DEFILLAMA_CHAIN_TVL_SERIES: list[tuple[str, str, str]] = [
    ("total", "defillama_tvl_total", "DeFi TVL: Total"),
    ("Ethereum", "defillama_tvl_ethereum", "DeFi TVL: Ethereum"),
    ("Solana", "defillama_tvl_solana", "DeFi TVL: Solana"),
    ("BSC", "defillama_tvl_bsc", "DeFi TVL: BSC"),
    ("Tron", "defillama_tvl_tron", "DeFi TVL: Tron"),
    ("Base", "defillama_tvl_base", "DeFi TVL: Base"),
    ("Bitcoin", "defillama_tvl_bitcoin", "DeFi TVL: Bitcoin"),
    ("Arbitrum", "defillama_tvl_arbitrum", "DeFi TVL: Arbitrum"),
]


def _make_defillama_chain_tvl_collector(
    chain: str, name: str, display_name: str,
) -> type[BaseCollector]:
    if chain == "total":
        api_url = "https://api.llama.fi/v2/historicalChainTvl"
    else:
        api_url = f"https://api.llama.fi/v2/historicalChainTvl/{chain}"

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://defillama.com/docs/api",
            domain="markets",
            category="defi",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(api_url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            rows = [
                {
                    "date": pd.to_datetime(r["date"], unit="s", utc=True),
                    "value": float(r["tvl"]),
                }
                for r in data
                if r.get("tvl") is not None and float(r["tvl"]) > 0
            ]
            if not rows:
                raise RuntimeError(f"No TVL data for {chain}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"DefiLlamaTVL_{name}"
    _Collector.__qualname__ = f"DefiLlamaTVL_{name}"
    return _Collector


# ── DefiLlama Protocol TVL (defi category) ──────────────────
# (protocol_slug, collector_name, display_name)
_DEFILLAMA_PROTOCOL_TVL_SERIES: list[tuple[str, str, str]] = [
    ("lido", "defillama_tvl_lido", "DeFi TVL: Lido"),
    ("aave-v3", "defillama_tvl_aave", "DeFi TVL: Aave V3"),
    ("eigencloud", "defillama_tvl_eigencloud", "DeFi TVL: EigenCloud"),
    ("wbtc", "defillama_tvl_wbtc", "DeFi TVL: WBTC"),
]


def _make_defillama_protocol_tvl_collector(
    slug: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://defillama.com/docs/api",
            domain="markets",
            category="defi",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://api.llama.fi/protocol/{slug}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            tvl_data = data.get("tvl", [])

            rows = [
                {
                    "date": pd.to_datetime(r["date"], unit="s", utc=True),
                    "value": float(r["totalLiquidityUSD"]),
                }
                for r in tvl_data
                if r.get("totalLiquidityUSD") is not None
                and float(r["totalLiquidityUSD"]) > 0
            ]
            if not rows:
                raise RuntimeError(f"No protocol TVL for {slug}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"DefiLlamaProtoTVL_{name}"
    _Collector.__qualname__ = f"DefiLlamaProtoTVL_{name}"
    return _Collector


# ── Yield per pool ──────────────────────────────────────────
# (pool_id, collector_name, display_name)
_YIELD_POOL_SERIES: list[tuple[str, str, str]] = [
    ("747c1d2a-c668-4682-b9f9-296708a3dd90", "defi_yield_lido_steth", "DeFi Yield: Lido stETH"),
    ("80b8bf92-b953-4c20-98ea-c9653ef2bb98", "defi_yield_binance_wbeth", "DeFi Yield: Binance wBETH"),
    ("d8c4eff5-c8a9-46fc-a888-057c4c668e72", "defi_yield_sky_susds", "DeFi Yield: Sky sUSDS"),
    ("66985a81-9c51-46ca-9977-42b4fe7bc6df", "defi_yield_ethena_susde", "DeFi Yield: Ethena sUSDe"),
    ("43641cf5-a92e-416b-bce9-27113d3c0db6", "defi_yield_maple_usdc", "DeFi Yield: Maple USDC"),
    ("d4b3c522-6127-4b89-bedf-83641cdcd2eb", "defi_yield_rocketpool", "DeFi Yield: Rocket Pool rETH"),
]


def _make_yield_pool_collector(
    pool_id: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://defillama.com/docs/api",
            domain="markets",
            category="defi",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://yields.llama.fi/chart/{pool_id}"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            raw = resp.json()
            data = raw if isinstance(raw, list) else raw.get("data", [])

            rows = [
                {
                    "date": pd.to_datetime(r["timestamp"][:10], utc=True),
                    "value": float(r["apy"]),
                }
                for r in data
                if r.get("apy") is not None
            ]
            if not rows:
                raise RuntimeError(f"No yield data for pool {pool_id}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"DefiYield_{name}"
    _Collector.__qualname__ = f"DefiYield_{name}"
    return _Collector


# ── Registry ─────────────────────────────────────────────────

def get_defillama_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {
        "defi_tvl_total": DeFiTotalTVLCollector,
        "defi_dex_volume": DeFiDEXVolumeCollector,
        "defi_total_fees": DeFiTotalFeesCollector,
        "defi_options_volume": DeFiOptionsVolumeCollector,
        "defi_perps_volume": DeFiPerpsVolumeCollector,
        "defi_avg_yield": DeFiAvgYieldCollector,
    }
    for chain, name, display in CHAIN_TVL_SERIES:
        collectors[name] = _make_chain_tvl_collector(chain, name, display)
    for sc_id, name, display in STABLECOIN_SERIES:
        collectors[name] = _make_stablecoin_collector(sc_id, name, display)
    for slug, name, display in PROTOCOL_TVL_SERIES:
        collectors[name] = _make_protocol_tvl_collector(slug, name, display)
    for chain, name, display in _DEFILLAMA_CHAIN_TVL_SERIES:
        collectors[name] = _make_defillama_chain_tvl_collector(chain, name, display)
    for slug, name, display in _DEFILLAMA_PROTOCOL_TVL_SERIES:
        collectors[name] = _make_defillama_protocol_tvl_collector(slug, name, display)
    for pool_id, name, display in _YIELD_POOL_SERIES:
        collectors[name] = _make_yield_pool_collector(pool_id, name, display)
    return collectors
