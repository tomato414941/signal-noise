"""DeFi Llama collectors: TVL by chain, stablecoin market caps, DEX volume."""
from __future__ import annotations

from datetime import UTC, datetime

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
    ("Optimism", "defi_tvl_op", "DeFi TVL: Optimism"),
    ("Hyperliquid L1", "defi_tvl_hyper", "DeFi TVL: Hyperliquid"),
    ("Mantle", "defi_tvl_mantle", "DeFi TVL: Mantle"),
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
            domain="financial",
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
        domain="financial",
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
    (3, "defi_sc_dai", "Stablecoin: DAI"),
    (5, "defi_sc_frax", "Stablecoin: FRAX"),
    (6, "defi_sc_tusd", "Stablecoin: TUSD"),
    (33, "defi_sc_usde", "Stablecoin: USDe"),
    (14, "defi_sc_pyusd", "Stablecoin: PYUSD"),
    (115, "defi_sc_usd1", "Stablecoin: WLFI USD1"),
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
            domain="financial",
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
        domain="financial",
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
    ("maker", "defi_proto_maker", "Protocol TVL: Maker"),
    ("ethena", "defi_proto_ethena", "Protocol TVL: Ethena"),
    ("jito", "defi_proto_jito", "Protocol TVL: Jito"),
    ("rocket-pool", "defi_proto_rpl", "Protocol TVL: Rocket Pool"),
    ("compound-finance", "defi_proto_comp", "Protocol TVL: Compound"),
    ("pendle", "defi_proto_pendle", "Protocol TVL: Pendle"),
    ("curve-dex", "defi_proto_crv", "Protocol TVL: Curve"),
    ("morpho", "defi_proto_morpho", "Protocol TVL: Morpho"),
    ("jupiter", "defi_proto_jup", "Protocol TVL: Jupiter"),
    ("raydium", "defi_proto_ray", "Protocol TVL: Raydium"),
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
            domain="financial",
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
        domain="financial",
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


# ── Bridge Volume ────────────────────────────────────────────

class DeFiBridgeVolumeCollector(BaseCollector):
    meta = CollectorMeta(
        name="defi_bridge_volume",
        display_name="Bridge Volume (Daily)",
        update_frequency="daily",
        api_docs_url="https://defillama.com/docs/api",
        domain="financial",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = (
            "https://api.llama.fi/overview/bridges"
            "?excludeTotalDataChart=false"
            "&excludeTotalDataChartBreakdown=true"
            "&dataType=dailyBridgeVolume"
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
            raise RuntimeError("No bridge volume data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


# ── Yields (Average APY) ────────────────────────────────────

class DeFiAvgYieldCollector(BaseCollector):
    meta = CollectorMeta(
        name="defi_avg_yield",
        display_name="DeFi Avg Yield (Median APY)",
        update_frequency="daily",
        api_docs_url="https://defillama.com/docs/api",
        domain="financial",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://yields.llama.fi/chart/median"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("data", [])

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


# ── Registry ─────────────────────────────────────────────────

def get_defillama_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {
        "defi_tvl_total": DeFiTotalTVLCollector,
        "defi_dex_volume": DeFiDEXVolumeCollector,
        "defi_total_fees": DeFiTotalFeesCollector,
        "defi_bridge_volume": DeFiBridgeVolumeCollector,
        "defi_avg_yield": DeFiAvgYieldCollector,
    }
    for chain, name, display in CHAIN_TVL_SERIES:
        collectors[name] = _make_chain_tvl_collector(chain, name, display)
    for sc_id, name, display in STABLECOIN_SERIES:
        collectors[name] = _make_stablecoin_collector(sc_id, name, display)
    for slug, name, display in PROTOCOL_TVL_SERIES:
        collectors[name] = _make_protocol_tvl_collector(slug, name, display)
    return collectors
