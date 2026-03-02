"""Extended Mempool.space collectors for Bitcoin on-chain data.

No API key required.  Docs: https://mempool.space/docs/api
"""
from __future__ import annotations


import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE = "https://mempool.space/api/v1"

# ── Mining pool hashrate collectors ────────────────────────────
MINING_POOLS: list[tuple[str, str, str]] = [
    ("foundryusa", "mp_foundry", "Mempool Foundry USA Hashrate"),
    ("antpool", "mp_antpool", "Mempool AntPool Hashrate"),
    ("f2pool", "mp_f2pool", "Mempool F2Pool Hashrate"),
    ("binance-pool", "mp_binance_pool", "Mempool Binance Pool Hashrate"),
    ("viabtc", "mp_viabtc", "Mempool ViaBTC Hashrate"),
    ("mara-pool", "mp_mara", "Mempool MARA Pool Hashrate"),
    ("spiderpool", "mp_spiderpool", "Mempool SpiderPool Hashrate"),
    ("luxor", "mp_luxor", "Mempool Luxor Hashrate"),
    ("sbicrypto", "mp_sbi", "Mempool SBI Crypto Hashrate"),
    ("ocean", "mp_ocean", "Mempool OCEAN Hashrate"),
]


def _make_pool_hashrate_collector(
    slug: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://mempool.space/docs/api",
            domain="markets",
            category="crypto",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"{_BASE}/mining/pool/{slug}/hashrate"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            # API returns direct list (not {"hashrates": [...]})
            items = data if isinstance(data, list) else data.get("hashrates", [])
            rows = []
            for r in items:
                ts = r.get("timestamp")
                hr = r.get("avgHashrate")
                if ts and hr is not None:
                    rows.append({
                        "date": pd.to_datetime(ts, unit="s", utc=True),
                        "value": float(hr),
                    })
            if not rows:
                raise RuntimeError(f"No hashrate data for pool {slug}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"MPool_{name}"
    _Collector.__qualname__ = f"MPool_{name}"
    return _Collector


# ── Network-level collectors ───────────────────────────────────

class MempoolBlocksMinedCollector(BaseCollector):
    meta = CollectorMeta(
        name="mp_blocks_mined",
        display_name="Mempool Blocks Mined (24h)",
        update_frequency="daily",
        api_docs_url="https://mempool.space/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = f"{_BASE}/mining/hashrate/1y"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for r in data.get("difficulty", []):
            ts = r.get("time") or r.get("timestamp")
            val = r.get("difficulty")
            if ts and val is not None:
                rows.append({
                    "date": pd.to_datetime(ts, unit="s", utc=True),
                    "value": float(val),
                })
        if not rows:
            raise RuntimeError("No blocks data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class MempoolNetworkHashrateCollector(BaseCollector):
    meta = CollectorMeta(
        name="mp_network_hashrate",
        display_name="Mempool Network Hashrate",
        update_frequency="daily",
        api_docs_url="https://mempool.space/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = f"{_BASE}/mining/hashrate/1y"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for r in data.get("hashrates", []):
            ts = r.get("timestamp")
            hr = r.get("avgHashrate")
            if ts and hr is not None:
                rows.append({
                    "date": pd.to_datetime(ts, unit="s", utc=True),
                    "value": float(hr),
                })
        if not rows:
            raise RuntimeError("No network hashrate data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class MempoolTxCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="mp_tx_count",
        display_name="Mempool TX Count (recent blocks)",
        update_frequency="hourly",
        api_docs_url="https://mempool.space/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://mempool.space/api/blocks"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        blocks = resp.json()
        rows = []
        for b in blocks:
            ts = b.get("timestamp")
            tc = b.get("tx_count")
            if ts and tc is not None:
                rows.append({
                    "date": pd.to_datetime(ts, unit="s", utc=True),
                    "value": float(tc),
                })
        if not rows:
            raise RuntimeError("No block tx count data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class MempoolBlockSizeCollector(BaseCollector):
    meta = CollectorMeta(
        name="mp_block_size",
        display_name="Mempool Avg Block Size",
        update_frequency="hourly",
        api_docs_url="https://mempool.space/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://mempool.space/api/blocks"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        blocks = resp.json()
        rows = []
        for b in blocks:
            ts = b.get("timestamp")
            sz = b.get("size")
            if ts and sz is not None:
                rows.append({
                    "date": pd.to_datetime(ts, unit="s", utc=True),
                    "value": float(sz),
                })
        if not rows:
            raise RuntimeError("No block size data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class MempoolBlockWeightCollector(BaseCollector):
    meta = CollectorMeta(
        name="mp_block_weight",
        display_name="Mempool Avg Block Weight",
        update_frequency="hourly",
        api_docs_url="https://mempool.space/docs/api",
        domain="markets",
        category="crypto",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://mempool.space/api/blocks"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        blocks = resp.json()
        rows = []
        for b in blocks:
            ts = b.get("timestamp")
            w = b.get("weight")
            if ts and w is not None:
                rows.append({
                    "date": pd.to_datetime(ts, unit="s", utc=True),
                    "value": float(w),
                })
        if not rows:
            raise RuntimeError("No block weight data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def get_mempool_extended_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {
        "mp_blocks_mined": MempoolBlocksMinedCollector,
        "mp_network_hashrate": MempoolNetworkHashrateCollector,
        "mp_tx_count": MempoolTxCountCollector,
        "mp_block_size": MempoolBlockSizeCollector,
        "mp_block_weight": MempoolBlockWeightCollector,
    }
    for slug, name, display in MINING_POOLS:
        collectors[name] = _make_pool_hashrate_collector(slug, name, display)
    return collectors
