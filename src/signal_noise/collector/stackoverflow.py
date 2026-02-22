from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (tag, collector_name, display_name)
SO_TAGS: list[tuple[str, str, str]] = [
    ("bitcoin", "so_bitcoin", "StackOverflow: bitcoin"),
    ("blockchain", "so_blockchain", "StackOverflow: blockchain"),
    ("ethereum", "so_ethereum", "StackOverflow: ethereum"),
    ("cryptocurrency", "so_crypto", "StackOverflow: cryptocurrency"),
    ("solidity", "so_solidity", "StackOverflow: solidity"),
    ("web3js", "so_web3js", "StackOverflow: web3.js"),
    ("smart-contracts", "so_smart_contracts", "StackOverflow: smart-contracts"),
    ("defi", "so_defi", "StackOverflow: defi"),
    ("nft", "so_nft", "StackOverflow: nft"),
    ("machine-learning", "so_ml", "StackOverflow: machine-learning"),
    ("artificial-intelligence", "so_ai", "StackOverflow: AI"),
    ("chatgpt", "so_chatgpt", "StackOverflow: chatgpt"),
]


def _make_so_collector(
    tag: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://api.stackexchange.com/docs",
            domain="developer",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = (
                f"https://api.stackexchange.com/2.3/questions"
                f"?order=desc&sort=creation&tagged={tag}"
                f"&site=stackoverflow&pagesize=1&filter=total"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            total = resp.json().get("total", 0)

            ts = pd.Timestamp.now(tz="UTC").floor("D")
            return pd.DataFrame({
                "date": [ts],
                "value": [float(total)],
            })

    _Collector.__name__ = f"SO_{name}"
    _Collector.__qualname__ = f"SO_{name}"
    return _Collector


def get_so_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_so_collector(*t) for t in SO_TAGS}
