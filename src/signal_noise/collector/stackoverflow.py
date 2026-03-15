from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

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
    ("langchain", "so_langchain", "StackOverflow: langchain"),
    ("rust", "so_rust", "StackOverflow: rust"),
    ("typescript", "so_typescript", "StackOverflow: typescript"),
    ("python", "so_python", "StackOverflow: python"),
    ("react-native", "so_react_native", "StackOverflow: react-native"),
    ("kubernetes", "so_kubernetes", "StackOverflow: kubernetes"),
    ("docker", "so_docker", "StackOverflow: docker"),
    ("webassembly", "so_wasm", "StackOverflow: webassembly"),
    ("swift", "so_swift", "StackOverflow: swift"),
    ("go", "so_go", "StackOverflow: go"),
    ("flutter", "so_flutter", "StackOverflow: flutter"),
    ("graphql", "so_graphql", "StackOverflow: graphql"),
    ("mongodb", "so_mongodb", "StackOverflow: mongodb"),
    ("postgresql", "so_postgresql", "StackOverflow: postgresql"),
    ("redis", "so_redis", "StackOverflow: redis"),
    ("elasticsearch", "so_elasticsearch", "StackOverflow: elasticsearch"),
]


def _make_so_collector(
    tag: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://api.stackexchange.com/docs",
            domain="technology",
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
