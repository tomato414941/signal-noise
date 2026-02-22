from __future__ import annotations

from collections import Counter

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

_GITHUB_HEADERS = {"Accept": "application/vnd.github+json"}

# (owner/repo, collector_name, display_name)
GITHUB_REPOS: list[tuple[str, str, str]] = [
    ("bitcoin/bitcoin", "gh_events_bitcoin", "GitHub Events: bitcoin/bitcoin"),
    ("ethereum/go-ethereum", "gh_events_geth", "GitHub Events: go-ethereum"),
    ("solana-labs/solana", "gh_events_solana", "GitHub Events: solana"),
    ("rust-lang/rust", "gh_events_rust", "GitHub Events: rust"),
    ("pytorch/pytorch", "gh_events_pytorch", "GitHub Events: pytorch"),
    ("openai/openai-python", "gh_events_openai", "GitHub Events: openai-python"),
    ("langchain-ai/langchain", "gh_events_langchain", "GitHub Events: langchain"),
    ("Lightning-AI/pytorch-lightning", "gh_events_lightning", "GitHub Events: pytorch-lightning"),
]


def _make_gh_events_collector(
    repo: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            data_type="dev_activity",
            api_docs_url=f"https://github.com/{repo}",
            domain="developer",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://api.github.com/repos/{repo}/events?per_page=100"
            resp = requests.get(
                url, headers=_GITHUB_HEADERS, timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            events = resp.json()

            event_counts = Counter(e.get("type", "unknown") for e in events)
            total = len(events)

            ts = pd.Timestamp.now(tz="UTC").floor("h")
            return pd.DataFrame({
                "timestamp": [ts],
                "value": [float(total)],
            })

    _Collector.__name__ = f"GHE_{name}"
    _Collector.__qualname__ = f"GHE_{name}"
    return _Collector


def get_gh_events_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_gh_events_collector(*t) for t in GITHUB_REPOS}
