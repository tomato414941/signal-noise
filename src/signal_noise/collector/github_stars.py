"""GitHub repository star count collectors.

Tracks cumulative star counts for key repositories as a proxy for
technology adoption and developer interest.  Daily snapshots accumulate
so downstream analysis can compute growth rates.

No API key required (60 req/h unauthenticated).
Shared throttle ensures we stay well under the limit.
"""
from __future__ import annotations

import threading
import time

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_lock = threading.Lock()
_last_request: float = 0.0
_MIN_INTERVAL: float = 5.0


def _throttled_get(url: str, timeout: int = 30) -> dict:
    global _last_request
    with _lock:
        wait = _MIN_INTERVAL - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)
        resp = requests.get(url, timeout=timeout)
        _last_request = time.monotonic()
    resp.raise_for_status()
    return resp.json()


# (owner/repo, collector_name, display_name)
_REPOS: list[tuple[str, str, str]] = [
    # AI / LLM
    ("openai/openai-python", "gh_stars_openai_py", "GitHub Stars: openai-python"),
    ("anthropics/anthropic-sdk-python", "gh_stars_anthropic_py", "GitHub Stars: anthropic-sdk-python"),
    ("langchain-ai/langchain", "gh_stars_langchain", "GitHub Stars: langchain"),
    ("huggingface/transformers", "gh_stars_transformers", "GitHub Stars: transformers"),
    ("ollama/ollama", "gh_stars_ollama", "GitHub Stars: ollama"),
    ("ggml-org/llama.cpp", "gh_stars_llamacpp", "GitHub Stars: llama.cpp"),
    # Crypto / Web3
    ("ethereum/go-ethereum", "gh_stars_geth", "GitHub Stars: go-ethereum"),
    ("solana-labs/solana", "gh_stars_solana", "GitHub Stars: solana"),
    ("bitcoin/bitcoin", "gh_stars_bitcoin", "GitHub Stars: bitcoin"),
    # Infra
    ("vercel/next.js", "gh_stars_nextjs", "GitHub Stars: next.js"),
    ("denoland/deno", "gh_stars_deno", "GitHub Stars: deno"),
    ("docker/compose", "gh_stars_compose", "GitHub Stars: docker-compose"),
    ("oven-sh/bun", "gh_stars_bun", "GitHub Stars: bun"),
    ("astral-sh/ruff", "gh_stars_ruff", "GitHub Stars: ruff"),
    ("astral-sh/uv", "gh_stars_uv", "GitHub Stars: uv"),
    ("pnpm/pnpm", "gh_stars_pnpm", "GitHub Stars: pnpm"),
    ("biomejs/biome", "gh_stars_biome", "GitHub Stars: biome"),
    ("tauri-apps/tauri", "gh_stars_tauri", "GitHub Stars: tauri"),
    # Frameworks
    ("facebook/react", "gh_stars_react", "GitHub Stars: react"),
    ("vuejs/core", "gh_stars_vue", "GitHub Stars: vue"),
    ("sveltejs/svelte", "gh_stars_svelte", "GitHub Stars: svelte"),
    # ML / AI
    ("pytorch/pytorch", "gh_stars_pytorch", "GitHub Stars: pytorch"),
    ("tensorflow/tensorflow", "gh_stars_tensorflow", "GitHub Stars: tensorflow"),
    ("keras-team/keras", "gh_stars_keras", "GitHub Stars: keras"),
    # Data / Big Data
    ("apache/spark", "gh_stars_spark", "GitHub Stars: spark"),
    ("apache/kafka", "gh_stars_kafka", "GitHub Stars: kafka"),
    # DevOps / Cloud
    ("kubernetes/kubernetes", "gh_stars_kubernetes", "GitHub Stars: kubernetes"),
    ("hashicorp/terraform", "gh_stars_terraform", "GitHub Stars: terraform"),
    ("grafana/grafana", "gh_stars_grafana", "GitHub Stars: grafana"),
    ("prometheus/prometheus", "gh_stars_prometheus", "GitHub Stars: prometheus"),
]


def _make_github_stars_collector(
    repo: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url=f"https://github.com/{repo}",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://api.github.com/repos/{repo}"
            data = _throttled_get(url, timeout=self.config.request_timeout)
            stars = data.get("stargazers_count")
            if stars is None:
                raise RuntimeError(f"No star count for {repo}")
            now = pd.Timestamp.now(tz="UTC").floor("D")
            return pd.DataFrame([{"date": now, "value": float(stars)}])

    _Collector.__name__ = f"GitHubStars_{name}"
    _Collector.__qualname__ = f"GitHubStars_{name}"
    return _Collector


def get_github_stars_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_github_stars_collector(repo, name, display)
        for repo, name, display in _REPOS
    }
