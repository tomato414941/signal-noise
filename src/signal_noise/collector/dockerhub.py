"""Docker Hub pull count collectors.

Tracks cumulative pull counts for popular container images as a proxy
for infrastructure adoption trends.  Daily snapshots accumulate over
time so downstream analysis can compute deltas.

No API key required.  Docs: https://docs.docker.com/docker-hub/api/latest/
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://hub.docker.com/v2/repositories"

# (namespace/image, collector_name, display_name)
_IMAGES: list[tuple[str, str, str]] = [
    ("library/nginx", "docker_nginx", "Docker: nginx"),
    ("library/python", "docker_python", "Docker: python"),
    ("library/node", "docker_node", "Docker: node"),
    ("library/postgres", "docker_postgres", "Docker: postgres"),
    ("library/redis", "docker_redis", "Docker: redis"),
    ("library/mongo", "docker_mongo", "Docker: mongo"),
    ("library/alpine", "docker_alpine", "Docker: alpine"),
    ("library/ubuntu", "docker_ubuntu", "Docker: ubuntu"),
    ("library/golang", "docker_golang", "Docker: golang"),
    ("library/rust", "docker_rust", "Docker: rust"),
    ("grafana/grafana", "docker_grafana", "Docker: grafana"),
    ("prom/prometheus", "docker_prometheus", "Docker: prometheus"),
]


def _make_dockerhub_collector(
    image: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://docs.docker.com/docker-hub/api/latest/",
            domain="technology",
            category="infra",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"{_BASE_URL}/{image}/"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            pulls = data.get("pull_count")
            if pulls is None:
                raise RuntimeError(f"No pull count for {image}")
            now = pd.Timestamp.now(tz="UTC").floor("D")
            return pd.DataFrame([{"date": now, "value": float(pulls)}])

    _Collector.__name__ = f"DockerHub_{name}"
    _Collector.__qualname__ = f"DockerHub_{name}"
    return _Collector


def get_dockerhub_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_dockerhub_collector(image, name, display)
        for image, name, display in _IMAGES
    }
