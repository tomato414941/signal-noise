from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (image_name, collector_name, display_name)
DOCKER_IMAGES: list[tuple[str, str, str]] = [
    ("nginx", "dockerhub_nginx_pulls", "Docker Hub: nginx"),
    ("python", "dockerhub_python_pulls", "Docker Hub: python"),
    ("node", "dockerhub_node_pulls", "Docker Hub: node"),
    ("postgres", "dockerhub_postgres_pulls", "Docker Hub: postgres"),
    ("redis", "dockerhub_redis_pulls", "Docker Hub: redis"),
    ("ubuntu", "dockerhub_ubuntu_pulls", "Docker Hub: ubuntu"),
    ("alpine", "dockerhub_alpine_pulls", "Docker Hub: alpine"),
    ("mysql", "dockerhub_mysql_pulls", "Docker Hub: mysql"),
    ("mongo", "dockerhub_mongo_pulls", "Docker Hub: mongo"),
    ("golang", "dockerhub_golang_pulls", "Docker Hub: golang"),
]


def _make_docker_collector(
    image: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://docs.docker.com/docker-hub/api/latest/",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://hub.docker.com/v2/repositories/library/{image}/"
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            pull_count = data.get("pull_count", 0)
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(pull_count)}])

    _Collector.__name__ = f"Docker_{name}"
    _Collector.__qualname__ = f"Docker_{name}"
    return _Collector


def get_docker_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_docker_collector(*t) for t in DOCKER_IMAGES}
