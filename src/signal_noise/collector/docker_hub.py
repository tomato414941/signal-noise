from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class DockerHubPullsCollector(BaseCollector):
    """Docker Hub total pull count for nginx (proxy for container adoption).

    Records the cumulative pull count as a daily snapshot
    from the public Docker Hub registry API.
    """

    meta = CollectorMeta(
        name="dockerhub_nginx_pulls",
        display_name="Docker Hub nginx Total Pulls",
        update_frequency="daily",
        api_docs_url="https://docs.docker.com/docker-hub/api/latest/",
        domain="developer",
        category="developer",
    )

    URL = "https://hub.docker.com/v2/repositories/library/nginx/"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        pull_count = data.get("pull_count", 0)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(pull_count)}])
