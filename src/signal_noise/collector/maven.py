"""Maven Central (Java/JVM) ecosystem stats.

Tracks total artifact count on Maven Central, reflecting JVM
ecosystem growth and Java/Kotlin package publishing velocity.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_SEARCH_URL = "https://search.maven.org/solrsearch/select"


class MavenCentralArtifactCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="maven_artifact_count",
        display_name="Maven Central Total Artifacts",
        update_frequency="daily",
        api_docs_url="https://central.sonatype.com/",
        domain="technology",
        category="developer",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _SEARCH_URL,
            params={"q": "*:*", "rows": "0", "wt": "json"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.json().get("response", {}).get("numFound")
        if count is None:
            raise RuntimeError("No Maven Central artifact count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
