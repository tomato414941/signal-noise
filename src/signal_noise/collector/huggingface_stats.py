from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class HuggingFaceModelsCollector(BaseCollector):
    """Hugging Face Hub — total trending model downloads (daily snapshot)."""

    meta = CollectorMeta(
        name="hf_trending_downloads",
        display_name="Hugging Face Trending Model Downloads",
        update_frequency="daily",
        api_docs_url="https://huggingface.co/docs/hub/api",
        domain="developer",
        category="developer",
    )

    URL = "https://huggingface.co/api/models?sort=downloads&direction=-1&limit=50"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        models = resp.json()
        if not models:
            raise RuntimeError("No Hugging Face data")
        total = sum(m.get("downloads", 0) for m in models)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
