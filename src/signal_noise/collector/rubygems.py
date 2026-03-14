"""RubyGems per-gem download stats.

Tracks cumulative download counts for major Ruby gems.
Reflects Ruby ecosystem health and framework adoption.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_GEM_URL = "https://rubygems.org/api/v1/gems/{gem}.json"


def _make_rubygems_collector(
    name: str, display_name: str, gem: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://guides.rubygems.org/rubygems-org-api/",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            url = _GEM_URL.format(gem=gem)
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            downloads = resp.json().get("downloads")
            if downloads is None:
                raise RuntimeError(f"No RubyGems data for {gem}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(downloads)}])

    _Collector.__name__ = f"RubyGems_{name}"
    _Collector.__qualname__ = f"RubyGems_{name}"
    return _Collector


_GEMS: list[tuple[str, str, str]] = [
    ("rubygems_rails", "RubyGems rails Downloads", "rails"),
    ("rubygems_devise", "RubyGems devise Downloads", "devise"),
    ("rubygems_sidekiq", "RubyGems sidekiq Downloads", "sidekiq"),
    ("rubygems_puma", "RubyGems puma Downloads", "puma"),
    ("rubygems_rspec", "RubyGems rspec Downloads", "rspec"),
]


def get_rubygems_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_rubygems_collector(name, display, gem)
        for name, display, gem in _GEMS
    }
