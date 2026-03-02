from __future__ import annotations

from datetime import UTC, datetime

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (search_query, collector_name, display_name)
S2_FIELDS: list[tuple[str, str, str]] = [
    ("artificial intelligence", "s2_ai_papers", "S2: AI Papers (yearly)"),
    ("large language model", "s2_llm_papers", "S2: LLM Papers (yearly)"),
    ("climate change", "s2_climate_papers", "S2: Climate Change Papers (yearly)"),
    ("quantum computing", "s2_quantum_papers", "S2: Quantum Computing Papers (yearly)"),
    ("CRISPR gene editing", "s2_crispr_papers", "S2: CRISPR Papers (yearly)"),
    ("cybersecurity", "s2_cybersec_papers", "S2: Cybersecurity Papers (yearly)"),
    ("nuclear fusion energy", "s2_fusion_papers", "S2: Nuclear Fusion Papers (yearly)"),
]


def _make_s2_collector(
    query: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="monthly",
            api_docs_url="https://api.semanticscholar.org/",
            domain="technology",
            category="academic",
        )

        def fetch(self) -> pd.DataFrame:
            current_year = datetime.now(UTC).year
            rows = []
            for year in range(current_year - 5, current_year + 1):
                url = (
                    f"https://api.semanticscholar.org/graph/v1/paper/search?"
                    f"query={query.replace(' ', '+')}"
                    f"&year={year}"
                    f"&limit=1"
                    f"&fields=title"
                )
                headers = {"User-Agent": "signal-noise/0.1 (research)"}
                try:
                    resp = requests.get(
                        url, headers=headers,
                        timeout=self.config.request_timeout,
                    )
                    resp.raise_for_status()
                    total = resp.json().get("total", 0)
                    date = pd.Timestamp(year=year, month=6, day=15, tz="UTC")
                    rows.append({"date": date, "value": float(total)})
                except Exception:
                    continue
            if not rows:
                raise RuntimeError(f"No Semantic Scholar data for '{query}'")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"S2_{name}"
    _Collector.__qualname__ = f"S2_{name}"
    return _Collector


def get_s2_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_s2_collector(*t) for t in S2_FIELDS}
