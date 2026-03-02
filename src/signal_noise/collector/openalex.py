from __future__ import annotations

from datetime import UTC, datetime

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (openalex_concept_id, collector_name, display_name)
# IDs from OpenAlex concepts: https://docs.openalex.org/api-entities/concepts
FIELDS: list[tuple[str, str, str]] = [
    ("C41008148", "openalex_cs_papers", "OpenAlex: Computer Science"),
    ("C121332964", "openalex_physics_papers", "OpenAlex: Physics"),
    ("C86803240", "openalex_biology_papers", "OpenAlex: Biology"),
    ("C71924100", "openalex_medicine_papers", "OpenAlex: Medicine"),
    ("C162324750", "openalex_economics_papers", "OpenAlex: Economics"),
    ("C33923547", "openalex_math_papers", "OpenAlex: Mathematics"),
    ("C127313418", "openalex_geology_papers", "OpenAlex: Geology"),
    ("C15744967", "openalex_psychology_papers", "OpenAlex: Psychology"),
    ("C17744445", "openalex_polisci_papers", "OpenAlex: Political Science"),
    ("C205649164", "openalex_climate_papers", "OpenAlex: Climate Change"),
]


def _make_openalex_collector(
    concept_id: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="monthly",
            api_docs_url="https://docs.openalex.org/",
            domain="creativity",
            category="academic",
        )

        def fetch(self) -> pd.DataFrame:
            current_year = datetime.now(UTC).year
            rows = []
            for year in range(current_year - 5, current_year + 1):
                url = (
                    f"https://api.openalex.org/works?"
                    f"filter=concept.id:{concept_id},"
                    f"publication_year:{year}"
                    f"&per_page=1"
                )
                headers = {
                    "User-Agent": "signal-noise/0.1 (https://github.com/tomato414941/signal-noise; research)",
                }
                try:
                    resp = requests.get(
                        url, headers=headers, timeout=self.config.request_timeout,
                    )
                    resp.raise_for_status()
                    count = resp.json()["meta"]["count"]
                    date = pd.Timestamp(year=year, month=6, day=15, tz="UTC")
                    rows.append({"date": date, "value": float(count)})
                except Exception:
                    continue
            if not rows:
                raise RuntimeError(f"No OpenAlex data for {concept_id}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"OpenAlex_{name}"
    _Collector.__qualname__ = f"OpenAlex_{name}"
    return _Collector


def get_openalex_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_openalex_collector(*t) for t in FIELDS}
