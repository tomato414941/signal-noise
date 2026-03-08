from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

INATURALIST_API_DOCS = "https://api.inaturalist.org/v1/docs/"
INATURALIST_HEADERS = {"User-Agent": "signal-noise/0.1 (research)"}

# (endpoint, iconic_taxa, collector_name, display_name, category)
INATURALIST_SERIES: list[tuple[str, str | None, str, str, str]] = [
    (
        "observations",
        None,
        "inaturalist_observations_total",
        "iNaturalist Verifiable Observations",
        "biodiversity",
    ),
    (
        "observations",
        "Aves",
        "inaturalist_observations_birds",
        "iNaturalist Bird Observations",
        "wildlife",
    ),
    (
        "observations",
        "Mammalia",
        "inaturalist_observations_mammals",
        "iNaturalist Mammal Observations",
        "wildlife",
    ),
    (
        "observations",
        "Amphibia",
        "inaturalist_observations_amphibians",
        "iNaturalist Amphibian Observations",
        "wildlife",
    ),
    (
        "observations",
        "Reptilia",
        "inaturalist_observations_reptiles",
        "iNaturalist Reptile Observations",
        "wildlife",
    ),
    (
        "observations/species_counts",
        None,
        "inaturalist_species_total",
        "iNaturalist Species Count",
        "biodiversity",
    ),
    (
        "observations/species_counts",
        "Plantae",
        "inaturalist_species_plants",
        "iNaturalist Plant Species Count",
        "biodiversity",
    ),
    (
        "observations/species_counts",
        "Insecta",
        "inaturalist_species_insects",
        "iNaturalist Insect Species Count",
        "biodiversity",
    ),
]


def _fetch_inaturalist_total_results(
    endpoint: str,
    iconic_taxa: str | None,
    timeout: float,
) -> int:
    params = {
        "per_page": 1,
        "verifiable": "true",
    }
    if iconic_taxa:
        params["iconic_taxa"] = iconic_taxa

    resp = requests.get(
        f"https://api.inaturalist.org/v1/{endpoint}",
        params=params,
        headers=INATURALIST_HEADERS,
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    total_results = data.get("total_results") if isinstance(data, dict) else None
    if total_results is None:
        raise RuntimeError(f"No iNaturalist total_results for {endpoint}")
    return int(total_results)


def _make_inaturalist_collector(
    endpoint: str,
    iconic_taxa: str | None,
    name: str,
    display_name: str,
    category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url=INATURALIST_API_DOCS,
            domain="environment",
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            count = _fetch_inaturalist_total_results(
                endpoint=endpoint,
                iconic_taxa=iconic_taxa,
                timeout=self.config.request_timeout,
            )
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(count)}])

    _Collector.__name__ = f"INaturalist{name.title()}Collector"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def get_inaturalist_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_inaturalist_collector(endpoint, iconic_taxa, name, display_name, category)
        for endpoint, iconic_taxa, name, display_name, category in INATURALIST_SERIES
    }
