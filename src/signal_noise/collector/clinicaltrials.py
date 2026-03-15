"""ClinicalTrials.gov collectors.

Tracks total registered clinical trials and counts by condition area.
Rising registrations in specific conditions (GLP-1, gene therapy, AI-assisted)
signal pharmaceutical R&D investment shifts.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://clinicaltrials.gov/api/v2/studies"


def _make_clinicaltrials_collector(
    name: str, display_name: str, condition: str | None,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://clinicaltrials.gov/data-api/about-api/",
            domain="society",
            category="public_health",
        )

        def fetch(self) -> pd.DataFrame:
            params: dict = {"countTotal": "true", "pageSize": "0"}
            if condition:
                params["query.cond"] = condition
            resp = requests.get(_API_URL, params=params, timeout=self.config.request_timeout)
            resp.raise_for_status()
            total = resp.json().get("totalCount")
            if total is None:
                raise RuntimeError(f"No totalCount for {name}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(total)}])

    _Collector.__name__ = f"ClinicalTrials_{name}"
    _Collector.__qualname__ = f"ClinicalTrials_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str | None]] = [
    ("ct_total", "ClinicalTrials Total Registered", None),
    ("ct_cancer", "ClinicalTrials Cancer Studies", "cancer"),
    ("ct_obesity", "ClinicalTrials Obesity Studies", "obesity"),
    ("ct_diabetes", "ClinicalTrials Diabetes Studies", "diabetes"),
    ("ct_alzheimer", "ClinicalTrials Alzheimer Studies", "alzheimer"),
    ("ct_covid", "ClinicalTrials COVID Studies", "covid"),
    ("ct_gene_therapy", "ClinicalTrials Gene Therapy Studies", "gene therapy"),
    ("ct_ai_ml", "ClinicalTrials AI/ML Studies", "artificial intelligence OR machine learning"),
    ("ct_depression", "ClinicalTrials Depression Studies", "depression"),
    ("ct_anxiety", "ClinicalTrials Anxiety Studies", "anxiety"),
    ("ct_heart_failure", "ClinicalTrials Heart Failure Studies", "heart failure"),
    ("ct_stroke", "ClinicalTrials Stroke Studies", "stroke"),
    ("ct_parkinson", "ClinicalTrials Parkinson Studies", "parkinson"),
    ("ct_rare_disease", "ClinicalTrials Rare Disease Studies", "rare disease"),
    ("ct_immunotherapy", "ClinicalTrials Immunotherapy Studies", "immunotherapy"),
    ("ct_glp1", "ClinicalTrials GLP-1 Studies", "GLP-1"),
    ("ct_cell_therapy", "ClinicalTrials Cell Therapy Studies", "cell therapy OR CAR-T"),
    ("ct_mrna", "ClinicalTrials mRNA Studies", "mRNA"),
]


def get_clinicaltrials_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_clinicaltrials_collector(name, display, cond)
        for name, display, cond in _SIGNALS
    }
