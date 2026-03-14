"""US Federal Register document count collector.

Tracks daily new federal documents (rules, proposed rules,
notices, presidential documents). Volume reflects regulatory
activity and executive branch output.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://www.federalregister.gov/api/v1/documents"


def _make_fedreg_collector(
    name: str, display_name: str, doc_type: str | None,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://www.federalregister.gov/developers/documentation/api/v1",
            domain="society",
            category="legislation",
        )

        def fetch(self) -> pd.DataFrame:
            today = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
            params: dict = {
                "per_page": "1",
                "conditions[publication_date][gte]": today,
            }
            if doc_type:
                params["conditions[type]"] = doc_type
            resp = requests.get(_API_URL, params=params, timeout=self.config.request_timeout)
            resp.raise_for_status()
            count = resp.json().get("count", 0)
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(count)}])

    _Collector.__name__ = f"FedReg_{name}"
    _Collector.__qualname__ = f"FedReg_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str | None]] = [
    ("fedreg_daily_total", "Federal Register Daily Documents", None),
    ("fedreg_daily_rules", "Federal Register Daily Rules", "RULE"),
    ("fedreg_daily_proposed", "Federal Register Daily Proposed Rules", "PRORULE"),
    ("fedreg_daily_notices", "Federal Register Daily Notices", "NOTICE"),
]


def get_federal_register_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_fedreg_collector(name, display, dt)
        for name, display, dt in _SIGNALS
    }
