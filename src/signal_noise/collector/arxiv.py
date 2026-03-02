"""arXiv — daily new submission counts by category."""
from __future__ import annotations

import requests
import pandas as pd
import xml.etree.ElementTree as ET

from signal_noise.collector.base import BaseCollector, CollectorMeta

_OAI_URL = "https://export.arxiv.org/oai2"

_CATEGORIES = [
    ("cs", "arxiv_submissions_cs", "arXiv CS Daily Submissions"),
    ("physics", "arxiv_submissions_physics", "arXiv Physics Daily Submissions"),
]


def _make_arxiv_collector(
    category: str, name: str, display_name: str,
) -> type[BaseCollector]:

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://info.arxiv.org/help/api/index.html",
            domain="creativity",
            category="academic",
        )

        SEARCH_URL = "https://export.arxiv.org/api/query"

        def fetch(self) -> pd.DataFrame:
            rows = []
            for days_ago in range(7):
                date = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_ago + 1))
                start = date.strftime("%Y%m%d0000")
                end = date.strftime("%Y%m%d2359")
                params = {
                    "search_query": f"cat:{category}*",
                    "start": 0,
                    "max_results": 1,
                    "sortBy": "submittedDate",
                    "sortOrder": "descending",
                    "submittedDate": f"[{start}+TO+{end}]",
                }
                try:
                    resp = requests.get(
                        self.SEARCH_URL,
                        params=params,
                        timeout=self.config.request_timeout,
                    )
                    resp.raise_for_status()
                    root = ET.fromstring(resp.text)
                    ns = {"atom": "http://www.w3.org/2005/Atom", "opensearch": "http://a9.com/-/spec/opensearch/1.1/"}
                    total_el = root.find("opensearch:totalResults", ns)
                    count = int(total_el.text) if total_el is not None else 0
                    rows.append({
                        "date": date.normalize(),
                        "value": count,
                    })
                except Exception:
                    continue
            if not rows:
                raise RuntimeError(f"No arXiv data for {category}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"ArXiv_{name}"
    _Collector.__qualname__ = f"ArXiv_{name}"
    return _Collector


def get_arxiv_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_arxiv_collector(cat, name, display)
        for cat, name, display in _CATEGORIES
    }
