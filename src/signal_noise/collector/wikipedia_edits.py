from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (article_title, collector_name, display_name)
# Track edit velocity on pages where edit wars signal emerging events
EDIT_PAGES: list[tuple[str, str, str]] = [
    ("Bitcoin", "wiki_edit_bitcoin", "Wiki Edits: Bitcoin"),
    ("Nvidia", "wiki_edit_nvidia", "Wiki Edits: Nvidia"),
    ("Federal_Reserve", "wiki_edit_fed", "Wiki Edits: Federal Reserve"),
    ("Inflation", "wiki_edit_inflation", "Wiki Edits: Inflation"),
    ("Recession", "wiki_edit_recession", "Wiki Edits: Recession"),
    ("Artificial_intelligence", "wiki_edit_ai", "Wiki Edits: AI"),
    ("Bank_run", "wiki_edit_bank_run", "Wiki Edits: Bank Run"),
    ("Stock_market_crash", "wiki_edit_crash", "Wiki Edits: Stock Market Crash"),
    ("OpenAI", "wiki_edit_openai", "Wiki Edits: OpenAI"),
    ("Donald_Trump", "wiki_edit_trump", "Wiki Edits: Donald Trump"),
    ("Taiwan", "wiki_edit_taiwan", "Wiki Edits: Taiwan"),
    ("Tariff", "wiki_edit_tariff", "Wiki Edits: Tariff"),
]


def _make_edit_collector(
    article: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://www.mediawiki.org/wiki/API:Revisions",
            domain="sentiment",
            category="attention",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=90)
            url = (
                f"https://en.wikipedia.org/w/api.php?"
                f"action=query&prop=revisions&titles={article}"
                f"&rvprop=timestamp&rvlimit=500"
                f"&rvstart={end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                f"&rvend={start.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                f"&format=json"
            )
            headers = {"User-Agent": "signal-noise/0.1 (research)"}
            resp = requests.get(
                url, headers=headers, timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            pages = data.get("query", {}).get("pages", {})
            revisions = []
            for page in pages.values():
                revisions.extend(page.get("revisions", []))

            if not revisions:
                raise RuntimeError(f"No edit data for {article}")

            by_day: dict[str, int] = {}
            for rev in revisions:
                day = rev["timestamp"][:10]
                by_day[day] = by_day.get(day, 0) + 1

            rows = [
                {"date": pd.Timestamp(day, tz="UTC"), "value": float(count)}
                for day, count in by_day.items()
            ]
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"WikiEdit_{name}"
    _Collector.__qualname__ = f"WikiEdit_{name}"
    return _Collector


def get_wiki_edit_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_edit_collector(*t) for t in EDIT_PAGES}
