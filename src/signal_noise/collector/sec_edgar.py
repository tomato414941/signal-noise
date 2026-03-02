from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (form_type, collector_name, display_name, description)
EDGAR_FORMS: list[tuple[str, str, str]] = [
    ("8-K", "edgar_8k_daily", "SEC EDGAR 8-K Filings (daily)"),
    ("10-K", "edgar_10k_daily", "SEC EDGAR 10-K Filings (daily)"),
    ("S-1", "edgar_s1_daily", "SEC EDGAR S-1 Filings (daily)"),
    ("13F-HR", "edgar_13f_daily", "SEC EDGAR 13-F Filings (daily)"),
]


def _make_edgar_collector(
    form_type: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://efts.sec.gov/LATEST/search-index",
            domain="financial",
            category="regulatory",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=90)
            url = (
                f"https://efts.sec.gov/LATEST/search-index?"
                f"q=%22{form_type}%22"
                f"&dateRange=custom"
                f"&startdt={start.strftime('%Y-%m-%d')}"
                f"&enddt={end.strftime('%Y-%m-%d')}"
                f"&forms={form_type}"
            )
            headers = {
                "User-Agent": "signal-noise research@example.com",
                "Accept": "application/json",
            }
            resp = requests.get(url, headers=headers, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])

            by_day: dict[str, int] = {}
            for hit in hits:
                source = hit.get("_source", {})
                filed = source.get("file_date", "")
                if filed:
                    day = filed[:10]
                    by_day[day] = by_day.get(day, 0) + 1

            if not by_day:
                url_fallback = (
                    f"https://efts.sec.gov/LATEST/search-index?"
                    f"q=%22{form_type}%22"
                    f"&forms={form_type}"
                )
                resp = requests.get(
                    url_fallback, headers=headers,
                    timeout=self.config.request_timeout,
                )
                resp.raise_for_status()
                total = resp.json().get("hits", {}).get("total", {})
                count = total.get("value", 0) if isinstance(total, dict) else total
                now = pd.Timestamp.now(tz="UTC").normalize()
                return pd.DataFrame([{"date": now, "value": float(count)}])

            rows = [
                {"date": pd.Timestamp(day, tz="UTC"), "value": float(count)}
                for day, count in by_day.items()
            ]
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"EDGAR_{name}"
    _Collector.__qualname__ = f"EDGAR_{name}"
    return _Collector


def get_edgar_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_edgar_collector(*t) for t in EDGAR_FORMS}
