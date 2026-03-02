from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CrossrefDOICollector(BaseCollector):
    """Daily DOI registration count from Crossref.

    Proxy for global scholarly publishing activity.
    High counts indicate peaks in academic output (e.g., conference deadlines).
    """

    meta = CollectorMeta(
        name="crossref_doi_daily",
        display_name="Crossref Daily DOI Registrations",
        update_frequency="daily",
        api_docs_url="https://api.crossref.org/swagger-ui/index.html",
        domain="technology",
        category="academic",
    )

    def fetch(self) -> pd.DataFrame:
        rows = []
        end = datetime.now(UTC)
        start = end - timedelta(days=90)
        cursor_date = start
        while cursor_date <= end:
            date_str = cursor_date.strftime("%Y-%m-%d")
            url = (
                f"https://api.crossref.org/works?"
                f"filter=from-index-date:{date_str},until-index-date:{date_str}"
                f"&rows=0"
            )
            headers = {
                "User-Agent": "signal-noise/0.1 (https://github.com/tomato414941/signal-noise; research)",
            }
            try:
                resp = requests.get(
                    url, headers=headers, timeout=self.config.request_timeout,
                )
                resp.raise_for_status()
                total = resp.json()["message"]["total-results"]
                rows.append({
                    "date": pd.Timestamp(date_str, tz="UTC"),
                    "value": float(total),
                })
            except Exception:
                pass
            cursor_date += timedelta(days=7)

        if not rows:
            raise RuntimeError("No Crossref data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
