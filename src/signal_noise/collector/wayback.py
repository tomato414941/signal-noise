from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (domain, collector_name, display_name)
WAYBACK_SITES: list[tuple[str, str, str]] = [
    ("bitcoin.org", "wb_bitcoin_org", "Wayback: bitcoin.org"),
    ("ethereum.org", "wb_ethereum_org", "Wayback: ethereum.org"),
    ("coinbase.com", "wb_coinbase", "Wayback: coinbase.com"),
    ("binance.com", "wb_binance", "Wayback: binance.com"),
    ("sec.gov", "wb_sec_gov", "Wayback: sec.gov"),
    ("federalreserve.gov", "wb_fed_gov", "Wayback: federalreserve.gov"),
]


def _make_wayback_collector(
    domain: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            data_type="web_attention",
            api_docs_url="https://web.archive.org/",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=730)
            url = (
                f"https://web.archive.org/cdx/search/cdx"
                f"?url={domain}&output=json&fl=timestamp"
                f"&from={start.strftime('%Y%m%d')}"
                f"&to={end.strftime('%Y%m%d')}"
                f"&limit=50000"
            )
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if len(data) < 2:
                raise RuntimeError(f"No Wayback data for {domain}")

            # First row is header ["timestamp"], rest are data
            day_counts: Counter[str] = Counter()
            for row in data[1:]:
                ts_str = row[0]
                day = ts_str[:8]  # YYYYMMDD
                day_counts[day] += 1

            rows = [
                {
                    "date": pd.to_datetime(day, format="%Y%m%d", utc=True),
                    "value": float(count),
                }
                for day, count in sorted(day_counts.items())
            ]

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"WB_{name}"
    _Collector.__qualname__ = f"WB_{name}"
    return _Collector


def get_wayback_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_wayback_collector(*t) for t in WAYBACK_SITES}
