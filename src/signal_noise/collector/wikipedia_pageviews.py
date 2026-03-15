"""Wikipedia Pageviews panic/interest-indicator collectors.

Uses the Wikimedia REST API to fetch daily pageview counts for articles
that serve as proxies for public panic or interest in specific topics.

No API key required.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_USER_AGENT = "signal-noise/0.1 (https://github.com/user/signal-noise)"

# (article_title, collector_name, display_name)
_PAGEVIEW_SERIES: list[tuple[str, str, str]] = [
    ("Bitcoin", "wikipedia_pv_bitcoin", "Wikipedia PV: Bitcoin"),
    ("Stock_market_crash", "wikipedia_pv_stock_crash", "Wikipedia PV: Stock Market Crash"),
    ("Recession", "wikipedia_pv_recession", "Wikipedia PV: Recession"),
    ("Bank_run", "wikipedia_pv_bank_run", "Wikipedia PV: Bank Run"),
    ("Inflation", "wikipedia_pv_inflation", "Wikipedia PV: Inflation"),
    ("Federal_Reserve", "wikipedia_pv_fed", "Wikipedia PV: Federal Reserve"),
    ("Gold", "wikipedia_pv_gold", "Wikipedia PV: Gold"),
    ("Cryptocurrency", "wikipedia_pv_crypto", "Wikipedia PV: Cryptocurrency"),
    ("Artificial_intelligence", "wikipedia_pv_ai", "Wikipedia PV: AI"),
    ("Pandemic", "wikipedia_pv_pandemic", "Wikipedia PV: Pandemic"),
    ("Nuclear_weapon", "wikipedia_pv_nuclear", "Wikipedia PV: Nuclear Weapon"),
    ("Climate_change", "wikipedia_pv_climate", "Wikipedia PV: Climate Change"),
    ("United_States", "wikipedia_pv_us", "Wikipedia PV: United States"),
    ("World_War_II", "wikipedia_pv_ww2", "Wikipedia PV: World War II"),
    ("COVID-19_pandemic", "wikipedia_pv_covid", "Wikipedia PV: COVID-19 Pandemic"),
    ("Taylor_Swift", "wikipedia_pv_taylor_swift", "Wikipedia PV: Taylor Swift"),
    ("Elon_Musk", "wikipedia_pv_elon_musk", "Wikipedia PV: Elon Musk"),
    ("Unemployment", "wikipedia_pv_unemployment", "Wikipedia PV: Unemployment"),
    ("Interest_rate", "wikipedia_pv_interest_rate", "Wikipedia PV: Interest Rate"),
    ("Hyperinflation", "wikipedia_pv_hyperinflation", "Wikipedia PV: Hyperinflation"),
    ("Bankruptcy", "wikipedia_pv_bankruptcy", "Wikipedia PV: Bankruptcy"),
    ("Data_breach", "wikipedia_pv_data_breach", "Wikipedia PV: Data Breach"),
    ("Supply_chain", "wikipedia_pv_supply_chain", "Wikipedia PV: Supply Chain"),
    ("Tariff", "wikipedia_pv_tariff", "Wikipedia PV: Tariff"),
    ("Stagflation", "wikipedia_pv_stagflation", "Wikipedia PV: Stagflation"),
    ("Housing_bubble", "wikipedia_pv_housing_bubble", "Wikipedia PV: Housing Bubble"),
    ("Sovereign_debt", "wikipedia_pv_sovereign_debt", "Wikipedia PV: Sovereign Debt"),
    ("Great_Depression", "wikipedia_pv_great_depression", "Wikipedia PV: Great Depression"),
    ("Famine", "wikipedia_pv_famine", "Wikipedia PV: Famine"),
]


def _make_wikipedia_pageviews_collector(
    article: str, name: str, display_name: str,
) -> type[BaseCollector]:
    base_url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        f"en.wikipedia/all-access/all-agents/{article}/daily/{{start}}/{{end}}"
    )

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://wikimedia.org/api/rest_v1/",
            domain="sentiment",
            category="sentiment",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=90)
            url = base_url.format(
                start=start.strftime("%Y%m%d00"),
                end=end.strftime("%Y%m%d00"),
            )
            resp = requests.get(
                url,
                headers={"User-Agent": _USER_AGENT},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            items = resp.json()["items"]
            rows = [
                {
                    "date": pd.to_datetime(item["timestamp"], format="%Y%m%d00", utc=True),
                    "value": float(item["views"]),
                }
                for item in items
            ]
            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"WikiPV_{name}"
    _Collector.__qualname__ = f"WikiPV_{name}"
    return _Collector


def get_wikipedia_pageviews_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_wikipedia_pageviews_collector(article, name, display)
        for article, name, display in _PAGEVIEW_SERIES
    }
