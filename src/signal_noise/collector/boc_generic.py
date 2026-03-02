from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (series_code, collector_name, display_name, frequency, domain, category)
BOC_SERIES: list[tuple[str, str, str, str, str, str]] = [
    # ── Exchange rates (daily) ──
    ("FXUSDCAD", "boc_usd_cad", "BOC: USD/CAD", "daily", "markets", "forex"),
    ("FXEURCAD", "boc_eur_cad", "BOC: EUR/CAD", "daily", "markets", "forex"),
    ("FXGBPCAD", "boc_gbp_cad", "BOC: GBP/CAD", "daily", "markets", "forex"),
    ("FXJPYCAD", "boc_jpy_cad", "BOC: JPY/CAD", "daily", "markets", "forex"),
    ("FXCNYCAD", "boc_cny_cad", "BOC: CNY/CAD", "daily", "markets", "forex"),
    ("FXAUDCAD", "boc_aud_cad", "BOC: AUD/CAD", "daily", "markets", "forex"),
    # ── Policy rate ──
    ("V39079", "boc_target_rate", "BOC: Overnight Target Rate", "daily", "markets", "rates"),
    # ── Government bond yields ──
    ("BD.CDN.2YR.DQ.YLD", "boc_yield_2y", "BOC: Canada 2Y Yield", "daily", "markets", "rates"),
    ("BD.CDN.5YR.DQ.YLD", "boc_yield_5y", "BOC: Canada 5Y Yield", "daily", "markets", "rates"),
    ("BD.CDN.10YR.DQ.YLD", "boc_yield_10y", "BOC: Canada 10Y Yield", "daily", "markets", "rates"),
]


def _make_boc_collector(
    series_code: str,
    name: str,
    display_name: str,
    frequency: str,
    domain: str,
    category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency=frequency,
            api_docs_url="https://www.bankofcanada.ca/valet/docs",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=365 * 5)
            url = (
                f"https://www.bankofcanada.ca/valet/observations/{series_code}/json"
                f"?start_date={start.strftime('%Y-%m-%d')}"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            payload = resp.json()
            observations = payload.get("observations", [])

            rows: list[dict] = []
            for obs in observations:
                series_data = obs.get(series_code, {})
                val = series_data.get("v")
                date_str = obs.get("d")
                if val is None or val == "" or date_str is None:
                    continue
                try:
                    rows.append({
                        "date": pd.to_datetime(date_str, utc=True),
                        "value": float(val),
                    })
                except (ValueError, TypeError):
                    continue

            if not rows:
                raise RuntimeError(f"No data for BOC series {series_code}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"BOC_{name}"
    _Collector.__qualname__ = f"BOC_{name}"
    return _Collector


def get_boc_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_boc_collector(*t) for t in BOC_SERIES}
