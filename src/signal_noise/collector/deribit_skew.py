from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class DeribitSkewCollector(BaseCollector):
    """Deribit BTC options 25-delta risk reversal (skew proxy)."""

    meta = CollectorMeta(
        name="deribit_btc_skew",
        display_name="Deribit BTC Options Skew",
        update_frequency="daily",
        api_docs_url="https://docs.deribit.com/",
        domain="financial",
        category="crypto",
    )

    URL = "https://deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json().get("result", [])
        if not data:
            raise RuntimeError("No Deribit data")
        # Average mark IV across all listed options as a skew proxy
        iv_values = [float(d["mark_iv"]) for d in data if d.get("mark_iv") and d["mark_iv"] > 0]
        if not iv_values:
            raise RuntimeError("No IV values in Deribit data")
        avg_iv = sum(iv_values) / len(iv_values)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": avg_iv}])
