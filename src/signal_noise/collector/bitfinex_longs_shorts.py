from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class BitfinexLongShortCollector(BaseCollector):
    """Bitfinex BTC long/short ratio (margin positions)."""

    meta = CollectorMeta(
        name="bitfinex_btc_ls_ratio",
        display_name="Bitfinex BTC Long/Short Ratio",
        update_frequency="daily",
        api_docs_url="https://docs.bitfinex.com/reference/rest-public-stats1",
        domain="sentiment",
        category="sentiment",
    )

    LONGS_URL = "https://api-pub.bitfinex.com/v2/stats1/pos.size:1m:tBTCUSD:long/hist?limit=365&sort=-1"
    SHORTS_URL = "https://api-pub.bitfinex.com/v2/stats1/pos.size:1m:tBTCUSD:short/hist?limit=365&sort=-1"

    def fetch(self) -> pd.DataFrame:
        longs_resp = requests.get(self.LONGS_URL, timeout=self.config.request_timeout)
        longs_resp.raise_for_status()
        shorts_resp = requests.get(self.SHORTS_URL, timeout=self.config.request_timeout)
        shorts_resp.raise_for_status()
        longs = {int(e[0]): float(e[1]) for e in longs_resp.json()}
        shorts = {int(e[0]): float(e[1]) for e in shorts_resp.json()}
        rows = []
        for ts_ms in sorted(set(longs) & set(shorts)):
            l_val = longs[ts_ms]
            s_val = shorts[ts_ms]
            if s_val != 0:
                ratio = l_val / abs(s_val)
                rows.append({
                    "timestamp": pd.Timestamp(ts_ms, unit="ms", tz="UTC"),
                    "value": ratio,
                })
        if not rows:
            raise RuntimeError("No Bitfinex L/S data")
        df = pd.DataFrame(rows)
        return df.sort_values("timestamp").reset_index(drop=True)
