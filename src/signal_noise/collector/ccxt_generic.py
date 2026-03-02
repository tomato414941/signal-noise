from __future__ import annotations

import time

import ccxt
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

CRYPTO_PAIRS: list[tuple[str, str, str]] = [
    ("SOL/USDT", "sol_usdt", "SOL/USDT"),
    ("BNB/USDT", "bnb_usdt", "BNB/USDT"),
    ("ADA/USDT", "ada_usdt", "ADA/USDT"),
    ("XRP/USDT", "xrp_usdt", "XRP/USDT"),
    ("DOGE/USDT", "doge_usdt", "DOGE/USDT"),
    ("DOT/USDT", "dot_usdt", "DOT/USDT"),
    ("AVAX/USDT", "avax_usdt", "AVAX/USDT"),
    ("LINK/USDT", "link_usdt", "LINK/USDT"),
    ("UNI/USDT", "uni_usdt", "UNI/USDT"),
    ("ATOM/USDT", "atom_usdt", "ATOM/USDT"),
    ("LTC/USDT", "ltc_usdt", "LTC/USDT"),
    ("FIL/USDT", "fil_usdt", "FIL/USDT"),
    ("NEAR/USDT", "near_usdt", "NEAR/USDT"),
    ("APT/USDT", "apt_usdt", "APT/USDT"),
    ("ARB/USDT", "arb_usdt", "ARB/USDT"),
    ("OP/USDT", "op_usdt", "OP/USDT"),
    ("SUI/USDT", "sui_usdt", "SUI/USDT"),
    ("SOL/BTC", "sol_btc", "SOL/BTC"),
    ("BNB/BTC", "bnb_btc", "BNB/BTC"),
    ("XRP/BTC", "xrp_btc", "XRP/BTC"),
]


def _make_ccxt_collector(
    pair: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://binance-docs.github.io/apidocs/spot/en/",
            domain="markets",
            category="crypto",
            signal_type="ohlcv",
        )

        def __init__(self, total: int = 5000, **kwargs):
            super().__init__(**kwargs)
            self.total = total

        def fetch(self) -> pd.DataFrame:
            exchange = ccxt.binance({"enableRateLimit": True})
            since = exchange.milliseconds() - self.total * 3_600_000
            all_data: list[list] = []
            while len(all_data) < self.total:
                batch = exchange.fetch_ohlcv(
                    pair, "1h", since=since, limit=1000
                )
                if not batch:
                    break
                all_data.extend(batch)
                since = batch[-1][0] + 1
                if len(batch) < 1000:
                    break
                time.sleep(0.2)

            df = pd.DataFrame(
                all_data,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df["value"] = df["close"]
            df = df[["timestamp", "value", "open", "high", "low", "volume"]]
            df = df.drop_duplicates(subset=["timestamp"])
            df = df.sort_values("timestamp").reset_index(drop=True)
            return df.head(self.total)

    _Collector.__name__ = f"Ccxt_{name}"
    _Collector.__qualname__ = f"Ccxt_{name}"
    return _Collector


def get_ccxt_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_ccxt_collector(*t) for t in CRYPTO_PAIRS}
