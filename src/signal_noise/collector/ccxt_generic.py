from __future__ import annotations

import ccxt
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

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
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            data_type="crypto",
            api_docs_url="https://binance-docs.github.io/apidocs/spot/en/",
        )

        def fetch(self) -> pd.DataFrame:
            exchange = ccxt.binance({"enableRateLimit": True})
            ohlcv = exchange.fetch_ohlcv(pair, "1h", limit=1000)
            df = pd.DataFrame(
                ohlcv,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df["value"] = df["close"]
            df = df[["timestamp", "value"]].drop_duplicates(subset=["timestamp"])
            df = df.sort_values("timestamp").reset_index(drop=True)
            return df

    _Collector.__name__ = f"Ccxt_{name}"
    _Collector.__qualname__ = f"Ccxt_{name}"
    return _Collector


def get_ccxt_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_ccxt_collector(*t) for t in CRYPTO_PAIRS}
