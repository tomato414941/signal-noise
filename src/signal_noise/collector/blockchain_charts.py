from __future__ import annotations

from datetime import UTC, datetime

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (chart_name, collector_name, display_name, data_type, domain, category)
BLOCKCHAIN_CHARTS: list[tuple[str, str, str, str, str, str]] = [
    ("n-unique-addresses", "bc_unique_addrs", "BTC Unique Addresses", "onchain", "financial", "crypto"),
    ("n-transactions", "bc_tx_count", "BTC Transaction Count", "onchain", "financial", "crypto"),
    ("transaction-fees-usd", "bc_tx_fees_usd", "BTC Transaction Fees (USD)", "onchain", "financial", "crypto"),
    ("output-volume", "bc_output_volume", "BTC Output Volume", "onchain", "financial", "crypto"),
    ("estimated-transaction-volume-usd", "bc_est_volume_usd", "BTC Est. Transaction Volume (USD)", "onchain", "financial", "crypto"),
    ("my-wallet-n-users", "bc_wallet_users", "Blockchain.com Wallet Users", "onchain", "financial", "crypto"),
    ("utxo-count", "bc_utxo_count", "BTC UTXO Set Size", "onchain", "financial", "crypto"),
    ("cost-per-transaction", "bc_cost_per_tx", "BTC Cost Per Transaction", "onchain", "financial", "crypto"),
    ("miners-revenue", "bc_miners_revenue", "BTC Miners Revenue", "onchain", "financial", "crypto"),
    ("market-cap", "bc_market_cap", "BTC Market Cap", "onchain", "financial", "crypto"),
    ("trade-volume", "bc_exchange_volume", "BTC Exchange Trade Volume", "onchain", "financial", "crypto"),
    ("avg-block-size", "bc_avg_block_size", "BTC Average Block Size", "onchain", "financial", "crypto"),
    ("median-confirmation-time", "bc_confirm_time", "BTC Median Confirmation Time", "onchain", "financial", "crypto"),
    ("n-transactions-per-block", "bc_tx_per_block", "BTC Transactions Per Block", "onchain", "financial", "crypto"),
]


def _make_bc_collector(
    chart: str, name: str, display_name: str, data_type: str,
    domain: str, category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            data_type=data_type,
            api_docs_url=f"https://www.blockchain.com/explorer/charts/{chart}",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            url = (
                f"https://api.blockchain.info/charts/{chart}"
                f"?timespan=2years&format=json"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            values = data.get("values", [])
            if not values:
                raise RuntimeError(f"No data for blockchain chart {chart}")

            rows = []
            for point in values:
                ts = datetime.fromtimestamp(point["x"], tz=UTC)
                rows.append({
                    "date": pd.Timestamp(ts),
                    "value": float(point["y"]),
                })

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"BC_{name}"
    _Collector.__qualname__ = f"BC_{name}"
    return _Collector


def get_blockchain_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_bc_collector(*t) for t in BLOCKCHAIN_CHARTS}
