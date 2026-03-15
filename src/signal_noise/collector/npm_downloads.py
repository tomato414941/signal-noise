from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (package_name, collector_name, display_name)
NPM_PACKAGES: list[tuple[str, str, str]] = [
    ("web3", "npm_web3", "NPM: web3"),
    ("ethers", "npm_ethers", "NPM: ethers"),
    ("bitcoinjs-lib", "npm_bitcoinjs", "NPM: bitcoinjs-lib"),
    ("@solana/web3.js", "npm_solana_web3", "NPM: @solana/web3.js"),
    ("hardhat", "npm_hardhat", "NPM: hardhat"),
    ("viem", "npm_viem", "NPM: viem"),
    ("ccxt", "npm_ccxt", "NPM: ccxt"),
    ("openai", "npm_openai", "NPM: openai"),
    ("langchain", "npm_langchain", "NPM: langchain"),
    ("@anthropic-ai/sdk", "npm_anthropic", "NPM: @anthropic-ai/sdk"),
    ("@google/generative-ai", "npm_google_genai", "NPM: @google/generative-ai"),
    ("typescript", "npm_typescript", "NPM: typescript"),
    ("react", "npm_react", "NPM: react"),
    ("next", "npm_next", "NPM: next"),
    ("svelte", "npm_svelte", "NPM: svelte"),
    ("esbuild", "npm_esbuild", "NPM: esbuild"),
    ("vite", "npm_vite", "NPM: vite"),
    ("prettier", "npm_prettier", "NPM: prettier"),
    ("eslint", "npm_eslint", "NPM: eslint"),
    ("zod", "npm_zod", "NPM: zod"),
    ("@trpc/server", "npm_trpc", "NPM: @trpc/server"),
    ("prisma", "npm_prisma", "NPM: prisma"),
    ("drizzle-orm", "npm_drizzle_orm", "NPM: drizzle-orm"),
]


def _make_npm_collector(
    package: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://github.com/npm/registry/blob/main/docs/download-counts.md",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=365)
            url = (
                f"https://api.npmjs.org/downloads/range/"
                f"{start.strftime('%Y-%m-%d')}:{end.strftime('%Y-%m-%d')}"
                f"/{package}"
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            downloads = data.get("downloads", [])
            if not downloads:
                raise RuntimeError(f"No NPM download data for {package}")

            rows = [
                {
                    "date": pd.to_datetime(d["day"], utc=True),
                    "value": float(d["downloads"]),
                }
                for d in downloads
            ]
            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"NPM_{name}"
    _Collector.__qualname__ = f"NPM_{name}"
    return _Collector


def get_npm_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_npm_collector(*t) for t in NPM_PACKAGES}
