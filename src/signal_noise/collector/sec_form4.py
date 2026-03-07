from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector._utils import build_timeseries_df
from signal_noise.collector.base import BaseCollector, CollectorMeta

_sec_form4_cache = SharedAPICache(ttl=21_600)
_LOOKBACK_DAYS = 90
_FORM_TYPES = {"4"}
_JSON_HEADERS = {
    "User-Agent": "signal-noise/0.1 research@example.com",
    "Accept": "application/json",
}
_XML_HEADERS = {
    "User-Agent": "signal-noise/0.1 research@example.com",
    "Accept": "application/xml,text/xml",
}


@dataclass(frozen=True)
class _TickerSpec:
    ticker: str
    issuer_name: str


@dataclass(frozen=True)
class _SignalSpec:
    ticker: str
    issuer_name: str
    metric: str
    name: str
    display_name: str


@dataclass(frozen=True)
class _FilingRef:
    cik: int
    accession: str


@dataclass(frozen=True)
class _OpenMarketTransaction:
    date: date
    code: str
    shares: float


_TICKER_SPECS: list[_TickerSpec] = [
    _TickerSpec("TSLA", "Tesla"),
    _TickerSpec("META", "Meta"),
    _TickerSpec("NVDA", "NVIDIA"),
    _TickerSpec("GS", "Goldman Sachs"),
    _TickerSpec("JPM", "JPMorgan"),
    _TickerSpec("XOM", "Exxon Mobil"),
    _TickerSpec("CVX", "Chevron"),
    _TickerSpec("DHI", "D.R. Horton"),
    _TickerSpec("LEN", "Lennar"),
    _TickerSpec("CAT", "Caterpillar"),
    _TickerSpec("DE", "Deere"),
]

_SIGNAL_SPECS: list[_SignalSpec] = [
    _SignalSpec(
        spec.ticker,
        spec.issuer_name,
        metric,
        f"form4_{spec.ticker.lower()}_{suffix}",
        f"{spec.issuer_name} Form 4 {label}",
    )
    for spec in _TICKER_SPECS
    for metric, suffix, label in (
        ("net_share_ratio", "net_share_ratio", "Net Open-Market Share Ratio"),
        ("open_market_tx_count", "open_market_tx_count", "Open-Market Transaction Count"),
    )
]


def _utc_today() -> date:
    return datetime.now(UTC).date()


def _request_json(url: str, *, timeout: int) -> dict:
    resp = requests.get(url, headers=_JSON_HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _request_xml(url: str, *, timeout: int) -> ET.Element:
    resp = requests.get(url, headers=_XML_HEADERS, timeout=timeout)
    resp.raise_for_status()
    return ET.fromstring(resp.text)


def _fetch_ticker_map(*, timeout: int = 30) -> dict[str, int]:
    def _fetch() -> dict[str, int]:
        payload = _request_json(
            "https://www.sec.gov/files/company_tickers.json",
            timeout=timeout,
        )
        return {
            item["ticker"].upper(): int(item["cik_str"])
            for item in payload.values()
        }

    return _sec_form4_cache.get_or_fetch("sec_form4:ticker_map", _fetch)


def _fetch_recent_form4_filings(
    ticker: str,
    *,
    lookback_days: int = _LOOKBACK_DAYS,
    timeout: int = 30,
) -> list[_FilingRef]:
    ticker_map = _fetch_ticker_map(timeout=timeout)
    cik = ticker_map.get(ticker.upper())
    if cik is None:
        raise RuntimeError(f"Unknown SEC ticker: {ticker}")

    payload = _request_json(
        f"https://data.sec.gov/submissions/CIK{cik:010d}.json",
        timeout=timeout,
    )
    recent = payload.get("filings", {}).get("recent", {})
    cutoff = _utc_today() - timedelta(days=lookback_days)
    out: list[_FilingRef] = []
    for form, filed, accession in zip(
        recent.get("form", []),
        recent.get("filingDate", []),
        recent.get("accessionNumber", []),
        strict=False,
    ):
        if form not in _FORM_TYPES:
            continue
        filed_day = date.fromisoformat(filed)
        if filed_day < cutoff:
            break
        out.append(_FilingRef(cik=cik, accession=accession))
    return out


def _select_raw_xml_name(index_payload: dict) -> str:
    items = index_payload.get("directory", {}).get("item", [])
    for item in items:
        name = item.get("name", "")
        lowered = name.lower()
        if lowered.endswith(".xml") and "xsl" not in lowered:
            return name
    raise RuntimeError("No raw XML found in SEC filing directory")


def _fetch_filing_root(cik: int, accession: str, *, timeout: int = 30) -> ET.Element:
    accession_digits = accession.replace("-", "")
    cache_key = f"sec_form4:xml:{cik}:{accession_digits}"

    def _fetch() -> ET.Element:
        index_payload = _request_json(
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_digits}/index.json",
            timeout=timeout,
        )
        xml_name = _select_raw_xml_name(index_payload)
        return _request_xml(
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_digits}/{xml_name}",
            timeout=timeout,
        )

    return _sec_form4_cache.get_or_fetch(cache_key, _fetch)


def _parse_float(text: str | None) -> float | None:
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_open_market_transactions(root: ET.Element) -> list[_OpenMarketTransaction]:
    period_of_report = root.findtext("./periodOfReport")
    out: list[_OpenMarketTransaction] = []
    for tx in root.findall(".//nonDerivativeTransaction"):
        code = (tx.findtext("./transactionCoding/transactionCode") or "").strip().upper()
        if code not in {"P", "S"}:
            continue
        tx_date = tx.findtext("./transactionDate/value") or period_of_report
        if not tx_date:
            continue
        shares = _parse_float(tx.findtext("./transactionAmounts/transactionShares/value"))
        if shares is None or shares <= 0:
            continue
        out.append(
            _OpenMarketTransaction(
                date=date.fromisoformat(tx_date[:10]),
                code=code,
                shares=shares,
            )
        )
    return out


def _empty_history_frame(lookback_days: int) -> pd.DataFrame:
    start = _utc_today() - timedelta(days=lookback_days)
    dates = pd.date_range(start=start, end=_utc_today(), freq="D", tz="UTC")
    return pd.DataFrame(
        {
            "date": dates,
            "net_share_ratio": 0.0,
            "open_market_tx_count": 0.0,
        }
    )


def _fetch_ticker_history(
    ticker: str,
    *,
    lookback_days: int = _LOOKBACK_DAYS,
    timeout: int = 30,
) -> pd.DataFrame:
    cache_key = f"sec_form4:history:{ticker.upper()}:{lookback_days}"

    def _fetch() -> pd.DataFrame:
        frame = _empty_history_frame(lookback_days)
        daily: dict[date, dict[str, float]] = {}
        cutoff = _utc_today() - timedelta(days=lookback_days)

        for filing in _fetch_recent_form4_filings(
            ticker,
            lookback_days=lookback_days,
            timeout=timeout,
        ):
            root = _fetch_filing_root(filing.cik, filing.accession, timeout=timeout)
            for tx in _parse_open_market_transactions(root):
                if tx.date < cutoff:
                    continue
                bucket = daily.setdefault(
                    tx.date,
                    {"buy_shares": 0.0, "sell_shares": 0.0, "tx_count": 0.0},
                )
                if tx.code == "P":
                    bucket["buy_shares"] += tx.shares
                else:
                    bucket["sell_shares"] += tx.shares
                bucket["tx_count"] += 1.0

        if not daily:
            return frame

        metric_rows: list[dict] = []
        for tx_day, bucket in sorted(daily.items()):
            total_shares = bucket["buy_shares"] + bucket["sell_shares"]
            ratio = 0.0
            if total_shares > 0:
                ratio = (bucket["buy_shares"] - bucket["sell_shares"]) / total_shares
            metric_rows.append(
                {
                    "date": pd.Timestamp(tx_day, tz="UTC"),
                    "net_share_ratio": float(ratio),
                    "open_market_tx_count": float(bucket["tx_count"]),
                }
            )

        metrics = pd.DataFrame(metric_rows)
        merged = frame.merge(metrics, on="date", how="left", suffixes=("_base", ""))
        merged["net_share_ratio"] = merged["net_share_ratio"].fillna(
            merged["net_share_ratio_base"]
        )
        merged["open_market_tx_count"] = merged["open_market_tx_count"].fillna(
            merged["open_market_tx_count_base"]
        )
        return merged[["date", "net_share_ratio", "open_market_tx_count"]]

    return _sec_form4_cache.get_or_fetch(cache_key, _fetch)


def _metric_series(ticker: str, metric: str) -> pd.DataFrame:
    history = _fetch_ticker_history(ticker)
    if metric not in history.columns:
        raise RuntimeError(f"Unknown SEC Form 4 metric: {metric}")
    rows = history[["date", metric]].rename(columns={metric: "value"}).to_dict(orient="records")
    return build_timeseries_df(rows, f"SEC Form 4 {ticker} {metric}")


def _make_signal_collector(spec: _SignalSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency="daily",
            api_docs_url="https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
            domain="markets",
            category="regulatory",
        )

        def fetch(self) -> pd.DataFrame:
            return _metric_series(spec.ticker, spec.metric)

    _Collector.__name__ = f"SECForm4_{spec.name}"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def get_sec_form4_collectors() -> dict[str, type[BaseCollector]]:
    return {spec.name: _make_signal_collector(spec) for spec in _SIGNAL_SPECS}
