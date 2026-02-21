from __future__ import annotations

from signal_noise.collector.base import SourceMeta
from signal_noise.collector.yahoo_finance import _YahooCollector

YAHOO_TICKERS: list[tuple[str, str, str, str]] = [
    ("^IXIC", "nasdaq", "NASDAQ Composite", "equity"),
    ("^DJI", "djia", "Dow Jones Industrial", "equity"),
    ("^RUT", "russell2000", "Russell 2000", "equity"),
    ("^STOXX50E", "eurostoxx50", "Euro Stoxx 50", "equity"),
    ("^FTSE", "ftse100", "FTSE 100", "equity"),
    ("^N225", "nikkei225", "Nikkei 225", "equity"),
    ("^HSI", "hang_seng", "Hang Seng Index", "equity"),
    ("^GDAXI", "dax", "DAX 40", "equity"),
    ("^FCHI", "cac40", "CAC 40", "equity"),
    ("^BSESN", "sensex", "BSE Sensex", "equity"),
    ("^KS11", "kospi", "KOSPI", "equity"),
    ("^TWII", "taiwan_weighted", "Taiwan Weighted", "equity"),
    ("^AXJO", "asx200", "ASX 200", "equity"),
    ("^VIX", "vix", "CBOE Volatility (VIX)", "volatility"),
    ("^VVIX", "vvix", "VIX of VIX", "volatility"),
    ("SI=F", "silver", "Silver Futures", "commodity"),
    ("CL=F", "oil_wti", "WTI Crude Oil", "commodity"),
    ("BZ=F", "oil_brent", "Brent Crude Oil", "commodity"),
    ("NG=F", "nat_gas", "Natural Gas Futures", "commodity"),
    ("HG=F", "copper", "Copper Futures", "commodity"),
    ("PL=F", "platinum", "Platinum Futures", "commodity"),
    ("PA=F", "palladium", "Palladium Futures", "commodity"),
    ("ZW=F", "wheat", "Wheat Futures", "commodity"),
    ("ZC=F", "corn", "Corn Futures", "commodity"),
    ("ZS=F", "soybean", "Soybean Futures", "commodity"),
    ("KC=F", "coffee", "Coffee Futures", "commodity"),
    ("CT=F", "cotton", "Cotton Futures", "commodity"),
    ("LBS=F", "lumber", "Lumber Futures", "commodity"),
    ("EURUSD=X", "eur_usd", "EUR/USD", "forex"),
    ("GBPUSD=X", "gbp_usd", "GBP/USD", "forex"),
    ("USDJPY=X", "usd_jpy", "USD/JPY", "forex"),
    ("AUDUSD=X", "aud_usd", "AUD/USD", "forex"),
    ("USDCHF=X", "usd_chf", "USD/CHF", "forex"),
    ("USDCAD=X", "usd_cad", "USD/CAD", "forex"),
    ("NZDUSD=X", "nzd_usd", "NZD/USD", "forex"),
    ("USDCNY=X", "usd_cny", "USD/CNY", "forex"),
    ("USDINR=X", "usd_inr", "USD/INR", "forex"),
    ("USDKRW=X", "usd_krw", "USD/KRW", "forex"),
    ("USDTRY=X", "usd_try", "USD/TRY", "forex"),
    ("USDBRL=X", "usd_brl", "USD/BRL", "forex"),
    ("^TNX", "us10y", "US 10Y Treasury Yield", "bond"),
    ("^TYX", "us30y", "US 30Y Treasury Yield", "bond"),
    ("^IRX", "us3m", "US 3M T-Bill Rate", "bond"),
    ("^FVX", "us5y", "US 5Y Treasury Yield", "bond"),
    ("XLF", "etf_financials", "Financial Select SPDR", "sector"),
    ("XLK", "etf_tech", "Technology Select SPDR", "sector"),
    ("XLE", "etf_energy", "Energy Select SPDR", "sector"),
    ("XLV", "etf_healthcare", "Health Care Select SPDR", "sector"),
    ("XLI", "etf_industrial", "Industrial Select SPDR", "sector"),
    ("XLP", "etf_staples", "Consumer Staples SPDR", "sector"),
    ("XLY", "etf_discretionary", "Consumer Discretionary SPDR", "sector"),
    ("XLB", "etf_materials", "Materials Select SPDR", "sector"),
    ("XLU", "etf_utilities", "Utilities Select SPDR", "sector"),
    ("XLRE", "etf_realestate", "Real Estate Select SPDR", "sector"),
    ("MSTR", "mstr", "MicroStrategy", "crypto_equity"),
    ("COIN", "coinbase", "Coinbase Global", "crypto_equity"),
    ("MARA", "mara", "Marathon Digital", "crypto_equity"),
    ("RIOT", "riot", "Riot Platforms", "crypto_equity"),
    # Consumer / fast food (Whopper inspiration)
    ("QSR", "qsr", "Restaurant Brands International", "consumer"),
    ("MCD", "mcd", "McDonald's", "consumer"),
    ("SBUX", "sbux", "Starbucks", "consumer"),
]


def _make_yahoo_collector(
    ticker: str, name: str, display_name: str, data_type: str
) -> type[_YahooCollector]:
    class _Collector(_YahooCollector):
        _ticker = ticker
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            data_type=data_type,
            api_docs_url=f"https://finance.yahoo.com/quote/{ticker}/",
        )

    _Collector.__name__ = f"Yahoo_{name}"
    _Collector.__qualname__ = f"Yahoo_{name}"
    return _Collector


def get_yahoo_collectors() -> dict[str, type[_YahooCollector]]:
    return {t[1]: _make_yahoo_collector(*t) for t in YAHOO_TICKERS}
