
from signal_noise.collector.yahoo_generic import YAHOO_TICKERS, _make_yahoo_collector, get_yahoo_collectors
from signal_noise.collector.ccxt_generic import CRYPTO_PAIRS, _make_ccxt_collector, get_ccxt_collectors
from signal_noise.collector.base import SourceMeta


class TestYahooGeneric:
    def test_ticker_count(self):
        assert len(YAHOO_TICKERS) >= 50

    def test_no_duplicate_names(self):
        names = [t[1] for t in YAHOO_TICKERS]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_yahoo_collector("^VIX", "test_vix", "Test VIX", "volatility")
        assert cls.meta.name == "test_vix"
        assert cls._ticker == "^VIX"
        assert isinstance(cls.meta, SourceMeta)

    def test_get_yahoo_collectors_returns_dict(self):
        collectors = get_yahoo_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(YAHOO_TICKERS)
        assert "vix" in collectors
        assert "nasdaq" in collectors

    def test_all_tickers_have_valid_data_type(self):
        valid_types = {"equity", "volatility", "commodity", "forex", "bond", "sector", "crypto_equity", "consumer", "mega_cap", "shipping", "aviation"}
        for ticker, name, display, dtype in YAHOO_TICKERS:
            assert dtype in valid_types, f"{name} has invalid type: {dtype}"


class TestCcxtGeneric:
    def test_pair_count(self):
        assert len(CRYPTO_PAIRS) >= 15

    def test_no_duplicate_names(self):
        names = [t[1] for t in CRYPTO_PAIRS]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_ccxt_collector("TEST/USDT", "test_usdt", "TEST/USDT")
        assert cls.meta.name == "test_usdt"
        assert isinstance(cls.meta, SourceMeta)

    def test_get_ccxt_collectors_returns_dict(self):
        collectors = get_ccxt_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(CRYPTO_PAIRS)
        assert "sol_usdt" in collectors
