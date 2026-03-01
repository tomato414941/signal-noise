from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from signal_noise.store.sqlite_store import SignalStore


@pytest.fixture
def store(tmp_path: Path) -> SignalStore:
    s = SignalStore(tmp_path / "test.db")
    yield s
    s.close()


def _seed_signals(store: SignalStore, n_signals: int = 3, n_rows: int = 100) -> list[str]:
    """Register meta and save synthetic daily data for multiple signals."""
    np.random.seed(42)
    names = [f"sig_{i}" for i in range(n_signals)]
    dates = pd.date_range("2023-01-01", periods=n_rows, freq="D")
    for i, name in enumerate(names):
        store.save_meta(name, "financial", "equity", 86400)
        values = np.cumsum(np.random.randn(n_rows)) + (i + 1) * 10
        df = pd.DataFrame({"timestamp": dates, "value": values})
        store.save(name, df)
    return names


class TestComputeSpectrum:
    def test_basic_svd(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=5, n_rows=250)
        result = compute_spectrum(store, min_rows=50, n_components=3, n_top_signals=3)
        assert result.n_signals == 5
        assert result.n_dates > 0
        assert len(result.components) == 3

    def test_variance_ratio_positive(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=5, n_rows=250)
        result = compute_spectrum(store, min_rows=50, n_components=3)
        for pc in result.components:
            assert pc.variance_ratio > 0

    def test_domain_composition_counts(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=5, n_rows=250)
        result = compute_spectrum(store, min_rows=50, n_components=2, n_top_signals=3)
        for pc in result.components:
            total_count = sum(pc.domain_composition.values())
            # top signals (3) + extended range (up to 20) should not double-count
            assert total_count <= 20
            assert total_count >= 3
            # All signals are "financial" domain
            assert "financial" in pc.domain_composition

    def test_top_signals_count(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=5, n_rows=250)
        result = compute_spectrum(store, min_rows=50, n_components=2, n_top_signals=4)
        for pc in result.components:
            assert len(pc.top_signals) <= 4

    def test_effective_dims(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=5, n_rows=250)
        result = compute_spectrum(store, min_rows=50)
        for pct in [50, 80, 90, 95, 99]:
            assert pct in result.effective_dims
            assert result.effective_dims[pct] >= 1

    def test_participation_ratio(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=5, n_rows=250)
        result = compute_spectrum(store, min_rows=50)
        assert result.participation_ratio > 0

    def test_too_few_signals_raises(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=2, n_rows=250)
        with pytest.raises(ValueError, match="Too few signals"):
            compute_spectrum(store, min_rows=50)

    def test_no_daily_signals_raises(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        with pytest.raises(ValueError, match="No daily signals"):
            compute_spectrum(store, min_rows=50)

    def test_single_signal_raises(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=1, n_rows=250)
        with pytest.raises(ValueError, match="Too few signals"):
            compute_spectrum(store, min_rows=50)

    def test_summary_output(self, store: SignalStore) -> None:
        from signal_noise.analysis.spectrum import compute_spectrum

        _seed_signals(store, n_signals=5, n_rows=250)
        result = compute_spectrum(store, min_rows=50, n_components=2)
        summary = result.summary()
        assert "Matrix:" in summary
        assert "Effective Dimensionality:" in summary
        assert "PC" in summary


class TestComputeQuality:
    def test_basic_quality(self, store: SignalStore) -> None:
        from signal_noise.analysis.quality import compute_quality

        _seed_signals(store, n_signals=3, n_rows=100)
        result = compute_quality(store, days=90)
        assert result.n_signals == 3
        assert result.n_healthy + result.n_degraded + result.n_poor == 3

    def test_no_data_health_zero(self, store: SignalStore) -> None:
        from signal_noise.analysis.quality import compute_quality

        store.save_meta("empty_sig", "financial", "equity", 86400)
        result = compute_quality(store, days=90)
        assert result.n_signals == 1
        sig = result.signals[0]
        assert sig.health_score == 0.0
        assert sig.completeness == 0.0

    def test_complete_data_high_completeness(self, store: SignalStore) -> None:
        from signal_noise.analysis.quality import compute_quality

        now = datetime.now(timezone.utc)
        dates = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(90)]
        df = pd.DataFrame({"date": dates, "value": np.random.randn(90)})
        store.save_meta("complete_sig", "financial", "equity", 86400)
        store.save("complete_sig", df)
        result = compute_quality(store, days=90)
        sig = result.signals[0]
        assert sig.completeness >= 0.9

    def test_old_data_low_freshness(self, store: SignalStore) -> None:
        from signal_noise.analysis.quality import compute_quality

        dates = [(datetime(2020, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(100)]
        df = pd.DataFrame({"date": dates, "value": np.random.randn(100)})
        store.save_meta("old_sig", "financial", "equity", 86400)
        store.save("old_sig", df)
        result = compute_quality(store, days=90)
        sig = result.signals[0]
        assert sig.freshness < 0.1

    def test_distribution_change_low_stability(self, store: SignalStore) -> None:
        from signal_noise.analysis.quality import compute_quality

        now = datetime.now(timezone.utc)
        n = 100
        dates = [(now - timedelta(days=n - i)).strftime("%Y-%m-%d") for i in range(n)]
        # First half: mean=0, second half: mean=100
        values = list(np.random.randn(50)) + list(np.random.randn(50) + 100)
        df = pd.DataFrame({"date": dates, "value": values})
        store.save_meta("unstable_sig", "financial", "equity", 86400)
        store.save("unstable_sig", df)
        result = compute_quality(store, days=120)
        sig = result.signals[0]
        assert sig.stability < 0.5

    def test_no_signals_raises(self, store: SignalStore) -> None:
        from signal_noise.analysis.quality import compute_quality

        with pytest.raises(ValueError, match="No signals found"):
            compute_quality(store)

    def test_domain_filter(self, store: SignalStore) -> None:
        from signal_noise.analysis.quality import compute_quality

        _seed_signals(store, n_signals=3, n_rows=100)
        store.save_meta("weather_sig", "earth", "weather", 86400)
        dates = pd.date_range("2023-01-01", periods=100, freq="D")
        df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(100)})
        store.save("weather_sig", df)
        result = compute_quality(store, days=90, domain="earth")
        assert result.n_signals == 1
        assert result.signals[0].name == "weather_sig"

    def test_summary_output(self, store: SignalStore) -> None:
        from signal_noise.analysis.quality import compute_quality

        _seed_signals(store, n_signals=3, n_rows=100)
        result = compute_quality(store, days=90)
        summary = result.summary()
        assert "Signals:" in summary
        assert "Healthy:" in summary
