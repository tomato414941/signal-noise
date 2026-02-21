from unittest.mock import patch

import pandas as pd
import pytest

from signal_noise.collector.base import BaseCollector, SourceMeta, DOMAINS, CATEGORIES, FREQUENCIES
from signal_noise.config import CollectorConfig


class DummyCollector(BaseCollector):
    meta = SourceMeta(
        name="dummy",
        display_name="Dummy Source",
        update_frequency="daily",
        data_type="test",
        api_docs_url="https://example.com",
    )

    def __init__(self, data=None, fail_count=0, **kwargs):
        super().__init__(**kwargs)
        self._data = data
        self._fail_count = fail_count
        self._call_count = 0

    def fetch(self) -> pd.DataFrame:
        self._call_count += 1
        if self._call_count <= self._fail_count:
            raise ConnectionError("API down")
        if self._data is not None:
            return self._data
        return pd.DataFrame({
            "date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
        })


class TestBaseCollector:
    def test_collect_fetches_and_saves(self, tmp_path):
        with patch("signal_noise.collector.base.RAW_DIR", tmp_path / "raw"), \
             patch("signal_noise.collector.base.CACHE_DIR", tmp_path / "cache"):
            c = DummyCollector()
            df = c.collect()
            assert len(df) == 5
            assert (tmp_path / "raw" / "dummy.parquet").exists()
            assert (tmp_path / "cache" / "dummy.json").exists()

    def test_collect_uses_cache(self, tmp_path):
        with patch("signal_noise.collector.base.RAW_DIR", tmp_path / "raw"), \
             patch("signal_noise.collector.base.CACHE_DIR", tmp_path / "cache"):
            c = DummyCollector()
            c.collect()
            c2 = DummyCollector()
            c2.collect()
            assert c2._call_count == 0  # used cache, no fetch

    def test_collect_ignores_stale_cache(self, tmp_path):
        with patch("signal_noise.collector.base.RAW_DIR", tmp_path / "raw"), \
             patch("signal_noise.collector.base.CACHE_DIR", tmp_path / "cache"):
            c = DummyCollector(config=CollectorConfig(cache_max_age_hours=0.0))
            c.collect()
            c2 = DummyCollector(config=CollectorConfig(cache_max_age_hours=0.0))
            c2.collect()
            assert c2._call_count == 1

    def test_retry_on_failure(self, tmp_path):
        with patch("signal_noise.collector.base.RAW_DIR", tmp_path / "raw"), \
             patch("signal_noise.collector.base.CACHE_DIR", tmp_path / "cache"):
            config = CollectorConfig(retry_backoff_base=0.01)
            c = DummyCollector(fail_count=2, config=config)
            df = c.collect()
            assert len(df) == 5
            assert c._call_count == 3

    def test_retry_exhausted_raises(self, tmp_path):
        with patch("signal_noise.collector.base.RAW_DIR", tmp_path / "raw"), \
             patch("signal_noise.collector.base.CACHE_DIR", tmp_path / "cache"):
            config = CollectorConfig(max_retries=2, retry_backoff_base=0.01)
            c = DummyCollector(fail_count=5, config=config)
            with pytest.raises(RuntimeError, match="Failed to fetch dummy"):
                c.collect()

    def test_parquet_append_dedup(self, tmp_path):
        with patch("signal_noise.collector.base.RAW_DIR", tmp_path / "raw"), \
             patch("signal_noise.collector.base.CACHE_DIR", tmp_path / "cache"):
            df1 = pd.DataFrame({
                "date": pd.date_range("2024-01-01", periods=3, freq="D"),
                "value": [1.0, 2.0, 3.0],
            })
            df2 = pd.DataFrame({
                "date": pd.date_range("2024-01-03", periods=3, freq="D"),
                "value": [30.0, 40.0, 50.0],
            })
            c1 = DummyCollector(data=df1, config=CollectorConfig(cache_max_age_hours=0.0))
            c1.collect()
            c2 = DummyCollector(data=df2, config=CollectorConfig(cache_max_age_hours=0.0))
            c2.collect()
            result = pd.read_parquet(tmp_path / "raw" / "dummy.parquet")
            assert len(result) == 5  # 3 + 3 - 1 overlap
            assert result.iloc[2]["value"] == 30.0  # newer wins

    def test_taxonomy_constants_non_empty(self):
        assert len(DOMAINS) >= 8
        assert len(CATEGORIES) >= 20
        assert len(FREQUENCIES) >= 5

    def test_status_no_data(self, tmp_path):
        with patch("signal_noise.collector.base.RAW_DIR", tmp_path / "raw"), \
             patch("signal_noise.collector.base.CACHE_DIR", tmp_path / "cache"):
            c = DummyCollector()
            s = c.status()
            assert s["name"] == "dummy"
            assert s["has_data"] is False
            assert s["cache_age_hours"] is None


class TestTaxonomy:
    def test_all_collectors_have_domain_and_category(self):
        from signal_noise.collector import COLLECTORS
        for name, cls in COLLECTORS.items():
            assert cls.meta.domain, f"{name} missing domain"
            assert cls.meta.category, f"{name} missing category"

    def test_all_domains_valid(self):
        from signal_noise.collector import COLLECTORS
        for name, cls in COLLECTORS.items():
            assert cls.meta.domain in DOMAINS, f"{name} has invalid domain: {cls.meta.domain}"

    def test_all_categories_valid(self):
        from signal_noise.collector import COLLECTORS
        for name, cls in COLLECTORS.items():
            assert cls.meta.category in CATEGORIES, f"{name} has invalid category: {cls.meta.category}"

    def test_all_frequencies_valid(self):
        from signal_noise.collector import COLLECTORS
        for name, cls in COLLECTORS.items():
            assert cls.meta.update_frequency in FREQUENCIES, (
                f"{name} has invalid frequency: {cls.meta.update_frequency}"
            )
