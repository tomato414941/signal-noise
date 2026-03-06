"""Tests for auto-discovery, manifest, and lazy loading."""

from signal_noise.collector.base import BaseCollector, DOMAINS, CATEGORIES, FREQUENCIES


class TestAutoDiscover:
    """Verify _discover() finds all collectors correctly."""

    def test_discover_returns_dict(self):
        from signal_noise.collector._loader import _discover
        result = _discover()
        assert isinstance(result, dict)
        assert len(result) > 1000

    def test_all_values_are_collector_subclasses(self):
        from signal_noise.collector._loader import _discover
        result = _discover()
        for name, cls in result.items():
            assert issubclass(cls, BaseCollector), f"{name} is not a BaseCollector subclass"
            assert hasattr(cls, "meta"), f"{name} has no meta"

    def test_skipped_classes_excluded(self):
        from signal_noise.collector._loader import _discover
        result = _discover()
        for name, cls in result.items():
            assert not cls.__name__.startswith("_"), f"{cls.__name__} should be excluded"
            assert not getattr(cls, "_skip_registration", False), f"{name} should be skipped"

    def test_meta_taxonomy_valid(self):
        from signal_noise.collector._loader import _discover
        result = _discover()
        for name, cls in result.items():
            m = cls.meta
            assert m.domain in DOMAINS, f"{name}: invalid domain '{m.domain}'"
            assert m.category in CATEGORIES, f"{name}: invalid category '{m.category}'"
            assert m.update_frequency in FREQUENCIES, f"{name}: invalid frequency '{m.update_frequency}'"

    def test_no_duplicate_names(self):
        from signal_noise.collector._loader import _discover
        result = _discover()
        meta_names = [cls.meta.name for cls in result.values()]
        assert len(meta_names) == len(set(meta_names))
        for name, cls in result.items():
            assert name == cls.meta.name, f"Key '{name}' != meta.name '{cls.meta.name}'"


class TestManifest:
    """Verify manifest build/load cycle."""

    def test_build_manifest(self):
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        assert "hash" in manifest
        assert "collectors" in manifest
        assert len(manifest["collectors"]) > 1000

    def test_load_manifest_after_build(self):
        from signal_noise.collector._manifest import build_manifest, load_manifest
        build_manifest()
        loaded = load_manifest()
        assert loaded is not None
        assert len(loaded["collectors"]) > 1000

    def test_manifest_entries_have_module_and_class(self):
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        for name, entry in manifest["collectors"].items():
            assert "module" in entry, f"{name} missing 'module'"
            assert "class" in entry, f"{name} missing 'class'"


class TestLazyRegistry:
    """Verify LazyCollectorRegistry works without full imports."""

    def test_len_without_import(self):
        from signal_noise.collector._lazy import LazyCollectorRegistry
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        reg = LazyCollectorRegistry(manifest["collectors"])
        assert len(reg) > 1000

    def test_contains_without_import(self):
        from signal_noise.collector._lazy import LazyCollectorRegistry
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        reg = LazyCollectorRegistry(manifest["collectors"])
        # Check known collector exists without triggering import
        assert "sp500" in reg or "btc_usd" in reg

    def test_getitem_loads_single_module(self):
        from signal_noise.collector._lazy import LazyCollectorRegistry
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        reg = LazyCollectorRegistry(manifest["collectors"])
        # Access one collector — should only load its module
        first_name = next(iter(manifest["collectors"]))
        cls = reg[first_name]
        assert issubclass(cls, BaseCollector)
        assert reg._loaded  # at least one loaded
        assert len(reg._loaded) < len(reg)  # not all loaded

    def test_get_returns_none_for_unknown(self):
        from signal_noise.collector._lazy import LazyCollectorRegistry
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        reg = LazyCollectorRegistry(manifest["collectors"])
        assert reg.get("nonexistent_collector_xyz") is None

    def test_load_alias_loads_single_module(self):
        from signal_noise.collector._lazy import LazyCollectorRegistry
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        reg = LazyCollectorRegistry(manifest["collectors"])
        first_name = next(iter(manifest["collectors"]))
        cls = reg.load(first_name)
        assert cls is not None
        assert cls is reg[first_name]

    def test_get_meta_returns_manifest_copy(self):
        from signal_noise.collector._lazy import LazyCollectorRegistry
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        reg = LazyCollectorRegistry(manifest["collectors"])
        first_name = next(iter(manifest["collectors"]))
        meta = reg.get_meta(first_name)
        assert meta == manifest["collectors"][first_name]["meta"]
        assert meta is not manifest["collectors"][first_name]["meta"]

    def test_is_streaming_reflects_manifest(self):
        from signal_noise.collector._lazy import LazyCollectorRegistry
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        reg = LazyCollectorRegistry(manifest["collectors"])
        first_name = next(iter(manifest["collectors"]))
        assert reg.is_streaming(first_name) is bool(
            manifest["collectors"][first_name].get("is_streaming", False)
        )

    def test_keys_matches_manifest(self):
        from signal_noise.collector._lazy import LazyCollectorRegistry
        from signal_noise.collector._manifest import build_manifest
        manifest = build_manifest()
        reg = LazyCollectorRegistry(manifest["collectors"])
        assert set(reg.keys()) == set(manifest["collectors"].keys())


class TestCollectorsIntegrity:
    """Verify COLLECTORS public interface matches expectations."""

    def test_collectors_count(self):
        from signal_noise.collector import COLLECTORS
        assert len(COLLECTORS) >= 1170

    def test_collectors_keys_are_strings(self):
        from signal_noise.collector import COLLECTORS
        for name in COLLECTORS:
            assert isinstance(name, str)
            assert len(name) > 0

    def test_discover_matches_lazy_registry(self):
        from signal_noise.collector._loader import _discover
        from signal_noise.collector import COLLECTORS
        discovered = _discover()
        assert set(discovered.keys()) == set(COLLECTORS.keys())
