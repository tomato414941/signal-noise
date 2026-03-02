"""Manifest-based lazy loading for collector registry.

The manifest maps collector names to their source modules, enabling
keys()/len() to work without importing any collector modules.
"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

_PACKAGE_DIR = Path(__file__).resolve().parent
_MANIFEST_PATH = _PACKAGE_DIR / ".collector_manifest.json"


def _compute_package_hash() -> str:
    h = hashlib.md5(usedforsecurity=False)
    for py_file in sorted(_PACKAGE_DIR.glob("*.py")):
        h.update(py_file.name.encode())
        h.update(str(py_file.stat().st_mtime_ns).encode())
    return h.hexdigest()


def build_manifest() -> dict:
    """Run full auto-discovery and save manifest to disk."""
    from signal_noise.collector._loader import _discover
    from signal_noise.collector.streaming import StreamingCollector

    collectors = _discover()
    entries: dict[str, dict] = {}

    for name, cls in collectors.items():
        m = cls.meta
        entries[name] = {
            "module": cls.__module__,
            "class": cls.__qualname__,
            "is_streaming": issubclass(cls, StreamingCollector),
            "meta": {
                "domain": m.domain,
                "category": m.category,
                "update_frequency": m.update_frequency,
                "requires_key": m.requires_key,
                "signal_type": m.signal_type,
                "collection_level": m.collection_level,
                "interval": m.interval,
            },
        }

    manifest = {
        "hash": _compute_package_hash(),
        "collectors": entries,
    }

    _MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    log.info("Built collector manifest: %d entries", len(entries))
    return manifest


def load_manifest() -> dict | None:
    """Load manifest if it exists and is fresh. Returns None if stale/missing."""
    if not _MANIFEST_PATH.exists():
        return None

    try:
        manifest = json.loads(_MANIFEST_PATH.read_text())
    except (json.JSONDecodeError, KeyError):
        return None

    if manifest.get("hash") != _compute_package_hash():
        log.info("Collector manifest is stale, rebuilding")
        return None

    return manifest
