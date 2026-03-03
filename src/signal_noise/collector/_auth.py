"""Unified API key / secret loading for collectors.

Pattern: env var → ~/.secrets/{provider} file → raise or return empty.
Secret files use ``export KEY=value`` format (same as shell source files).
"""
from __future__ import annotations

import os
from pathlib import Path

_cache: dict[str, str] = {}


def _parse_secret_file(path: Path, env_vars: list[str]) -> dict[str, str]:
    found: dict[str, str] = {}
    if not path.exists():
        return found
    for line in path.read_text().splitlines():
        line = line.strip()
        for var in env_vars:
            prefix = f"export {var}="
            if line.startswith(prefix):
                found[var] = line.split("=", 1)[1].strip().strip("'\"")
    return found


def load_secret(
    provider: str,
    env_var: str,
    *,
    signup_url: str = "",
    optional: bool = False,
) -> str:
    if env_var in _cache:
        return _cache[env_var]

    val = os.environ.get(env_var, "")
    if not val:
        parsed = _parse_secret_file(Path.home() / ".secrets" / provider, [env_var])
        val = parsed.get(env_var, "")

    if not val and not optional:
        msg = f"{env_var} not set."
        if signup_url:
            msg += f" Get a key at {signup_url}"
        raise RuntimeError(msg)

    _cache[env_var] = val
    return val


def load_secrets(
    provider: str,
    env_vars: list[str],
    *,
    signup_url: str = "",
    optional: bool = False,
) -> dict[str, str]:
    # Return cached if all present
    if all(v in _cache for v in env_vars):
        return {v: _cache[v] for v in env_vars}

    result: dict[str, str] = {}
    for v in env_vars:
        result[v] = os.environ.get(v, "")

    missing = [v for v in env_vars if not result[v]]
    if missing:
        parsed = _parse_secret_file(Path.home() / ".secrets" / provider, missing)
        for v in missing:
            result[v] = parsed.get(v, "")

    still_missing = [v for v in env_vars if not result[v]]
    if still_missing and not optional:
        msg = f"{' / '.join(still_missing)} not set."
        if signup_url:
            msg += f" Register at {signup_url}"
        raise RuntimeError(msg)

    _cache.update(result)
    return result
