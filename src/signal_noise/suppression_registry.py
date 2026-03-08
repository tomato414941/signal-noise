from __future__ import annotations

import os
import socket
import tomllib
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

from signal_noise.config import SUPPRESSIONS_PATH


@dataclass(frozen=True)
class SuppressionRule:
    selectors: tuple[str, ...]
    match: str
    scopes: tuple[str, ...]
    reason_code: str
    detail: str | None = None
    review_after: str | None = None


def _parse_csv(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def get_active_suppression_scopes(
    *,
    hostname: str | None = None,
    extra_scopes: str | None = None,
) -> tuple[str, ...]:
    current_host = hostname or socket.gethostname()
    active = ["all"]
    if current_host:
        active.append(current_host)
    for scope in _parse_csv(extra_scopes or os.getenv("SIGNAL_NOISE_SUPPRESSION_SCOPE", "")):
        if scope not in active:
            active.append(scope)
    return tuple(active)


def _normalize_rule(raw: dict[str, object]) -> SuppressionRule:
    selectors_value = raw.get("selectors")
    if selectors_value is None:
        selector = str(raw.get("selector", "")).strip()
        selectors = (selector,) if selector else ()
    else:
        selectors = tuple(str(item).strip() for item in selectors_value if str(item).strip())

    if not selectors:
        raise ValueError("Suppression rule requires selector or selectors")

    match = str(raw.get("match", "exact")).strip() or "exact"
    if match not in {"exact", "glob"}:
        raise ValueError(f"Unsupported suppression match type: {match}")

    scopes_value = raw.get("scopes")
    if scopes_value is None:
        scope = str(raw.get("scope", "all")).strip() or "all"
        scopes = (scope,)
    else:
        scopes = tuple(str(item).strip() for item in scopes_value if str(item).strip())
    if not scopes:
        scopes = ("all",)

    reason_code = str(raw.get("reason_code", "")).strip()
    if not reason_code:
        raise ValueError("Suppression rule requires reason_code")

    detail = str(raw.get("detail", "")).strip() or None
    review_after = str(raw.get("review_after", "")).strip() or None
    return SuppressionRule(
        selectors=selectors,
        match=match,
        scopes=scopes,
        reason_code=reason_code,
        detail=detail,
        review_after=review_after,
    )


def load_suppression_rules(path: Path | None = None) -> list[SuppressionRule]:
    registry_path = Path(
        os.getenv("SIGNAL_NOISE_SUPPRESSION_FILE", str(path or SUPPRESSIONS_PATH))
    )
    if not registry_path.exists():
        return []

    with registry_path.open("rb") as fh:
        raw = tomllib.load(fh)

    rules = raw.get("rules", [])
    if not isinstance(rules, list):
        raise ValueError("Suppression registry must contain a [rules] array")
    return [_normalize_rule(rule) for rule in rules]


def _match_priority(
    *,
    selector: str,
    match: str,
    scope: str,
) -> tuple[int, int, int]:
    return (
        1 if scope != "all" else 0,
        1 if match == "exact" else 0,
        len(selector),
    )


def resolve_suppressions(
    collector_names: list[str] | tuple[str, ...] | set[str],
    *,
    rules: list[SuppressionRule] | None = None,
    hostname: str | None = None,
    extra_scopes: str | None = None,
) -> dict[str, dict[str, str | None]]:
    active_scopes = set(
        get_active_suppression_scopes(hostname=hostname, extra_scopes=extra_scopes)
    )
    loaded_rules = rules if rules is not None else load_suppression_rules()
    resolved: dict[str, dict[str, str | None]] = {}

    for name in sorted(collector_names):
        best: tuple[tuple[int, int, int], int, SuppressionRule, str, str] | None = None
        for index, rule in enumerate(loaded_rules):
            for scope in rule.scopes:
                if scope not in active_scopes:
                    continue
                for selector in rule.selectors:
                    matched = (
                        name == selector
                        if rule.match == "exact"
                        else fnmatchcase(name, selector)
                    )
                    if not matched:
                        continue
                    candidate = (
                        _match_priority(selector=selector, match=rule.match, scope=scope),
                        -index,
                        rule,
                        selector,
                        scope,
                    )
                    if best is None or candidate > best:
                        best = candidate

        if best is None:
            continue

        _, _, rule, selector, scope = best
        resolved[name] = {
            "reason": rule.reason_code,
            "detail": rule.detail,
            "source": "registry",
            "scope": scope,
            "review_after": rule.review_after,
            "selector": selector,
        }

    return resolved
