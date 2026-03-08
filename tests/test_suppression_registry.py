from __future__ import annotations

from signal_noise.suppression_registry import load_suppression_rules, resolve_suppressions


def test_load_suppression_rules_from_toml(tmp_path) -> None:
    registry_path = tmp_path / "suppressions.toml"
    registry_path.write_text(
        """
[[rules]]
selector = "daily_demo"
reason_code = "missing_api_key"
detail = "Demo key missing."
review_after = "2026-04-15"

[[rules]]
selectors = ["stream_*", "batch_*"]
match = "glob"
scopes = ["alpha-os", "all"]
reason_code = "upstream_unstable"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    rules = load_suppression_rules(registry_path)

    assert len(rules) == 2
    assert rules[0].selectors == ("daily_demo",)
    assert rules[0].match == "exact"
    assert rules[0].scopes == ("all",)
    assert rules[0].reason_code == "missing_api_key"
    assert rules[0].detail == "Demo key missing."
    assert rules[0].review_after == "2026-04-15"
    assert rules[1].selectors == ("stream_*", "batch_*")
    assert rules[1].match == "glob"
    assert rules[1].scopes == ("alpha-os", "all")


def test_resolve_suppressions_respects_scope_and_exact_precedence(tmp_path) -> None:
    registry_path = tmp_path / "suppressions.toml"
    registry_path.write_text(
        """
[[rules]]
selectors = ["stream_*"]
match = "glob"
scopes = ["all"]
reason_code = "upstream_unstable"
detail = "Global fallback."

[[rules]]
selector = "stream_demo"
scopes = ["alpha-os"]
reason_code = "missing_api_key"
detail = "Host-specific rule."
review_after = "2026-05-01"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    rules = load_suppression_rules(registry_path)
    resolved = resolve_suppressions(
        ["stream_demo", "stream_other", "daily_demo"],
        rules=rules,
        hostname="alpha-os",
    )

    assert resolved["stream_demo"] == {
        "reason": "missing_api_key",
        "detail": "Host-specific rule.",
        "source": "registry",
        "scope": "alpha-os",
        "review_after": "2026-05-01",
        "selector": "stream_demo",
    }
    assert resolved["stream_other"] == {
        "reason": "upstream_unstable",
        "detail": "Global fallback.",
        "source": "registry",
        "scope": "all",
        "review_after": None,
        "selector": "stream_*",
    }
    assert "daily_demo" not in resolved


def test_resolve_suppressions_uses_extra_scope_env_style(tmp_path) -> None:
    registry_path = tmp_path / "suppressions.toml"
    registry_path.write_text(
        """
[[rules]]
selector = "daily_demo"
scopes = ["prod"]
reason_code = "source_removed"
detail = "Only hidden in prod."
""".strip()
        + "\n",
        encoding="utf-8",
    )

    rules = load_suppression_rules(registry_path)
    resolved = resolve_suppressions(
        ["daily_demo"],
        rules=rules,
        hostname="devbox",
        extra_scopes="prod",
    )

    assert resolved["daily_demo"]["scope"] == "prod"
    assert resolved["daily_demo"]["reason"] == "source_removed"
