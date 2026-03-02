from signal_noise.cli import _parse_excludes, _select_collectors
from signal_noise.collector import COLLECTORS


def test_parse_excludes_csv():
    assert _parse_excludes("a,b, c ,,") == {"a", "b", "c"}
    assert _parse_excludes("") == set()
    assert _parse_excludes(None) == set()


def test_select_collectors_respects_exclude():
    first = next(iter(COLLECTORS.keys()))
    selected = _select_collectors(exclude={first})
    assert first not in selected
    assert len(selected) == len(COLLECTORS) - 1
