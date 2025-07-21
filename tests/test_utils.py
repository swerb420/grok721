import pytest

try:
    from main import compute_vibe
except Exception:  # pragma: no cover - optional deps may not be installed
    pytest.skip("main module requires optional dependencies", allow_module_level=True)


def test_compute_vibe_positive():
    score, label = compute_vibe("POSITIVE", 0.9, 10, 5, 2)
    assert score > 0
    assert label
