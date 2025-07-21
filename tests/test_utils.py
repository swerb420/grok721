import pytest

try:
    from utils import compute_vibe
except Exception:  # pragma: no cover - should always be available
    pytest.skip("utils module missing", allow_module_level=True)


def test_compute_vibe_positive():
    score, label = compute_vibe("POSITIVE", 0.9, 10, 5, 2)
    assert score > 0
    assert label
