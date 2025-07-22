import pytest

class DummyError(Exception):
    pass


def test_retry_func_preserves_exception(monkeypatch, gas_module):
    calls = []

    def always_fail():
        calls.append(1)
        raise DummyError("boom")

    monkeypatch.setattr(gas_module.time, "sleep", lambda s: None)
    # ensure all HTTP methods exist on the requests stub
    for method in ["put", "delete", "patch", "head", "options"]:
        monkeypatch.setattr(
            gas_module.requests, method, lambda *a, **k: None, raising=False
        )

    with pytest.raises(DummyError):
        gas_module.retry_func(always_fail, retries=3, base_backoff=0)

    assert len(calls) == 3
