import types
import pytest


def test_execute_dune_query_http_error(monkeypatch, dune_module):
    class DummyHTTPError(Exception):
        def __init__(self, status):
            self.response = types.SimpleNamespace(status_code=status)

    class Response:
        def __init__(self, status):
            self.status_code = status
        def json(self):
            return {}
        def raise_for_status(self):
            if self.status_code >= 400:
                raise DummyHTTPError(self.status_code)

    monkeypatch.setattr(dune_module.requests, "post", lambda *a, **k: Response(400))
    monkeypatch.setattr(dune_module.requests, "get", lambda *a, **k: Response(200))

    with pytest.raises(DummyHTTPError):
        dune_module.execute_dune_query("123", "key")
