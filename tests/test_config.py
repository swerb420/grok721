import pytest
import importlib

import config


def test_get_config_warns_for_placeholder(monkeypatch):
    monkeypatch.delenv("PLACEHOLDER_KEY", raising=False)
    with pytest.warns(RuntimeWarning):
        value = config.get_config("PLACEHOLDER_KEY", "xxxxxxxxxx")
    assert value == "xxxxxxxxxx"


def test_get_config_warns_for_env_placeholder(monkeypatch):
    monkeypatch.setenv("PLACEHOLDER_KEY", "xxxxxxxxxx")
    with pytest.warns(RuntimeWarning):
        value = config.get_config("PLACEHOLDER_KEY")
    assert value == "xxxxxxxxxx"

