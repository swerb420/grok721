import types
import subprocess
import pytest
import ram_optimizer


def test_available_memory_mb(monkeypatch):
    mem = types.SimpleNamespace(available=1024 * 1024 * 1024)
    monkeypatch.setattr(ram_optimizer.psutil, "virtual_memory", lambda: mem)
    assert ram_optimizer.available_memory_mb() == 1024


def test_run_when_memory_free(monkeypatch):
    calls = []
    avail = [100, 200, 800]

    def fake_available():
        return avail.pop(0)

    monkeypatch.setattr(ram_optimizer, "available_memory_mb", fake_available)
    monkeypatch.setattr(ram_optimizer.time, "sleep", lambda s: calls.append(s))

    cmd = []
    monkeypatch.setattr(
        ram_optimizer.subprocess,
        "run",
        lambda c: cmd.append(c) or types.SimpleNamespace(returncode=0),
    )

    rc = ram_optimizer.run_when_memory_free(
        ["echo", "hi"],
        500,
        check_interval=1,
    )
    assert rc == 0
    assert len(calls) == 2
    assert cmd == [["echo", "hi"]]


def test_schedule_commands(monkeypatch):
    executed = []
    monkeypatch.setattr(
        ram_optimizer,
        "run_when_memory_free",
        lambda cmd, *a, **k: executed.append(cmd),
    )
    ram_optimizer.schedule_commands([["a"], ["b"]], 100)
    assert executed == [["a"], ["b"]]


def test_run_when_memory_free_error(monkeypatch):
    monkeypatch.setattr(ram_optimizer, "available_memory_mb", lambda: 1000)
    monkeypatch.setattr(ram_optimizer.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        ram_optimizer.subprocess,
        "run",
        lambda c: types.SimpleNamespace(returncode=1),
    )
    rc = ram_optimizer.run_when_memory_free(["cmd"], 10)
    assert rc == 1
    with pytest.raises(subprocess.CalledProcessError):
        ram_optimizer.run_when_memory_free(["cmd"], 10, raise_on_error=True)


def test_schedule_commands_raises(monkeypatch):
    def fake(cmd, *a, **k):
        raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(ram_optimizer, "run_when_memory_free", fake)
    with pytest.raises(subprocess.CalledProcessError):
        ram_optimizer.schedule_commands([["a"]], 100, raise_on_error=True)
