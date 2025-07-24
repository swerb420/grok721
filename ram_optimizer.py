"""Simple memory-aware task scheduler.

This module provides utilities to run commands only when enough memory is
available. It can help avoid overwhelming systems with limited RAM by
blocking until a minimum amount of free memory is detected.
"""

from __future__ import annotations

from typing import Iterable, List
import logging
import subprocess
import time

import psutil


def available_memory_mb() -> int:
    """Return available system memory in megabytes."""
    return psutil.virtual_memory().available // (1024 * 1024)


def run_when_memory_free(
    cmd: List[str],
    min_free_mb: int,
    check_interval: int = 5,
    timeout: int | None = None,
    raise_on_error: bool = False,
) -> int:
    """Run *cmd* when at least ``min_free_mb`` of memory is free.

    The function checks memory usage every ``check_interval`` seconds and
    blocks until enough memory is available. If ``timeout`` is provided, the
    wait is limited to at most that many seconds. When the timeout expires the
    function returns ``1`` without executing the command. The command's exit
    code is returned when it does run.
    """
    start = time.time()
    while available_memory_mb() < min_free_mb:
        if timeout is not None and time.time() - start >= timeout:
            return 1
        time.sleep(check_interval)

    result = subprocess.run(cmd)
    if result.returncode != 0:
        logging.error("Command %s failed with return code %s", cmd, result.returncode)
        if raise_on_error:
            raise subprocess.CalledProcessError(result.returncode, cmd)
    return result.returncode


def schedule_commands(
    commands: Iterable[List[str]],
    min_free_mb: int,
    check_interval: int = 5,
    raise_on_error: bool = False,
) -> None:
    """Run a sequence of commands when enough memory is available."""
    for cmd in commands:
        run_when_memory_free(
            cmd,
            min_free_mb,
            check_interval,
            raise_on_error=raise_on_error,
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run commands sequentially when sufficient memory is free."
    )
    parser.add_argument(
        "--min-free-mb",
        type=int,
        default=512,
        help="Minimum free memory in MB required to start a command.",
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=5,
        help="Seconds between memory checks while waiting.",
    )
    parser.add_argument(
        "commands",
        nargs="+",
        help="Commands to execute sequentially.",
    )
    args = parser.parse_args()
    schedule_commands(
        [c.split() for c in args.commands],
        args.min_free_mb,
        args.check_interval,
    )
