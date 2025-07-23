"""Simple API usage tracker."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict
from datetime import datetime


class APIOptimizer:
    def __init__(self) -> None:
        self.usage = defaultdict(int)

    def track(self, api_name: str) -> None:
        self.usage[api_name] += 1

    def report(self) -> Dict[str, int]:
        return dict(self.usage)
