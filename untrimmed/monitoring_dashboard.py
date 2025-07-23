"""Monitoring placeholders for the untrimmed pipeline."""

from __future__ import annotations

from typing import Dict
from datetime import datetime


class MonitoringSystem:
    def __init__(self) -> None:
        self.metrics: Dict[str, float] = {}

    def record_metric(self, name: str, value: float) -> None:
        self.metrics[name] = value

    def generate_report(self) -> Dict[str, float]:
        return dict(self.metrics)
