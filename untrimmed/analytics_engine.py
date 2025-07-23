"""Minimal analytics helper."""

from __future__ import annotations

from typing import Dict, List
import numpy as np


class AdvancedAnalytics:
    def moving_average(self, data: List[float], window: int = 3) -> List[float]:
        if window <= 0:
            return []
        res = []
        for i in range(len(data)):
            start = max(0, i - window + 1)
            res.append(float(np.mean(data[start : i + 1])))
        return res

    def detect_spikes(self, data: List[float], threshold: float = 2.0) -> List[int]:
        arr = np.array(data)
        mean = arr.mean()
        std = arr.std() or 1.0
        z = (arr - mean) / std
        return [i for i, v in enumerate(z) if abs(v) > threshold]
