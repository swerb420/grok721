"""Simplified strategy framework used in untrimmed pipeline examples.
This is a trimmed version of the long example sent in the issue.
It implements a basic Strategy interface and a few toy strategies.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from datetime import datetime

import pandas as pd
import numpy as np


class Strategy(ABC):
    """Base class for trading strategies."""

    def __init__(self, name: str):
        self.name = name
        self.parameters: Dict[str, Any] = {}
        self.state: Dict[str, Any] = {}

    @abstractmethod
    def initialize(self) -> None:
        """Initialize strategy state."""

    @abstractmethod
    def generate_signal(
        self, timestamp: datetime, current: pd.Series, history: pd.DataFrame
    ) -> Optional[Dict[str, Any]]:
        """Generate a trading signal."""

    def update_parameters(self, params: Dict[str, Any]) -> None:
        self.parameters.update(params)

    def get_parameters(self) -> Dict[str, Any]:
        return dict(self.parameters)


class SentimentMomentumStrategy(Strategy):
    """Example strategy using sentiment momentum."""

    def __init__(self) -> None:
        super().__init__("Sentiment Momentum")
        self.parameters = {
            "sentiment_threshold": 0.6,
            "momentum_period": 10,
        }

    def initialize(self) -> None:
        self.state = {"position": None}

    def generate_signal(
        self, timestamp: datetime, current: pd.Series, history: pd.DataFrame
    ) -> Optional[Dict[str, Any]]:
        if len(history) < self.parameters["momentum_period"]:
            return None
        momentum = (
            history["sentiment_score"].iloc[-1]
            - history["sentiment_score"].iloc[-self.parameters["momentum_period"]]
        )
        if (
            current.get("sentiment_score", 0.5) > self.parameters["sentiment_threshold"]
            and momentum > 0
        ):
            self.state["position"] = "long"
            return {"action": "buy", "symbol": current.get("symbol", "TICKER")}
        return None


class MeanReversionStrategy(Strategy):
    """Very simple mean reversion example."""

    def __init__(self) -> None:
        super().__init__("Mean Reversion")
        self.parameters = {"window": 20}

    def initialize(self) -> None:
        self.state = {"position": None}

    def generate_signal(
        self, timestamp: datetime, current: pd.Series, history: pd.DataFrame
    ) -> Optional[Dict[str, Any]]:
        window = self.parameters["window"]
        if len(history) < window:
            return None
        avg = history["close"].iloc[-window:].mean()
        if current["close"] < 0.98 * avg:
            return {"action": "buy", "symbol": current.get("symbol", "TICKER")}
        if current["close"] > 1.02 * avg:
            return {"action": "sell", "symbol": current.get("symbol", "TICKER")}
        return None


__all__ = [
    "Strategy",
    "SentimentMomentumStrategy",
    "MeanReversionStrategy",
]
