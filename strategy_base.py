"""Base classes for strategies with example implementations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional
from datetime import datetime
import pandas as pd


class Strategy(ABC):
    def __init__(self, name: str):
        self.name = name
        self.parameters: Dict[str, Any] = {}

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def generate_signal(
        self, timestamp: datetime, current_data: pd.Series, historical_data: pd.DataFrame
    ) -> Optional[Dict[str, Any]]:
        pass


class ExampleStrategy(Strategy):
    def __init__(self):
        super().__init__("Example")

    def initialize(self):
        pass

    def generate_signal(self, timestamp, current_data, historical_data):
        if current_data.get("close", 0) > 0:
            return {"action": "buy"}
        return None
