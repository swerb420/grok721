"""Simple backtesting engine used for examples."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
import pandas as pd


@dataclass
class Trade:
    entry_time: datetime
    exit_time: Optional[datetime]
    entry_price: float
    exit_price: Optional[float]
    side: str
    position_size: float

    @property
    def pnl(self) -> float:
        if self.exit_price is None:
            return 0.0
        return (self.exit_price - self.entry_price) * self.position_size if self.side == "long" else (self.entry_price - self.exit_price) * self.position_size


class BacktestingEngine:
    def __init__(self, initial_capital: float = 100000):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.trades: List[Trade] = []

    def run_backtest(self, strategy: 'Strategy', data: pd.DataFrame) -> Dict[str, Any]:
        strategy.initialize()
        for timestamp, row in data.iterrows():
            signal = strategy.generate_signal(timestamp, row, data.loc[:timestamp])
            if signal and signal.get('action') == 'buy':
                trade = Trade(timestamp, timestamp, row['close'], row['close'], 'long', 1)
                self.trades.append(trade)
        return {'trades': len(self.trades)}
