"""Basic backtesting engine used by example strategies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np


@dataclass
class Trade:
    entry_time: datetime
    exit_time: Optional[datetime]
    entry_price: float
    exit_price: Optional[float]
    position_size: float
    side: str
    symbol: str
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

    @property
    def is_open(self) -> bool:
        return self.exit_time is None

    @property
    def pnl(self) -> float:
        if self.exit_price is None:
            return 0.0
        if self.side == "long":
            return (self.exit_price - self.entry_price) * self.position_size
        return (self.entry_price - self.exit_price) * self.position_size


class BacktestingEngine:
    def __init__(self, initial_capital: float = 10000):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.trades: List[Trade] = []

    def run_backtest(
        self, strategy: "Strategy", data: pd.DataFrame
    ) -> Dict[str, Any]:
        strategy.initialize()
        for ts, row in data.iterrows():
            signal = strategy.generate_signal(ts, row, data.loc[:ts])
            if signal:
                self._execute_signal(signal, ts, row)
        self._close_all(row)
        return {
            "final_equity": self.capital,
            "total_trades": len(self.trades),
        }

    def _execute_signal(self, signal: Dict[str, Any], ts: datetime, row: pd.Series) -> None:
        action = signal.get("action")
        if action in {"buy", "sell"}:
            side = "long" if action == "buy" else "short"
            trade = Trade(
                entry_time=ts,
                exit_time=None,
                entry_price=row["close"],
                exit_price=None,
                position_size=1.0,
                side=side,
                symbol=signal.get("symbol", "TICKER"),
            )
            self.trades.append(trade)
        elif action == "close":
            for t in self.trades:
                if t.is_open:
                    t.exit_time = ts
                    t.exit_price = row["close"]
                    self.capital += t.pnl

    def _close_all(self, last_row: pd.Series) -> None:
        for t in self.trades:
            if t.is_open:
                t.exit_time = last_row.name
                t.exit_price = last_row["close"]
                self.capital += t.pnl
