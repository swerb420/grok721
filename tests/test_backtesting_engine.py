import pandas as pd
from backtesting_engine import BacktestingEngine

class DummyStrategy:
    def initialize(self):
        pass
    def generate_signal(self, ts, row, history):
        return {'action': 'buy'}

def test_capital_updates_after_trades():
    data = pd.DataFrame(
        {
            'open': [100, 105, 110],
            'close': [105, 110, 115],
        },
        index=pd.date_range('2021-01-01', periods=3, freq='D')
    )
    engine = BacktestingEngine(initial_capital=1000)
    result = engine.run_backtest(DummyStrategy(), data)
    assert result['trades'] == 3
    assert result['final_capital'] == 1015
    assert result['cumulative_profit'] == 15
