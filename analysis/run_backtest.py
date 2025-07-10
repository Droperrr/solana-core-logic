import pandas as pd
from analysis.strategies.h007_trigger_strategy import H007TriggerStrategy
from analysis.backtesting_engine import Backtester
from analysis.performance_analyzer import PerformanceAnalyzer
from datetime import datetime

BACKTEST_DATA_PATH = "analysis/backtest_input_data.pkl"
REPORT_PATH = "backtest_report.md"

# Параметры стратегии
TAKE_PROFIT = 0.20  # 20%
STOP_LOSS = 0.10    # 10%
TRADE_SIZE = 0.10   # 10% капитала
FEE_PCT = 0.001     # 0.1%
SLIPPAGE_PCT = 0.05 # 0.05%
INITIAL_CAPITAL = 10000.0

def main():
    # 1. Загрузка данных
    df = pd.read_pickle(BACKTEST_DATA_PATH)
    print(f"Loaded {len(df)} rows for backtest.")
    # 2. Инициализация стратегии
    strategy = H007TriggerStrategy(
        take_profit_pct=TAKE_PROFIT,
        stop_loss_pct=STOP_LOSS,
        trade_size_pct=TRADE_SIZE
    )
    # 3. Инициализация и запуск Backtester
    backtester = Backtester(
        strategy=strategy,
        historical_data=df,
        fee_pct=FEE_PCT,
        slippage_pct=SLIPPAGE_PCT,
        initial_capital=INITIAL_CAPITAL
    )
    backtester.run()
    trade_log = backtester.get_trade_log()
    equity_curve = backtester.get_equity_curve()
    print(f"Backtest complete. Trades: {len(trade_log)}. Final capital: {equity_curve.iloc[-1]:.2f}")
    # 4. PerformanceAnalyzer и отчет
    analyzer = PerformanceAnalyzer(
        trade_log=trade_log,
        equity_curve=equity_curve,
        backtest_start=str(df['timestamp'].min()),
        backtest_end=str(df['timestamp'].max())
    )
    analyzer.generate_report(REPORT_PATH)
    print(f"Backtest report generated: {REPORT_PATH}")

if __name__ == "__main__":
    main() 