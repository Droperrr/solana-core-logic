import pandas as pd
from typing import Any, Optional

class Backtester:
    """
    Универсальный движок для симуляции торговых стратегий на исторических данных.
    """
    def __init__(self, strategy: Any, historical_data: pd.DataFrame, fee_pct: float = 0.001, slippage_pct: float = 0.05, initial_capital: float = 10000.0):
        """
        :param strategy: объект стратегии (например, H007TriggerStrategy)
        :param historical_data: DataFrame с событиями и ценами, отсортированный по времени
        :param fee_pct: комиссия за сделку (по умолчанию 0.1%)
        :param slippage_pct: проскальзывание в процентах (по умолчанию 0.05%)
        :param initial_capital: начальный капитал (по умолчанию 10000)
        """
        self.strategy = strategy
        self.data = historical_data.reset_index(drop=True)
        self.fee_pct = fee_pct
        self.slippage_pct = slippage_pct
        self.initial_capital = initial_capital
        self.trade_log = []
        self.equity_curve = []

    def run(self):
        position = None  # None или dict с инфо о позиции
        capital = self.initial_capital
        self.equity_curve = [capital]
        for idx, row in self.data.iterrows():
            event = row.get('event', {}) if 'event' in row else {}
            price = row['price'] if 'price' in row else None
            timestamp = row['timestamp'] if 'timestamp' in row else None

            # Проверка сигнала на вход
            if position is None and self.strategy.check_entry_signal(event):
                # Размер позиции от текущего капитала
                position_size = capital * self.strategy.trade_size_pct
                # Цена входа с учетом комиссии и проскальзывания (short)
                entry_price = price * (1 - self.fee_pct) * (1 - self.slippage_pct / 100)
                position = {
                    'entry_time': timestamp,
                    'entry_price': entry_price,
                    'size_pct': self.strategy.trade_size_pct,
                    'position_size': position_size,
                    'trigger_event': event
                }
                continue

            # Проверка сигнала на выход
            if position is not None:
                exit_reason = self.strategy.check_exit_signal(price, position['entry_price'])
                if exit_reason:
                    # Цена выхода с учетом комиссии и проскальзывания (short)
                    exit_price = price * (1 - self.fee_pct) * (1 + self.slippage_pct / 100)
                    pnl_pct = (exit_price - position['entry_price']) / position['entry_price']
                    pnl_abs = position['position_size'] * pnl_pct
                    capital += pnl_abs
                    self.trade_log.append({
                        'entry_time': position['entry_time'],
                        'exit_time': timestamp,
                        'entry_price': position['entry_price'],
                        'exit_price': exit_price,
                        'pnl_pct': pnl_pct,
                        'pnl_abs': pnl_abs,
                        'exit_reason': exit_reason,
                        'trigger_event': position['trigger_event']
                    })
                    position = None  # Закрываем позицию
            self.equity_curve.append(capital)

    def get_trade_log(self) -> pd.DataFrame:
        return pd.DataFrame(self.trade_log)

    def get_equity_curve(self) -> pd.Series:
        return pd.Series(self.equity_curve) 