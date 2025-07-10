from typing import Optional

class H007TriggerStrategy:
    """
    Торговая стратегия на основе сигнала H-007 (UNKNOWN event с defi_related классификацией).
    Параметры:
        take_profit_pct: процент прибыли для фиксации (например, 0.05 для 5%)
        stop_loss_pct: процент убытка для ограничения (например, 0.02 для 2%)
        trade_size_pct: доля портфеля для сделки (например, 0.1 для 10%)
    """
    def __init__(self, take_profit_pct: float, stop_loss_pct: float, trade_size_pct: float):
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.trade_size_pct = trade_size_pct

    def check_entry_signal(self, event: dict) -> bool:
        """
        Проверяет, является ли событие триггером для входа в сделку.
        Базовая логика: event_type == 'UNKNOWN' и unknown_classification == 'defi_related'.
        """
        return (
            event.get('event_type') == 'UNKNOWN' and
            event.get('unknown_classification') == 'defi_related'
        )

    def check_exit_signal(self, current_price: float, entry_price: float) -> Optional[str]:
        """
        Проверяет, достигнута ли цель по прибыли или убытку.
        Возвращает 'TAKE_PROFIT', 'STOP_LOSS' или None.
        """
        if entry_price == 0:
            return None
        change_pct = (current_price - entry_price) / entry_price
        if change_pct <= -self.stop_loss_pct:
            return 'STOP_LOSS'
        if change_pct >= self.take_profit_pct:
            return 'TAKE_PROFIT'
        return None 