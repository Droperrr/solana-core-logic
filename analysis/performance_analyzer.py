import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from typing import Optional

class PerformanceAnalyzer:
    def __init__(self, trade_log: pd.DataFrame, equity_curve: pd.Series, backtest_start: Optional[str] = None, backtest_end: Optional[str] = None):
        self.trade_log = trade_log.copy()
        self.equity_curve = equity_curve.copy()
        self.backtest_start = backtest_start
        self.backtest_end = backtest_end
        self.metrics = {}
        self._calculate_all_metrics()

    def _calculate_all_metrics(self):
        log = self.trade_log
        eq = self.equity_curve
        self.metrics['total_trades'] = len(log)
        self.metrics['final_capital'] = eq.iloc[-1] if len(eq) > 0 else None
        self.metrics['net_pnl_pct'] = ((eq.iloc[-1] / eq.iloc[0] - 1) * 100) if len(eq) > 1 else None
        self.metrics['profit_factor'] = self.calculate_profit_factor()
        self.metrics['win_rate'] = self.calculate_win_rate()
        self.metrics['max_drawdown'] = self.calculate_max_drawdown()
        self.metrics['sharpe_ratio'] = self.calculate_sharpe_ratio()
        self.metrics['avg_win'] = self.calculate_avg_win()
        self.metrics['avg_loss'] = self.calculate_avg_loss()
        self.metrics['max_consec_wins'] = self.calculate_max_consecutive(True)
        self.metrics['max_consec_losses'] = self.calculate_max_consecutive(False)
        self.metrics['avg_holding_period'] = self.calculate_avg_holding_period()

    def calculate_profit_factor(self):
        wins = self.trade_log[self.trade_log['pnl_abs'] > 0]['pnl_abs'].sum()
        losses = -self.trade_log[self.trade_log['pnl_abs'] < 0]['pnl_abs'].sum()
        if losses == 0:
            return np.inf if wins > 0 else 0
        return wins / losses

    def calculate_win_rate(self):
        if len(self.trade_log) == 0:
            return None
        wins = (self.trade_log['pnl_abs'] > 0).sum()
        return wins / len(self.trade_log) * 100

    def calculate_max_drawdown(self):
        eq = self.equity_curve.values
        if len(eq) < 2:
            return 0
        peak = eq[0]
        max_dd = 0
        for x in eq:
            if x > peak:
                peak = x
            dd = (peak - x) / peak
            if dd > max_dd:
                max_dd = dd
        return max_dd * 100

    def calculate_sharpe_ratio(self):
        returns = self.equity_curve.pct_change().dropna()
        if len(returns) == 0:
            return None
        return (returns.mean() / returns.std()) * np.sqrt(252)  # 252 торговых дня

    def calculate_avg_win(self):
        wins = self.trade_log[self.trade_log['pnl_abs'] > 0]['pnl_abs']
        return wins.mean() if not wins.empty else 0

    def calculate_avg_loss(self):
        losses = self.trade_log[self.trade_log['pnl_abs'] < 0]['pnl_abs']
        return losses.mean() if not losses.empty else 0

    def calculate_max_consecutive(self, win: bool):
        arr = (self.trade_log['pnl_abs'] > 0) if win else (self.trade_log['pnl_abs'] < 0)
        max_streak = streak = 0
        for val in arr:
            if val:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
        return max_streak

    def calculate_avg_holding_period(self):
        if len(self.trade_log) == 0:
            return None
        entry = pd.to_datetime(self.trade_log['entry_time'])
        exit = pd.to_datetime(self.trade_log['exit_time'])
        holding = (exit - entry).dt.total_seconds() / 3600  # в часах
        return holding.mean()

    def _verdict(self):
        pf = self.metrics['profit_factor']
        dd = self.metrics['max_drawdown']
        if pf is not None and dd is not None and pf > 1.5 and dd < 20:
            return f"GO: Profit Factor {pf:.2f} > 1.5 and Max Drawdown {dd:.2f}% < 20%"
        return f"NO-GO: Profit Factor {pf:.2f}, Max Drawdown {dd:.2f}%"

    def _trigger_event_analysis(self):
        # Группировка по unknown_classification и unknown_reason
        def extract(event, key):
            if isinstance(event, dict):
                return event.get(key, None)
            return None
        df = self.trade_log.copy()
        df['unknown_classification'] = df['trigger_event'].apply(lambda e: extract(e, 'unknown_classification'))
        df['unknown_reason'] = df['trigger_event'].apply(lambda e: extract(e, 'unknown_reason'))
        grouped = df.groupby(['unknown_classification', 'unknown_reason'])['pnl_abs'].agg(['count', 'sum', 'mean'])
        return grouped.sort_values('mean', ascending=False)

    def _equity_curve_plot(self, output_path: str):
        plt.figure(figsize=(10, 4))
        self.equity_curve.plot()
        plt.title('Equity Curve')
        plt.xlabel('Step')
        plt.ylabel('Capital')
        plt.tight_layout()
        img_path = output_path.replace('.md', '_equity.png')
        plt.savefig(img_path)
        plt.close()
        return img_path

    def generate_report(self, output_path: str):
        # 1. Executive Summary
        start = self.backtest_start or str(self.trade_log['entry_time'].min())
        end = self.backtest_end or str(self.trade_log['exit_time'].max())
        final_cap = self.metrics['final_capital']
        net_pnl = self.metrics['net_pnl_pct']
        verdict = self._verdict()
        # 2. KPIs
        kpi_table = pd.DataFrame({k: [v] for k, v in self.metrics.items()}).T
        kpi_table.columns = ['Value']
        # 3. Equity Curve
        eq_img = self._equity_curve_plot(output_path)
        # 4. Trade Log Summary
        first5 = self.trade_log.head(5)
        last5 = self.trade_log.tail(5)
        # 5. Trigger Event Analysis
        trigger_analysis = self._trigger_event_analysis()
        # Markdown report
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(f"# Backtest Report\n\n")
            f.write(f"## Executive Summary\n")
            f.write(f"- **Strategy Name:** H007 Trigger Strategy\n")
            f.write(f"- **Backtest Period:** {start} to {end}\n")
            f.write(f"- **Final Capital:** {final_cap:.2f}\n")
            f.write(f"- **Net P&L (%):** {net_pnl:.2f}\n")
            f.write(f"- **GO/NO-GO Verdict:** {verdict}\n\n")
            f.write(f"## Key Performance Indicators (KPIs)\n")
            f.write(kpi_table.to_markdown() + '\n\n')
            f.write(f"## Equity Curve Visualization\n")
            f.write(f"![Equity Curve]({eq_img})\n\n")
            f.write(f"## Trade Log Summary\n")
            f.write(f"### First 5 Trades\n")
            f.write(first5.to_markdown() + '\n\n')
            f.write(f"### Last 5 Trades\n")
            f.write(last5.to_markdown() + '\n\n')
            f.write(f"## Analysis of Trigger Events\n")
            f.write(trigger_analysis.to_markdown() + '\n\n') 