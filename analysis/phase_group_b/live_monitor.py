#!/usr/bin/env python3
"""
🔄 LIVE МОНИТОРИНГ: Операция Адаптация
Автообновляемый мониторинг прогресса сбора данных для Группы Б
"""

import os
import sys
import sqlite3
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

class LiveGroupBMonitor:
    """Live мониторинг сбора данных для Группы Б"""
    
    def __init__(self):
        self.tokens = self._load_tokens()
        self.start_time = datetime.now()
        self.last_update = None
        self.history = []
        
    def _load_tokens(self) -> List[str]:
        """Загружает токены Группы Б"""
        try:
            with open("data/group_b_tokens.txt", 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []
    
    def get_live_stats(self) -> Dict:
        """Получает актуальную статистику"""
        if not os.path.exists("solana_db.sqlite"):
            return {
                'status': 'DB_NOT_FOUND',
                'progress': 0,
                'tokens_completed': 0,
                'total_transactions': 0,
                'collection_rate': 0,
                'estimated_completion': None
            }
        
        try:
            conn = sqlite3.connect("solana_db.sqlite")
            cursor = conn.cursor()
            
            tokens_completed = 0
            total_transactions = 0
            token_details = []
            
            for token in self.tokens:
                # Проверяем транзакции
                cursor.execute("""
                    SELECT COUNT(*) FROM transactions 
                    WHERE token_a_mint = ? OR token_b_mint = ?
                """, (token, token))
                tx_count = cursor.fetchone()[0]
                
                # Проверяем enriched события
                cursor.execute("""
                    SELECT COUNT(*) FROM ml_ready_events 
                    WHERE token_a_mint = ? OR token_b_mint = ?
                """, (token, token))
                event_count = cursor.fetchone()[0]
                
                # Последняя активность
                cursor.execute("""
                    SELECT MAX(timestamp) FROM transactions 
                    WHERE token_a_mint = ? OR token_b_mint = ?
                """, (token, token))
                last_activity = cursor.fetchone()[0]
                
                if tx_count > 0:
                    tokens_completed += 1
                    total_transactions += tx_count
                
                token_details.append({
                    'address': token,
                    'transactions': tx_count,
                    'events': event_count,
                    'last_activity': last_activity,
                    'completed': tx_count > 0
                })
            
            conn.close()
            
            # Вычисляем прогресс
            progress = (tokens_completed / len(self.tokens)) * 100
            
            # Оценка скорости сбора
            now = datetime.now()
            elapsed_seconds = (now - self.start_time).total_seconds()
            collection_rate = tokens_completed / elapsed_seconds if elapsed_seconds > 0 else 0
            
            # Оценка времени завершения
            remaining_tokens = len(self.tokens) - tokens_completed
            estimated_completion = None
            if collection_rate > 0 and remaining_tokens > 0:
                remaining_seconds = remaining_tokens / collection_rate
                estimated_completion = now + timedelta(seconds=remaining_seconds)
            
            stats = {
                'status': 'COMPLETED' if progress == 100 else 'IN_PROGRESS',
                'progress': progress,
                'tokens_completed': tokens_completed,
                'total_tokens': len(self.tokens),
                'total_transactions': total_transactions,
                'collection_rate': collection_rate * 3600,  # токенов в час
                'estimated_completion': estimated_completion,
                'token_details': token_details,
                'elapsed_time': elapsed_seconds
            }
            
            # Добавляем в историю
            self.history.append({
                'timestamp': now,
                'tokens_completed': tokens_completed,
                'total_transactions': total_transactions
            })
            
            # Оставляем только последние 60 измерений (примерно час при обновлении каждую минуту)
            if len(self.history) > 60:
                self.history = self.history[-60:]
            
            return stats
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'error': str(e),
                'progress': 0
            }
    
    def print_live_dashboard(self, stats: Dict):
        """Выводит live dashboard"""
        
        # Очищаем экран
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # Заголовок с временем
        now = datetime.now()
        print("█" * 80)
        print("🔄 LIVE МОНИТОРИНГ: ОПЕРАЦИЯ АДАПТАЦИЯ")
        print(f"⏰ {now.strftime('%H:%M:%S')} | Запущен: {self.start_time.strftime('%H:%M:%S')}")
        print("█" * 80)
        
        if stats['status'] == 'ERROR':
            print(f"❌ ОШИБКА: {stats.get('error', 'Unknown error')}")
            return
        
        if stats['status'] == 'DB_NOT_FOUND':
            print("⏳ ОЖИДАНИЕ: База данных еще не создана")
            print("💡 Процесс сбора данных запускается...")
            return
        
        # Основная статистика
        print(f"\n📊 СТАТУС СБОРА ДАННЫХ ГРУППЫ Б")
        print(f"   Прогресс: {stats['tokens_completed']}/{stats['total_tokens']} токенов ({stats['progress']:.1f}%)")
        print(f"   Транзакций собрано: {stats['total_transactions']:,}")
        print(f"   Скорость сбора: {stats['collection_rate']:.1f} токенов/час")
        
        # Прогресс-бар
        bar_length = 50
        filled = int(bar_length * stats['progress'] / 100)
        bar = '█' * filled + '░' * (bar_length - filled)
        print(f"\n📈 [{bar}] {stats['progress']:.1f}%")
        
        # Оценка времени завершения
        if stats['estimated_completion']:
            completion_time = stats['estimated_completion'].strftime('%H:%M:%S')
            remaining = stats['estimated_completion'] - now
            remaining_minutes = int(remaining.total_seconds() / 60)
            print(f"⏰ Ожидаемое завершение: {completion_time} (через ~{remaining_minutes} мин)")
        
        # Детали по токенам (последние 10)
        print(f"\n📋 ПОСЛЕДНИЕ ОБРАБОТАННЫЕ ТОКЕНЫ:")
        print("   " + "-" * 70)
        
        recent_tokens = [t for t in stats['token_details'] if t['completed']][-10:]
        for i, token in enumerate(recent_tokens, 1):
            addr_short = f"{token['address'][:6]}...{token['address'][-6:]}"
            print(f"   ✅ {addr_short} | TX: {token['transactions']:4d} | Events: {token['events']:4d}")
        
        # Текущий процесс
        pending_tokens = [t for t in stats['token_details'] if not t['completed']]
        if pending_tokens:
            next_token = pending_tokens[0]['address']
            print(f"\n⚙️ ТЕКУЩИЙ: {next_token[:6]}...{next_token[-6:]}")
        
        # Статистика изменений
        if len(self.history) > 1:
            prev = self.history[-2]
            current = self.history[-1]
            
            tx_diff = current['total_transactions'] - prev['total_transactions']
            token_diff = current['tokens_completed'] - prev['tokens_completed']
            
            print(f"\n📈 ИЗМЕНЕНИЯ ЗА ПОСЛЕДНЮЮ МИНУТУ:")
            print(f"   +{token_diff} токенов | +{tx_diff:,} транзакций")
        
        # Статус
        if stats['status'] == 'COMPLETED':
            print(f"\n🎉 СБОР ДАННЫХ ЗАВЕРШЕН!")
            print(f"   ✅ Все {stats['total_tokens']} токенов обработаны")
            print(f"   📊 Собрано {stats['total_transactions']:,} транзакций")
            print(f"   ⚡ Готов к анализу паттернов!")
        else:
            remaining = stats['total_tokens'] - stats['tokens_completed']
            print(f"\n⚙️ СБОР ДАННЫХ ПРОДОЛЖАЕТСЯ")
            print(f"   📥 Осталось: {remaining} токенов")
            print(f"   ⏳ Следующее обновление через 60 сек...")
        
        print("█" * 80)
    
    def run_live_monitoring(self):
        """Запускает live мониторинг"""
        print("🚀 Запуск live мониторинга операции адаптации...")
        print("⚠️  Нажмите Ctrl+C для выхода")
        
        try:
            while True:
                stats = self.get_live_stats()
                self.print_live_dashboard(stats)
                
                # Сохраняем snapshot
                self._save_snapshot(stats)
                
                # Если завершено, выходим
                if stats.get('status') == 'COMPLETED':
                    print("\n🎯 ГОТОВ К АНАЛИЗУ! Запустите:")
                    print("   python analysis/phase_group_b/validate_entry_pattern.py")
                    break
                
                # Ждем минуту
                time.sleep(60)
                
        except KeyboardInterrupt:
            print(f"\n\n👋 Live мониторинг остановлен")
            print(f"   Для возобновления: python analysis/phase_group_b/live_monitor.py")
    
    def _save_snapshot(self, stats: Dict):
        """Сохраняет snapshot статистики"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"output/live_monitoring_snapshot_{timestamp}.json"
        
        snapshot = {
            'timestamp': datetime.now().isoformat(),
            'stats': stats,
            'monitoring_duration_minutes': (datetime.now() - self.start_time).total_seconds() / 60
        }
        
        os.makedirs('output', exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False, default=str)

def main():
    """Главная функция live мониторинга"""
    monitor = LiveGroupBMonitor()
    monitor.run_live_monitoring()

if __name__ == "__main__":
    main() 