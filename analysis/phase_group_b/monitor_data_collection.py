#!/usr/bin/env python3
"""
Мониторинг прогресса сбора данных для Группы Б
Показывает красивый прогресс с детальной статистикой
"""

import os
import sys
import sqlite3
import time
import json
from datetime import datetime
from typing import Dict, List, Tuple

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

class GroupBDataCollectionMonitor:
    """Мониторинг сбора данных для Группы Б"""
    
    def __init__(self, db_path: str = "solana_db.sqlite"):
        self.db_path = db_path
        self.tokens = self._load_tokens()
        
    def _load_tokens(self) -> List[str]:
        """Загружает список токенов Группы Б"""
        try:
            with open("data/group_b_tokens.txt", 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []
    
    def get_collection_stats(self) -> Dict:
        """Получает статистику сбора данных"""
        if not os.path.exists(self.db_path):
            return {
                'tokens_total': len(self.tokens),
                'tokens_collected': 0,
                'transactions_total': 0,
                'enriched_events': 0,
                'tokens_details': [],
                'collection_status': 'NOT_STARTED'
            }
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {
            'tokens_total': len(self.tokens),
            'tokens_collected': 0,
            'transactions_total': 0,
            'enriched_events': 0,
            'tokens_details': [],
            'collection_status': 'IN_PROGRESS'
        }
        
        # Проверяем каждый токен
        for token in self.tokens:
            # Количество транзакций
            cursor.execute("""
                SELECT COUNT(*) FROM transactions 
                WHERE token_a_mint = ? OR token_b_mint = ?
            """, (token, token))
            tx_count = cursor.fetchone()[0]
            
            # Количество enriched событий
            cursor.execute("""
                SELECT COUNT(*) FROM ml_ready_events 
                WHERE token_a_mint = ? OR token_b_mint = ?
            """, (token, token))
            enriched_count = cursor.fetchone()[0]
            
            # Последняя активность
            cursor.execute("""
                SELECT MAX(timestamp) FROM transactions 
                WHERE token_a_mint = ? OR token_b_mint = ?
            """, (token, token))
            last_activity = cursor.fetchone()[0]
            
            token_info = {
                'address': token,
                'transactions': tx_count,
                'enriched_events': enriched_count,
                'last_activity': last_activity,
                'status': 'COLLECTED' if tx_count > 0 else 'PENDING'
            }
            
            stats['tokens_details'].append(token_info)
            
            if tx_count > 0:
                stats['tokens_collected'] += 1
            
            stats['transactions_total'] += tx_count
            stats['enriched_events'] += enriched_count
        
        # Определяем общий статус
        if stats['tokens_collected'] == 0:
            stats['collection_status'] = 'NOT_STARTED'
        elif stats['tokens_collected'] == stats['tokens_total']:
            stats['collection_status'] = 'COMPLETED'
        else:
            stats['collection_status'] = 'IN_PROGRESS'
        
        conn.close()
        return stats
    
    def print_beautiful_status(self, stats: Dict):
        """Выводит красивый статус сбора данных"""
        
        # Заголовок
        print("\n" + "="*80)
        print("🚀 ОПЕРАЦИЯ: АДАПТАЦИЯ - МОНИТОРИНГ СБОРА ДАННЫХ ГРУППЫ Б")
        print("="*80)
        
        # Общая статистика
        progress_pct = (stats['tokens_collected'] / stats['tokens_total']) * 100
        status_emoji = {
            'NOT_STARTED': '⏳',
            'IN_PROGRESS': '⚙️',
            'COMPLETED': '✅'
        }
        
        print(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
        print(f"   Статус: {status_emoji[stats['collection_status']]} {stats['collection_status']}")
        print(f"   Прогресс: {stats['tokens_collected']}/{stats['tokens_total']} токенов ({progress_pct:.1f}%)")
        print(f"   Транзакций собрано: {stats['transactions_total']:,}")
        print(f"   Событий обогащено: {stats['enriched_events']:,}")
        
        # Прогресс-бар
        bar_length = 50
        filled_length = int(bar_length * progress_pct / 100)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        print(f"\n📈 ПРОГРЕСС: [{bar}] {progress_pct:.1f}%")
        
        # Детали по токенам
        if stats['tokens_details']:
            print(f"\n📋 ДЕТАЛИ ПО ТОКЕНАМ:")
            print("   " + "-"*76)
            print("   №  | Статус | Транзакций | События | Адрес")
            print("   " + "-"*76)
            
            for i, token in enumerate(stats['tokens_details'], 1):
                status_symbol = '✅' if token['status'] == 'COLLECTED' else '⏳'
                address_short = f"{token['address'][:8]}...{token['address'][-8:]}"
                
                print(f"   {i:2d} | {status_symbol}     | {token['transactions']:8d} | {token['enriched_events']:6d} | {address_short}")
        
        print("   " + "-"*76)
        
        # Рекомендации
        print(f"\n💡 РЕКОМЕНДАЦИИ:")
        if stats['collection_status'] == 'NOT_STARTED':
            print("   🔄 Запустите сбор данных: python scripts/batch_process_all_tokens.py --tokens-file data/group_b_tokens.txt")
        elif stats['collection_status'] == 'IN_PROGRESS':
            print("   ⏳ Сбор данных выполняется. Ожидайте завершения...")
            print("   📊 Запустите анализ частично собранных данных если нужно")
        else:
            print("   🎯 Сбор завершен! Переходите к анализу паттерна:")
            print("   📈 python analysis/phase_group_b/validate_entry_pattern.py")
        
        print("\n" + "="*80)
    
    def save_progress_report(self, stats: Dict):
        """Сохраняет отчет о прогрессе"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"output/group_b_collection_progress_{timestamp}.json"
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'collection_stats': stats,
            'analysis_ready': stats['collection_status'] == 'COMPLETED'
        }
        
        os.makedirs('output', exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"📄 Отчет сохранен: {filename}")

def main():
    """Главная функция мониторинга"""
    monitor = GroupBDataCollectionMonitor()
    
    try:
        while True:
            # Получаем текущую статистику
            stats = monitor.get_collection_stats()
            
            # Очищаем экран (для Windows)
            os.system('cls' if os.name == 'nt' else 'clear')
            
            # Выводим красивый статус
            monitor.print_beautiful_status(stats)
            
            # Сохраняем отчет
            monitor.save_progress_report(stats)
            
            # Если сбор завершен, выходим
            if stats['collection_status'] == 'COMPLETED':
                print("\n🎉 СБОР ДАННЫХ ЗАВЕРШЕН! Готово к анализу.")
                break
            
            # Ждем перед следующим обновлением
            print(f"\n⏳ Следующее обновление через 30 секунд... (Ctrl+C для выхода)")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print(f"\n\n👋 Мониторинг остановлен пользователем.")
        print(f"   Для возобновления: python analysis/phase_group_b/monitor_data_collection.py")

if __name__ == "__main__":
    main() 