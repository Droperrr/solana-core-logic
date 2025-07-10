#!/usr/bin/env python3
"""
Быстрая проверка прогресса сбора данных для Группы Б
"""

import os
import sys
import sqlite3
from datetime import datetime

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def check_group_b_progress():
    """Проверяет прогресс сбора данных для Группы Б"""
    
    # Загружаем токены
    try:
        with open("data/group_b_tokens.txt", 'r') as f:
            tokens = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("❌ Файл data/group_b_tokens.txt не найден")
        return
    
    print(f"🎯 Проверка прогресса для {len(tokens)} токенов Группы Б...")
    
    # Проверяем БД
    if not os.path.exists("solana_db.sqlite"):
        print("❌ База данных solana_db.sqlite не найдена")
        print("💡 Сбор данных еще не начался")
        return
    
    conn = sqlite3.connect("solana_db.sqlite")
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("📊 СТАТУС СБОРА ДАННЫХ ГРУППЫ Б")
    print("="*60)
    
    tokens_with_data = 0
    total_transactions = 0
    total_events = 0
    
    for i, token in enumerate(tokens, 1):
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
        
        if tx_count > 0:
            tokens_with_data += 1
            total_transactions += tx_count
            total_events += event_count
            status = "✅ СОБРАНО"
        else:
            status = "⏳ ОЖИДАНИЕ"
        
        token_short = f"{token[:8]}...{token[-8:]}"
        print(f"{i:2d}. {token_short} | {status} | TX: {tx_count:4d} | Events: {event_count:4d}")
    
    print("="*60)
    print(f"📈 ИТОГО:")
    print(f"   Токенов с данными: {tokens_with_data}/{len(tokens)} ({tokens_with_data/len(tokens)*100:.1f}%)")
    print(f"   Транзакций: {total_transactions:,}")
    print(f"   Обогащенных событий: {total_events:,}")
    
    if tokens_with_data == 0:
        print("\n💡 РЕКОМЕНДАЦИЯ: Сбор данных еще не начался или завис.")
        print("   Запустите: python scripts/batch_process_all_tokens.py --tokens-file data/group_b_tokens.txt")
    elif tokens_with_data < len(tokens):
        print(f"\n⚙️ СТАТУС: Сбор в процессе ({tokens_with_data}/{len(tokens)} завершено)")
        print("   Ожидайте завершения или проверьте процесс сбора")
    else:
        print(f"\n🎉 СТАТУС: Сбор завершен!")
        print("   Готово для анализа: python analysis/phase_group_b/validate_entry_pattern.py")
    
    print("="*60)
    
    conn.close()

if __name__ == "__main__":
    check_group_b_progress() 