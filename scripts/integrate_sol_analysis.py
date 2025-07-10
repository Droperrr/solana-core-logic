#!/usr/bin/env python3
"""
Скрипт интеграции SOL Trading Analysis в Feature Store
Обновляет build_feature_store_sqlite.py для включения SOL-признаков
"""

import json
import sqlite3
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional
import logging

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_sol_trading_features(enriched_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Извлекает SOL trading признаки из enriched_data
    
    Args:
        enriched_data: Обогащенные данные транзакции
    
    Returns:
        Словарь с SOL trading признаками
    """
    sol_features = {
        'largest_sol_buy_amount': 0.0,
        'largest_sol_sell_amount': 0.0,
        'total_sol_buy_amount': 0.0,
        'total_sol_sell_amount': 0.0,
        'sol_buy_count': 0,
        'sol_sell_count': 0,
        'net_sol_change': 0.0
    }
    
    try:
        # Проверяем наличие SOL trades в enriched_data
        if isinstance(enriched_data, str):
            enriched_data = json.loads(enriched_data)
        
        sol_trades = enriched_data.get('sol_trades', {})
        if not sol_trades:
            return sol_features
        
        # Извлекаем признаки
        sol_features.update({
            'largest_sol_buy_amount': float(sol_trades.get('largest_sol_buy_amount', 0)),
            'largest_sol_sell_amount': float(sol_trades.get('largest_sol_sell_amount', 0)),
            'total_sol_buy_amount': float(sol_trades.get('total_sol_buy_amount', 0)),
            'total_sol_sell_amount': float(sol_trades.get('total_sol_sell_amount', 0)),
            'sol_buy_count': int(sol_trades.get('sol_buy_count', 0)),
            'sol_sell_count': int(sol_trades.get('sol_sell_count', 0)),
            'net_sol_change': float(sol_trades.get('net_sol_change', 0))
        })
        
        logger.debug(f"Извлечены SOL features: {sol_features}")
        
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.warning(f"Ошибка извлечения SOL features: {e}")
    
    return sol_features

def test_sol_feature_extraction():
    """
    Тестирует извлечение SOL признаков на реальных данных из БД
    """
    logger.info("🧪 Тестируем извлечение SOL-признаков...")
    
    try:
        conn = sqlite3.connect('db/solana_db.sqlite')
        cursor = conn.cursor()
        
        # Получаем несколько транзакций с enriched_data
        cursor.execute("""
            SELECT signature, enriched_data 
            FROM transactions 
            WHERE enriched_data IS NOT NULL 
            AND enriched_data != ''
            LIMIT 5
        """)
        
        results = cursor.fetchall()
        
        if not results:
            logger.warning("Нет транзакций с enriched_data для тестирования")
            return False
        
        logger.info(f"Найдено {len(results)} транзакций для тестирования")
        
        successful_extractions = 0
        for signature, enriched_data_str in results:
            try:
                sol_features = extract_sol_trading_features(enriched_data_str)
                
                # Проверяем, извлечены ли хоть какие-то SOL данные
                has_sol_data = any(v != 0 for v in sol_features.values() if isinstance(v, (int, float)))
                
                logger.info(f"Транзакция {signature[:16]}...")
                logger.info(f"  SOL features: {sol_features}")
                logger.info(f"  Есть SOL данные: {has_sol_data}")
                
                if has_sol_data:
                    successful_extractions += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при тестировании транзакции {signature[:16]}...: {e}")
        
        conn.close()
        
        success_rate = successful_extractions / len(results) * 100
        logger.info(f"""
🎯 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:
✅ Успешных извлечений: {successful_extractions}/{len(results)}
📊 Процент успеха: {success_rate:.1f}%
        """)
        
        return successful_extractions > 0
        
    except Exception as e:
        logger.error(f"Ошибка тестирования: {e}")
        return False

def update_build_feature_store_script():
    """
    Обновляет scripts/build_feature_store_sqlite.py для включения SOL-признаков
    """
    script_path = Path('scripts/build_feature_store_sqlite.py')
    
    if not script_path.exists():
        logger.error(f"Файл {script_path} не найден")
        return False
    
    try:
        # Читаем текущий содержимое
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Проверяем, уже ли добавлена поддержка SOL
        if 'sol_trading_features' in content or 'largest_sol_buy_amount' in content:
            logger.info("SOL-признаки уже интегрированы в build_feature_store_sqlite.py")
            return True
        
        # Создаем резервную копию
        backup_path = script_path.with_suffix('.py.backup')
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"Создана резервная копия: {backup_path}")
        
        # Добавляем импорт функции извлечения SOL-признаков
        import_addition = """
from scripts.integrate_sol_analysis import extract_sol_trading_features
"""
        
        # Находим место для добавления SOL-признаков в CREATE TABLE
        create_table_addition = """
            largest_sol_buy_amount REAL DEFAULT 0,
            largest_sol_sell_amount REAL DEFAULT 0,
            total_sol_buy_amount REAL DEFAULT 0,
            total_sol_sell_amount REAL DEFAULT 0,
            sol_buy_count INTEGER DEFAULT 0,
            sol_sell_count INTEGER DEFAULT 0,
            net_sol_change REAL DEFAULT 0,"""
        
        # Добавляем обработку SOL-признаков в INSERT запрос
        insert_addition = """
        # Извлекаем SOL trading признаки
        sol_features = extract_sol_trading_features(enriched_data)"""
        
        # Модифицируем содержимое (простая версия - добавляем в конец)
        modified_content = content + f"""

# === ИНТЕГРАЦИЯ SOL TRADING ANALYSIS ===
{import_addition}

def extract_sol_features_for_feature_store():
    \"\"\"
    Обновленная версия для интеграции SOL-признаков
    \"\"\"
    pass

# Эта функция будет интегрирована в основной build_feature_store_sqlite.py
"""
        
        # Записываем модифицированный файл
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(modified_content)
        
        logger.info("✅ SOL-признаки добавлены в build_feature_store_sqlite.py")
        logger.info("⚠️ ВНИМАНИЕ: Требуется ручная доработка SQL-запросов в файле")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка обновления build_feature_store_sqlite.py: {e}")
        return False

def create_enhanced_feature_store_script():
    """
    Создает улучшенную версию build_feature_store_sqlite.py с поддержкой SOL
    """
    script_path = Path('scripts/build_feature_store_sqlite_enhanced.py')
    
    script_content = '''#!/usr/bin/env python3
"""
Улучшенная версия build_feature_store_sqlite.py с поддержкой SOL Trading Analysis
"""

import sqlite3
import json
import sys
import os
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Any, List, Optional

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.integrate_sol_analysis import extract_sol_trading_features

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_enhanced_feature_store_table(conn):
    """Создает таблицу feature_store_daily с поддержкой SOL-признаков"""
    
    cursor = conn.cursor()
    
    # Удаляем старую таблицу если нужно пересоздать
    # cursor.execute("DROP TABLE IF EXISTS feature_store_daily")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS feature_store_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            wallet_address TEXT NOT NULL,
            token_address TEXT,
            
            -- Базовые метрики
            total_transactions INTEGER DEFAULT 0,
            total_volume REAL DEFAULT 0,
            total_fees REAL DEFAULT 0,
            
            -- SWAP метрики  
            swap_count INTEGER DEFAULT 0,
            buy_volume REAL DEFAULT 0,
            sell_volume REAL DEFAULT 0,
            net_volume REAL DEFAULT 0,
            
            -- SOL Trading Analysis (НОВЫЕ ПОЛЯ)
            largest_sol_buy_amount REAL DEFAULT 0,
            largest_sol_sell_amount REAL DEFAULT 0,
            total_sol_buy_amount REAL DEFAULT 0,
            total_sol_sell_amount REAL DEFAULT 0,
            sol_buy_count INTEGER DEFAULT 0,
            sol_sell_count INTEGER DEFAULT 0,
            net_sol_change REAL DEFAULT 0,
            
            -- Метаданные
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(date, wallet_address, token_address)
        )
    """)
    
    # Создаем индексы
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_date ON feature_store_daily(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_wallet ON feature_store_daily(wallet_address)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_token ON feature_store_daily(token_address)")
    
    conn.commit()
    logger.info("✅ Таблица feature_store_daily с SOL-поддержкой создана")

def extract_wallet_from_enriched_data(enriched_data: Dict[str, Any]) -> Optional[str]:
    """Извлекает адрес кошелька из enriched_data"""
    try:
        if isinstance(enriched_data, str):
            enriched_data = json.loads(enriched_data)
        
        # Ищем в различных местах
        for event in enriched_data.get('events', []):
            if event.get('wallet_address'):
                return event['wallet_address']
        
        return None
    except:
        return None

def build_enhanced_feature_store():
    """Строит feature store с SOL-признаками"""
    
    logger.info("🚀 Начинаем построение enhanced feature store...")
    
    conn = sqlite3.connect('db/solana_db.sqlite')
    
    try:
        # Создаем таблицу
        create_enhanced_feature_store_table(conn)
        
        # Получаем все транзакции с enriched_data
        cursor = conn.cursor()
        cursor.execute("""
            SELECT signature, block_time, enriched_data, source_query_address
            FROM transactions 
            WHERE enriched_data IS NOT NULL 
            AND enriched_data != ''
            ORDER BY block_time
        """)
        
        transactions = cursor.fetchall()
        logger.info(f"Найдено {len(transactions)} транзакций для обработки")
        
        processed_count = 0
        sol_features_count = 0
        
        for signature, block_time, enriched_data_str, source_query_address in transactions:
            try:
                if not block_time:
                    continue
                
                # Преобразуем block_time в дату
                transaction_date = datetime.fromtimestamp(block_time).date()
                
                # Парсим enriched_data
                try:
                    enriched_data = json.loads(enriched_data_str)
                except json.JSONDecodeError:
                    continue
                
                # Извлекаем SOL trading признаки
                sol_features = extract_sol_trading_features(enriched_data)
                
                # Проверяем, есть ли SOL данные
                has_sol_data = any(v != 0 for v in sol_features.values() if isinstance(v, (int, float)))
                if has_sol_data:
                    sol_features_count += 1
                
                # Определяем кошелек
                wallet_address = extract_wallet_from_enriched_data(enriched_data) or source_query_address
                
                if not wallet_address:
                    continue
                
                # Определяем токен (можем использовать source_query_address если это токен)
                token_address = source_query_address if source_query_address else None
                
                # UPSERT в feature_store_daily
                cursor.execute("""
                    INSERT INTO feature_store_daily (
                        date, wallet_address, token_address,
                        total_transactions,
                        largest_sol_buy_amount, largest_sol_sell_amount,
                        total_sol_buy_amount, total_sol_sell_amount,
                        sol_buy_count, sol_sell_count, net_sol_change,
                        updated_at
                    ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(date, wallet_address, token_address) DO UPDATE SET
                        total_transactions = total_transactions + 1,
                        largest_sol_buy_amount = MAX(largest_sol_buy_amount, excluded.largest_sol_buy_amount),
                        largest_sol_sell_amount = MAX(largest_sol_sell_amount, excluded.largest_sol_sell_amount),
                        total_sol_buy_amount = total_sol_buy_amount + excluded.total_sol_buy_amount,
                        total_sol_sell_amount = total_sol_sell_amount + excluded.total_sol_sell_amount,
                        sol_buy_count = sol_buy_count + excluded.sol_buy_count,
                        sol_sell_count = sol_sell_count + excluded.sol_sell_count,
                        net_sol_change = net_sol_change + excluded.net_sol_change,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    transaction_date,
                    wallet_address,
                    token_address,
                    sol_features['largest_sol_buy_amount'],
                    sol_features['largest_sol_sell_amount'],
                    sol_features['total_sol_buy_amount'],
                    sol_features['total_sol_sell_amount'],
                    sol_features['sol_buy_count'],
                    sol_features['sol_sell_count'],
                    sol_features['net_sol_change']
                ))
                
                processed_count += 1
                
                if processed_count % 100 == 0:
                    conn.commit()
                    logger.info(f"Обработано {processed_count}/{len(transactions)} транзакций")
                
            except Exception as e:
                logger.error(f"Ошибка обработки транзакции {signature}: {e}")
                continue
        
        conn.commit()
        
        # Статистика
        cursor.execute("SELECT COUNT(*) FROM feature_store_daily")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM feature_store_daily WHERE sol_buy_count > 0 OR sol_sell_count > 0")
        sol_records = cursor.fetchone()[0]
        
        logger.info(f"""
🎉 ПОСТРОЕНИЕ FEATURE STORE ЗАВЕРШЕНО:
📊 Обработано транзакций: {processed_count}
📈 Транзакций с SOL данными: {sol_features_count}
🗂️ Записей в feature_store_daily: {total_records}
💰 Записей с SOL активностью: {sol_records}
        """)
        
    finally:
        conn.close()

if __name__ == "__main__":
    build_enhanced_feature_store()
'''
    
    try:
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        logger.info(f"✅ Создан enhanced feature store script: {script_path}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка создания enhanced script: {e}")
        return False

def main():
    """Главная функция интеграции"""
    
    logger.info("🎯 ИНТЕГРАЦИЯ SOL TRADING ANALYSIS В FEATURE STORE")
    logger.info("=" * 60)
    
    # Шаг 1: Тестируем извлечение SOL-признаков
    logger.info("Шаг 1: Тестирование извлечения SOL-признаков...")
    if not test_sol_feature_extraction():
        logger.error("❌ Тестирование не прошло. Проверьте наличие SOL данных в enriched_data")
        return False
    
    # Шаг 2: Создаем enhanced версию feature store
    logger.info("\\nШаг 2: Создание enhanced feature store script...")
    if not create_enhanced_feature_store_script():
        logger.error("❌ Не удалось создать enhanced script")
        return False
    
    logger.info("""
🎉 ИНТЕГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!

Следующие шаги:
1. Запустите: python scripts/build_feature_store_sqlite_enhanced.py
2. Проверьте результаты в таблице feature_store_daily
3. Убедитесь, что SOL-признаки заполнены корректно

📁 Созданные файлы:
- scripts/build_feature_store_sqlite_enhanced.py (новый enhanced script)
    """)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 