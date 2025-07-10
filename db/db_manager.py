# db/db_manager.py

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import config.config as app_config
import time
import sys
import traceback
import logging
import os
import json
import sqlite3
from pathlib import Path
from contextlib import contextmanager

# Глобальные переменные
_engine: Engine = None
_sqlite_connection = None
_sqlite_db_path = None

# --- Функция для конвертации строк результатов запроса в словари ---
def dict_factory(cursor, row):
    """Конвертирует строки результатов запроса в словари"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# --- Инициализация SQLAlchemy engine (для обратной совместимости) ---
def initialize_engine():
    global _engine
    if _engine is not None:
        return
    
    # Формируем строку подключения к SQLite
    db_path = get_sqlite_db_path()
    db_url = f"sqlite:///{db_path}"
    
    try:
        print(f"[DB Manager] Connecting with SQLAlchemy: {db_url}")
        _engine = create_engine(db_url, future=True)
        
        # Тестовое подключение
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        print("[DB Manager] SQLAlchemy engine успешно инициализирован (SQLite).")
    except Exception as e:
        print(f"[DB Manager ERROR] Ошибка при инициализации SQLAlchemy engine: {e}")
        traceback.print_exc()
        sys.exit(1)

def get_engine():
    """Получает существующий или создает новый SQLAlchemy engine (для обратной совместимости)"""
    global _engine
    if _engine is None:
        initialize_engine()
    return _engine

def get_sqlite_db_path():
    """
    Возвращает путь к файлу базы данных SQLite.
    По умолчанию использует файл db/solana_db.sqlite в директории проекта.
    """
    global _sqlite_db_path
    
    if _sqlite_db_path is not None:
        return _sqlite_db_path
    
    # Используем стандартный путь к базе данных
    _sqlite_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')
    
    # Создаем директорию для базы данных, если её нет
    Path(os.path.dirname(_sqlite_db_path)).mkdir(parents=True, exist_ok=True)
    
    return _sqlite_db_path

# --- Основная функция для получения соединения с SQLite ---
def get_connection():
    """
    Создает и возвращает соединение с базой данных SQLite.
    
    Returns:
        Connection: Соединение с базой данных SQLite.
    """
    global _sqlite_connection
    
    # Если соединение уже существует и открыто, возвращаем его
    if _sqlite_connection is not None:
        try:
            # Проверяем, что соединение работает
            _sqlite_connection.execute("SELECT 1").fetchone()
            return _sqlite_connection
        except sqlite3.Error:
            # Если соединение не работает, создадим новое
            _sqlite_connection = None
    
    try:
        db_path = get_sqlite_db_path()
        print(f"[DB Manager] Connecting to SQLite: {db_path}")
        
        # Создаем соединение с SQLite
        conn = sqlite3.connect(db_path)
        conn.row_factory = dict_factory
        
        # Создаем таблицы, если их еще нет
        ensure_tables_exist(conn)
        
        _sqlite_connection = conn
        return conn
    except Exception as e:
        print(f"[DB Manager ERROR] Ошибка при создании соединения с SQLite: {e}")
        traceback.print_exc()
        raise

def ensure_tables_exist(conn):
    """
    Создает необходимые таблицы в базе данных SQLite, если они еще не созданы.
    
    Args:
        conn: Соединение с базой данных SQLite.
    """
    cursor = conn.cursor()
    
    # Создаем таблицу transactions
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signature TEXT NOT NULL UNIQUE,
        block_time INTEGER,
        slot INTEGER,
        fee_payer TEXT,
        transaction_type TEXT,
        source_query_type TEXT,
        source_query_address TEXT,
        raw_json TEXT,
        enriched_data TEXT,
        parser_version TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Создаем таблицу dump_operations
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dump_operations (
        id TEXT PRIMARY KEY,
        token_id TEXT NOT NULL,
        status TEXT NOT NULL,
        started_at REAL NOT NULL,
        finished_at REAL,
        progress REAL DEFAULT 0.0,
        stage TEXT,
        result TEXT,
        error_message TEXT,
        progress_percent REAL DEFAULT 0.0,
        stage_description TEXT
    )
    ''')
    
    # Создаем таблицу operation_logs
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS operation_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operation_id TEXT NOT NULL,
        timestamp REAL NOT NULL,
        level TEXT NOT NULL,
        message TEXT NOT NULL
    )
    ''')
    
    # Создаем индекс по полю signature
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_signature ON transactions(signature)')
    
    # Создаем индекс по полю block_time
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_block_time ON transactions(block_time)')
    
    # Создаем индекс по полю fee_payer
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_fee_payer ON transactions(fee_payer)')
    
    # Создаем индекс по полю source_query_address
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_source_query_address ON transactions(source_query_address)')
    
    # Фиксируем изменения
    conn.commit()

def release_connection(conn):
    """Закрывает соединение с базой данных."""
    try:
        conn.close()
    except Exception:
        pass

def close_engine():
    """Закрывает SQLAlchemy engine (для обратной совместимости)."""
    global _engine
    global _sqlite_connection
    
    if _engine is not None:
        _engine.dispose()
        _engine = None
        print("[DB Manager] SQLAlchemy engine закрыт.")
    
    if _sqlite_connection is not None:
        try:
            _sqlite_connection.close()
            _sqlite_connection = None
            print("[DB Manager] Соединение с SQLite закрыто.")
        except Exception as e:
            print(f"[DB Manager ERROR] Ошибка при закрытии соединения с SQLite: {e}")

def check_existing_signatures(engine, signature_list: list[str]) -> set[str]:
    """
    Проверяет, какие подписи транзакций уже существуют в базе данных.
    
    Args:
        engine: SQLAlchemy engine или соединение с SQLite (для обратной совместимости).
        signature_list: Список подписей транзакций для проверки.
        
    Returns:
        set[str]: Множество подписей транзакций, которые уже существуют в базе данных.
    """
    if not signature_list:
        return set()
    
    existing_signatures = set()
    
    try:
        # Определяем тип объекта engine
        if hasattr(engine, 'connect'):  # SQLAlchemy engine
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT signature FROM transactions WHERE signature IN :sigs;"),
                    {"sigs": tuple(signature_list)}
                )
                for row in result:
                    existing_signatures.add(row[0])
        else:  # SQLite connection
            cursor = engine.cursor()
            
            # SQLite не поддерживает оператор IN с большим количеством параметров,
            # поэтому разбиваем на пакеты
            batch_size = 500
            for i in range(0, len(signature_list), batch_size):
                batch = signature_list[i:i+batch_size]
                placeholders = ', '.join(['?'] * len(batch))
                query = f"SELECT signature FROM transactions WHERE signature IN ({placeholders})"
                cursor.execute(query, batch)
                for row in cursor.fetchall():
                    existing_signatures.add(row['signature'])
    except Exception as error:
        print(f"[DB Manager ERROR] Ошибка при проверке существующих сигнатур: {error}")
        traceback.print_exc()
    
    return existing_signatures

@contextmanager
def database_transaction(conn):
    """Контекстный менеджер для безопасной работы с транзакциями."""
    logger = logging.getLogger("db.transaction")
    if conn is None:
        raise ValueError("Соединение с БД не предоставлено.")
    try:
        logger.debug("BEGIN: Начало транзакции БД")
        yield conn
        logger.debug("COMMIT: Транзакция завершена успешно")
    except Exception as e:
        logger.warning(f"ROLLBACK: Исключение внутри блока транзакции: {e}. Выполняется откат.")
        try:
            conn.rollback()
            logger.info("Транзакция успешно отменена (ROLLBACK).")
        except Exception as rb_err:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при попытке отката транзакции: {rb_err}")
        raise

# --- Блок тестирования модуля ---
if __name__ == "__main__":
    print("--- Тестирование DB Manager (SQLite) ---")
    
    # Получаем соединение с SQLite
    conn = get_connection()
    
    try:
        cursor = conn.cursor()
        
        # Выполняем тестовый запрос
        print("Выполнение тестового запроса...")
        cursor.execute("SELECT sqlite_version()")
        version = cursor.fetchone()
        print(f"Версия SQLite: {version['sqlite_version()']}")
        
        # Тест вставки тестовой записи
        print("Вставка тестовой записи...")
        test_signature = 'test_signature_' + str(int(time.time()))
        test_data = {
            'signature': test_signature,
            'block_time': int(time.time()),
            'slot': 100000000,
            'fee_payer': 'test_fee_payer',
            'transaction_type': 'test_type',
            'source_query_type': 'test',
            'source_query_address': 'test_address',
            'raw_json': json.dumps({'test': 'data'}),
            'enriched_data': json.dumps({'test': 'enriched_data'}),
            'parser_version': '1.0.0'
        }
        
        try:
            cursor.execute('''
            INSERT INTO transactions 
            (signature, block_time, slot, fee_payer, transaction_type, source_query_type, source_query_address, raw_json, enriched_data, parser_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                test_data['signature'],
                test_data['block_time'],
                test_data['slot'],
                test_data['fee_payer'],
                test_data['transaction_type'],
                test_data['source_query_type'],
                test_data['source_query_address'],
                test_data['raw_json'],
                test_data['enriched_data'],
                test_data['parser_version']
            ))
            conn.commit()
            print("Тестовая запись успешно вставлена.")
        except sqlite3.IntegrityError:
            print("Запись с такой подписью уже существует.")
            conn.rollback()
        
        # Тест check_existing_signatures
        print("Тестирование check_existing_signatures...")
        test_sigs = ['sig_non_existent_1', test_signature, 'sig_non_existent_2']
        existing = check_existing_signatures(conn, test_sigs)
        print(f"  Тестовые сигнатуры: {test_sigs}")
        print(f"  Найденные существующие сигнатуры: {existing}")
        if test_signature in existing:
            print("  Проверка наличия сигнатуры прошла успешно.")
        else:
            print("  Ошибка: тестовая сигнатура не найдена.")
        
        # Подсчет количества записей
        cursor.execute("SELECT COUNT(*) as count FROM transactions")
        count = cursor.fetchone()['count']
        print(f"Всего записей в таблице transactions: {count}")
        
    except Exception as e:
        print(f"[DB Manager ERROR] Ошибка при тестировании: {e}")
        traceback.print_exc()
    finally:
        # Закрываем соединение
        release_connection(conn)