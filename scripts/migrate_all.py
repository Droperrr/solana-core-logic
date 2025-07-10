#!/usr/bin/env python3
"""
Скрипт для полной миграции всех данных из файловой системы в БД с подробным логированием
"""
import os
import sys
import subprocess
import time
import logging
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Настройка логирования
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/full_migration.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("full_migration")

def run_check_before():
    """
    Запускает проверку количества транзакций в БД перед миграцией
    """
    logger.info("Проверка количества транзакций в БД перед миграцией...")
    process = subprocess.run([sys.executable, "scripts/check_transaction_count.py"], 
                            capture_output=True, text=True)
    logger.info(f"Результат проверки перед миграцией:\n{process.stdout}")
    return process.stdout

def run_migration(dry_run=False, limit=None):
    """
    Запускает скрипт миграции
    """
    command = [sys.executable, "scripts/migrate_files_to_db.py"]
    
    if dry_run:
        command.append("--dry-run")
    
    if limit:
        command.extend(["--limit", str(limit)])
    
    logger.info(f"Запуск миграции с командой: {' '.join(command)}")
    process = subprocess.run(command, capture_output=True, text=True)
    
    if process.returncode != 0:
        logger.error(f"Ошибка при выполнении миграции:\n{process.stderr}")
    else:
        logger.info(f"Результат миграции:\n{process.stdout}")
    
    return process.returncode == 0

def run_check_after():
    """
    Запускает проверку количества транзакций в БД после миграции
    """
    logger.info("Проверка количества транзакций в БД после миграции...")
    process = subprocess.run([sys.executable, "scripts/check_transaction_count.py"], 
                            capture_output=True, text=True)
    logger.info(f"Результат проверки после миграции:\n{process.stdout}")
    return process.stdout

def full_migration_process(dry_run=False, limit=None):
    """
    Выполняет полный процесс миграции с проверками до и после
    """
    start_time = time.time()
    logger.info(f"Начало процесса миграции (dry_run={dry_run}, limit={limit})...")
    
    # Проверка перед миграцией
    before_count = run_check_before()
    
    # Миграция данных
    success = run_migration(dry_run, limit)
    
    # Проверка после миграции
    after_count = run_check_after()
    
    # Итоги
    elapsed_time = time.time() - start_time
    logger.info(f"Процесс миграции завершен за {elapsed_time:.2f} с.")
    logger.info(f"Успешность: {success}")
    
    return success

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Полная миграция данных из файловой системы в БД")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Не выполнять реальную запись в БД")
    parser.add_argument("--limit", "-l", type=int, help="Максимальное количество транзакций для обработки")
    
    args = parser.parse_args()
    
    full_migration_process(dry_run=args.dry_run, limit=args.limit) 