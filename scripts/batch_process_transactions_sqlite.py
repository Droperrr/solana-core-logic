#!/usr/bin/env python3
"""
Скрипт-обертка для пакетной обработки транзакций из командной строки.
Вся основная логика вынесена в `processing_utils.py`.
"""
import os
import sys
import logging
import argparse

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rpc.client import RPCClient
import config.config as app_config
from scripts.processing_utils import (
    get_sqlite_connection,
    create_tables,
    load_token_addresses,
    process_token_batch
)

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/batch_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("batch_processor_cli")

def main():
    """
    Основная функция для запуска из командной строки.
    Парсит аргументы и запускает обработку пакета токенов.
    """
    parser = argparse.ArgumentParser(description="Массовая обработка транзакций токенов и сохранение в SQLite.")
    parser.add_argument('--rpc-url', type=str, default=app_config.HELIUS_API_URL, help='URL RPC-эндопойнта Solana.')
    parser.add_argument('--db-path', type=str, default=None, help='Путь к файлу базы данных SQLite.')
    parser.add_argument('--tokens-file', type=str, default='tokens.txt', help='Путь к файлу со списком адресов токенов.')
    parser.add_argument('--limit', type=int, default=1000, help='Максимальное количество транзакций для обработки на один токен.')
    parser.add_argument('--token', type=str, help='Обработать только один указанный токен.')

    args = parser.parse_args()

    logger.info("--- Запуск пакетной обработки транзакций ---")
    logger.info(f"RPC URL: {args.rpc_url}")
    logger.info(f"Файл токенов: {args.tokens_file}")
    logger.info(f"Лимит транзакций на токен: {args.limit}")

    # Инициализация RPC-клиента
    rpc_client = RPCClient(args.rpc_url)

    # Получение соединения с БД и создание таблиц
    conn = get_sqlite_connection(args.db_path)
    create_tables(conn)

    # Загрузка адресов токенов
    if args.token:
        token_addresses = [args.token]
        logger.info(f"Будет обработан один токен: {args.token}")
    else:
        token_addresses = load_token_addresses(args.tokens_file)
    
    if not token_addresses:
        logger.warning("Нет токенов для обработки. Завершение работы.")
        return

    # Запуск обработки
    try:
        process_token_batch(
            token_addresses=token_addresses,
            rpc_client=rpc_client,
            conn=conn,
            signatures_limit=args.limit
        )
    except Exception as e:
        logger.critical(f"Критическая ошибка во время выполнения process_token_batch: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
        logger.info("--- Пакетная обработка транзакций завершена ---")

if __name__ == "__main__":
    main() 