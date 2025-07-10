#!/usr/bin/env python3
"""
Скрипт для загрузки транзакций для токенов из файла tokens.txt и сохранения их напрямую в базу данных SQLite.
"""
import os
import sys
import json
import time
import logging
import argparse
import sqlite3
from typing import List, Dict, Any

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rpc.client import RPCClient
from db.db_writer import upsert_transaction_sqlite
from db.db_manager import get_connection

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/download_transactions.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("transaction_downloader")

def load_tokens_from_file(file_path: str) -> List[str]:
    """
    Загружает адреса токенов из текстового файла.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tokens = [line.strip() for line in f if line.strip()]
        return tokens
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла токенов {file_path}: {str(e)}")
        return []

def download_and_save_transactions_for_token(
    client: RPCClient,
    conn: sqlite3.Connection,
    token_address: str,
    limit: int = 100
):
    """
    Загружает транзакции для указанного токена и сохраняет их в базу данных.
    """
    try:
        logger.info(f"Загрузка сигнатур транзакций для токена: {token_address}")
        signatures_data = client.get_signatures_for_address(token_address, limit=limit)

        if not signatures_data:
            logger.warning(f"Не удалось получить сигнатуры для токена {token_address}")
            return

        logger.info(f"Получено {len(signatures_data)} сигнатур для токена {token_address}")

        for idx, sig_data in enumerate(signatures_data):
            signature = sig_data.get("signature")
            if not signature:
                logger.warning(f"Сигнатура не найдена в данных: {sig_data}")
                continue

            logger.info(f"[{idx+1}/{len(signatures_data)}] Загрузка и сохранение транзакции {signature}")
            tx_data = client.get_transaction(signature)

            if not tx_data:
                logger.warning(f"Не удалось получить данные для транзакции {signature}")
                continue

            # Извлекаем основные данные для записи в БД
            block_time = tx_data.get('blockTime')
            slot = tx_data.get('slot')
            
            # ИСПРАВЛЕНИЕ: accountKeys - это список строк, а не словарей.
            # fee_payer - это просто первый элемент списка.
            account_keys = tx_data.get('transaction', {}).get('message', {}).get('accountKeys', [])
            fee_payer = account_keys[0] if account_keys else None

            upsert_transaction_sqlite(
                conn=conn,
                signature=signature,
                block_time=block_time,
                slot=slot,
                fee_payer=fee_payer,
                transaction_type=None,  # Будет определен на этапе обогащения
                raw_json=tx_data,
                enriched_data={},  # Обогащение происходит позже
                source_query_type='token',
                source_query_address=token_address,
                parser_version="downloader-v1.0"
            )

            time.sleep(0.1)  # Задержка между запросами

        logger.info(f"Обработка токена {token_address} завершена.")

    except Exception as e:
        logger.error(f"Ошибка при загрузке транзакций для токена {token_address}: {str(e)}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Загрузка транзакций для токенов напрямую в БД")
    parser.add_argument("--tokens", "-t", type=str, default="tokens.txt",
                      help="Путь к файлу с токенами (по умолчанию: tokens.txt)")
    parser.add_argument("--limit", "-l", type=int, default=1000,
                      help="Количество транзакций для загрузки на каждый токен")
    args = parser.parse_args()

    tokens = load_tokens_from_file(args.tokens)
    if not tokens:
        logger.error(f"Не найдены токены в файле {args.tokens}")
        return

    logger.info(f"Загружено {len(tokens)} токенов из файла {args.tokens}")

    client = RPCClient()
    conn = get_connection()  # Получаем одно соединение на весь сеанс

    try:
        for token in tokens:
            logger.info(f"Начинаем обработку токена: {token}")
            download_and_save_transactions_for_token(client, conn, token, limit=args.limit)
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с базой данных закрыто.")

    logger.info(f"Загрузка завершена. Обработаны данные для {len(tokens)} токенов.")

if __name__ == "__main__":
    main() 