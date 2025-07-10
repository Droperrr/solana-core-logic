#!/usr/bin/env python3
"""
Скрипт для создания и обновления материализованных представлений (ML-витрин) на основе данных о транзакциях.
"""
import os
import sys
import time
import logging
import argparse
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
from psycopg2 import sql
import config.config as app_config

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/refresh_views.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ml_views")

# SQL для создания материализованного представления ml_ready_events
CREATE_ML_READY_EVENTS_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS ml_ready_events AS
SELECT 
    t.signature,
    t.block_time,
    (t.enriched_data->>'token_a_mint')::text as token_a_mint,
    (t.enriched_data->>'token_b_mint')::text as token_b_mint,
    (t.enriched_data->>'event_type')::text as event_type,
    (t.enriched_data->>'from_amount')::numeric as from_amount,
    (t.enriched_data->>'to_amount')::numeric as to_amount,
    (t.enriched_data->>'from_wallet')::text as from_wallet,
    (t.enriched_data->>'to_wallet')::text as to_wallet,
    (t.enriched_data->>'platform')::text as platform,
    JSONB_ARRAY_ELEMENTS_TEXT(CASE 
        WHEN JSONB_TYPEOF(t.enriched_data->'wallet_tags') = 'array' 
        THEN t.enriched_data->'wallet_tags' 
        ELSE '[]'::jsonb 
    END) as wallet_tag,
    t.parser_version,
    t.rpc_source,
    t.enriched_data
FROM 
    transactions t
WHERE 
    t.enriched_data IS NOT NULL
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_ml_ready_events_token_a ON ml_ready_events(token_a_mint);
CREATE INDEX IF NOT EXISTS idx_ml_ready_events_token_b ON ml_ready_events(token_b_mint);
CREATE INDEX IF NOT EXISTS idx_ml_ready_events_event_type ON ml_ready_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ml_ready_events_wallet_tag ON ml_ready_events(wallet_tag);
CREATE INDEX IF NOT EXISTS idx_ml_ready_events_block_time ON ml_ready_events(block_time);
"""

# SQL для обновления материализованного представления ml_ready_events
REFRESH_ML_READY_EVENTS_SQL = """
REFRESH MATERIALIZED VIEW ml_ready_events;
"""

def get_db_connection():
    """
    Создает и возвращает соединение с базой данных.
    
    Returns:
        Соединение с базой данных
    """
    try:
        conn = psycopg2.connect(
            host=app_config.DB_HOST,
            port=app_config.DB_PORT,
            dbname=app_config.DB_NAME,
            user=app_config.DB_USER,
            password=app_config.DB_PASSWORD
        )
        logger.info("Успешное подключение к базе данных")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {str(e)}")
        raise

def create_materialized_views(conn):
    """
    Создает материализованные представления в базе данных.
    
    Args:
        conn: Соединение с базой данных
    """
    try:
        logger.info("Создание материализованного представления ml_ready_events")
        with conn.cursor() as cur:
            cur.execute(CREATE_ML_READY_EVENTS_SQL)
        conn.commit()
        logger.info("Материализованное представление ml_ready_events успешно создано")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при создании материализованного представления: {str(e)}")
        raise

def refresh_materialized_views(conn):
    """
    Обновляет материализованные представления в базе данных.
    
    Args:
        conn: Соединение с базой данных
    """
    try:
        logger.info("Обновление материализованного представления ml_ready_events")
        start_time = time.time()
        with conn.cursor() as cur:
            cur.execute(REFRESH_ML_READY_EVENTS_SQL)
        conn.commit()
        duration = time.time() - start_time
        logger.info(f"Материализованное представление ml_ready_events успешно обновлено за {duration:.2f} сек.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при обновлении материализованного представления: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Управление материализованными представлениями для ML")
    parser.add_argument("--create", "-c", action="store_true", 
                      help="Создать материализованные представления, если они не существуют")
    parser.add_argument("--refresh", "-r", action="store_true", 
                      help="Обновить материализованные представления")
    args = parser.parse_args()
    
    if not args.create and not args.refresh:
        logger.info("Не указаны действия. Укажите --create для создания или --refresh для обновления.")
        return
    
    try:
        conn = get_db_connection()
        
        if args.create:
            create_materialized_views(conn)
        
        if args.refresh:
            refresh_materialized_views(conn)
            
        conn.close()
        logger.info("Операции с материализованными представлениями завершены")
    except Exception as e:
        logger.error(f"Произошла ошибка при выполнении операций: {str(e)}")

if __name__ == "__main__":
    main() 