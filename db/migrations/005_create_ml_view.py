"""
Миграция: создание materialized view ml_ready_events и таблицы feature_store_daily_poc для ML/аналитики.
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

SQL = '''
-- 1. Удаляем старое представление и индекс, если они есть
DROP MATERIALIZED VIEW IF EXISTS ml_ready_events;

-- 2. Создаем новое, более отказоустойчивое представление
CREATE MATERIALIZED VIEW ml_ready_events AS
SELECT
    t.signature,
    t.block_time,
    t.slot,
    t.parser_version,
    (t.error IS NULL) AS success,

    -- Явно извлекаем поля из JSON, используя ->> для получения текста
    -- и добавляем CAST для правильных типов данных.
    -- Это более надежно, чем jsonb_to_recordset со сложным определением.
    (event ->> 'event_id')::TEXT AS event_id,
    (event ->> 'event_type')::TEXT AS event_type,
    (event ->> 'protocol')::TEXT AS protocol,
    (event ->> 'instruction_type')::TEXT AS instruction_type,
    (event ->> 'initiator')::TEXT AS initiator,
    (event ->> 'token_a_mint')::TEXT AS token_a_mint,
    (event ->> 'token_a_amount_change')::BIGINT AS token_a_amount_change,
    (event ->> 'token_b_mint')::TEXT AS token_b_mint,
    (event ->> 'token_b_amount_change')::BIGINT AS token_b_amount_change
    -- Добавьте сюда другие поля из EnrichedEvent, которые вам нужны, по аналогии
FROM
    transactions t,
    -- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Используем jsonb_array_elements вместо jsonb_to_recordset.
    -- Эта функция проще и надежнее для простого "разворачивания" массива.
    -- Также добавляем проверку, что enriched_data и enriched_events существуют и не равны NULL.
    jsonb_array_elements(
        CASE
            WHEN t.enriched_data -> 'enriched_events' IS NOT NULL
            THEN t.enriched_data -> 'enriched_events'
            ELSE '[]'::jsonb -- Если ключ отсутствует, возвращаем пустой массив
        END
    ) AS event
WHERE
    -- Дополнительная проверка, чтобы убедиться, что мы работаем только с не-NULL данными
    t.enriched_data IS NOT NULL;

-- 3. Уникальный индекс для поддержки CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS ml_ready_events_event_id_idx ON ml_ready_events (event_id);

-- 4. Таблица признаков (feature store)
CREATE TABLE IF NOT EXISTS feature_store_daily_poc (
    token_mint TEXT NOT NULL,
    day DATE NOT NULL,
    gini_coefficient NUMERIC,
    liquidity_change_velocity NUMERIC,
    external_to_internal_volume_ratio NUMERIC,
    PRIMARY KEY (token_mint, day)
);
'''

def run_migration(conn):
    with conn.cursor() as cur:
        cur.execute(SQL)
        conn.commit()

def main():
    import os
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "solana_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "Droper80moper!"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432")
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    run_migration(conn)
    logging.info("Migration 005_create_ml_view applied successfully.")

if __name__ == "__main__":
    main() 