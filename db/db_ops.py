# db/db_ops.py

import psycopg2
import logging
import json

logger = logging.getLogger("db_ops")

def upsert_discovered_pool(
    conn,
    token_mint: str,
    pool_address: str,
    dex_id: str | None,
    pool_type: str | None,
    first_seen_signature: str, # Для ON CONFLICT DO NOTHING
    last_seen_signature: str   # Для ON CONFLICT DO UPDATE
) -> bool:
    """
    Выполняет UPSERT в таблицу discovered_pools.
    Вставляет новую запись или обновляет last_seen_signature, dex_id, pool_type при конфликте.
    """
    sql = """
    INSERT INTO discovered_pools (
        token_mint_address, pool_address, dex_id, pool_type,
        first_seen_signature, last_seen_signature
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (token_mint_address, pool_address) DO UPDATE SET
        last_seen_signature = EXCLUDED.last_seen_signature,
        -- Обновляем dex_id и pool_type, только если новое значение не NULL
        dex_id = COALESCE(EXCLUDED.dex_id, discovered_pools.dex_id),
        pool_type = COALESCE(EXCLUDED.pool_type, discovered_pools.pool_type);
    """
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (
            token_mint, pool_address, dex_id, pool_type,
            first_seen_signature, last_seen_signature
        ))
        # Коммит выполняется в main.py после обработки всех пулов для одной транзакции
        return True
    except psycopg2.Error as e:
        logger.error(f"Ошибка UPSERT в discovered_pools для пула {pool_address}: {e}")
        # Откат должен быть выполнен в вызывающей функции (main.py)
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка UPSERT в discovered_pools для пула {pool_address}: {e}", exc_info=True)
        return False
    finally:
        if cursor:
            cursor.close()

def upsert_token_timeline(conn, mint: str, event_type: str, ts, details: dict):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO token_timeline (mint, event_type, ts, details)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (mint, event_type, ts)
            DO UPDATE SET details = EXCLUDED.details
        ''', (mint, event_type, ts, json.dumps(details)))

def upsert_wallet_link(
    conn,
    wallet_a: str,
    wallet_b: str,
    link_type: str,
    weight: float,
    source: str,
    first_seen,
    last_seen,
    tx_signature: str = None,
    context: dict = None,
    src_role: str = None,
    dst_role: str = None,
    confidence: float = None,
    qc_tags: list = None,
    version: int = None,
    source_event_ids: list = None,
    enrich_context: dict = None,
    ttl = None
):
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO wallet_links (
                wallet_a, wallet_b, link_type, weight, source, first_seen, last_seen,
                tx_signature, context, src_role, dst_role,
                confidence, qc_tags, version, source_event_ids, enrich_context, ttl
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (wallet_a, wallet_b, link_type, source, tx_signature)
            DO UPDATE SET
                weight = EXCLUDED.weight,
                last_seen = EXCLUDED.last_seen,
                context = EXCLUDED.context,
                src_role = EXCLUDED.src_role,
                dst_role = EXCLUDED.dst_role,
                confidence = EXCLUDED.confidence,
                qc_tags = EXCLUDED.qc_tags,
                version = EXCLUDED.version,
                source_event_ids = EXCLUDED.source_event_ids,
                enrich_context = EXCLUDED.enrich_context,
                ttl = EXCLUDED.ttl
        ''', (
            wallet_a, wallet_b, link_type, weight, source, first_seen, last_seen,
            tx_signature, json.dumps(context) if context is not None else None, src_role, dst_role,
            confidence, qc_tags, version, source_event_ids, json.dumps(enrich_context) if enrich_context is not None else None, ttl
        ))