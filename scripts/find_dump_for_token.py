import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
import logging
from services.onchain_price_engine import OnChainPriceEngine
import sqlite3

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find first price dump for a token.")
    parser.add_argument("--token_address", required=True, help="Token mint address")
    args = parser.parse_args()

    # Настраиваем логирование: INFO и выше, вывод в консоль
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    logger = logging.getLogger("find_dump_for_token")

    engine = OnChainPriceEngine()
    # Переопределяем find_first_dump для подробного логирования
    def find_first_dump_verbose(self, token_address: str, dump_threshold: float = 0.5):
        import pandas as pd
        from analysis.data_provider import get_all_events_for_token
        logger.info(f"Начало анализа для токена {token_address}")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT first_dump_signature FROM token_lifecycle WHERE token_address = ?", (token_address,))
        row = cursor.fetchone()
        if row and row[0]:
            logger.info(f"Дамп уже найден ранее для токена {token_address}: {row[0]}")
            conn.close()
            return None
        try:
            df = get_all_events_for_token(token_address)
        except Exception as e:
            logger.error(f"Ошибка при получении событий: {e}")
            conn.close()
            return None
        if df.empty:
            logger.warning(f"Нет событий для токена {token_address}")
            conn.close()
            return None
        swaps = df[df['event_type'] == 'SWAP'].sort_values('block_time')
        logger.info(f"Найдено {len(swaps)} SWAP-событий для токена {token_address}")
        previous_price = None
        previous_signature = None
        previous_block_time = None
        for idx, row in enumerate(swaps.itertuples(), 1):
            signature = row.signature
            block_time = int(row.block_time.timestamp()) if hasattr(row.block_time, 'timestamp') else int(row.block_time)
            try:
                import json
                raw_tx = None
                if hasattr(row, 'event_data_raw') and row.event_data_raw:
                    try:
                        raw_tx = json.loads(row.event_data_raw) if isinstance(row.event_data_raw, str) else row.event_data_raw
                    except Exception as e:
                        logger.warning(f"Не удалось распарсить event_data_raw для {signature}: {e}")
                if raw_tx is None:
                    cur2 = conn.cursor()
                    cur2.execute("SELECT raw_json FROM transactions WHERE signature = ?", (signature,))
                    tx_row = cur2.fetchone()
                    if tx_row and tx_row[0]:
                        raw_tx = json.loads(tx_row[0]) if isinstance(tx_row[0], str) else tx_row[0]
                if raw_tx is None:
                    logger.warning(f"Не удалось получить raw_tx для {signature}, пропуск")
                    continue
                price = self.calculate_price_from_swap(row._asdict(), raw_tx)
                if price is None:
                    logger.warning(f"Не удалось рассчитать цену для транзакции {signature}")
                    continue
            except Exception as e:
                logger.warning(f"Ошибка при обработке транзакции {signature}: {e}")
                continue
            # Логируем цену и изменение
            if previous_price is not None:
                price_change = 100 * (price - previous_price) / previous_price if previous_price != 0 else 0
                change_str = f"{price_change:+.2f}%"
            else:
                change_str = "N/A"
            logger.info(f"[TX {idx}/{len(swaps)}] sig: {signature[:6]}... Price: {price:.8f}. Change: {change_str}")
            if previous_price is not None and self.is_price_dump(previous_price, price, threshold=dump_threshold):
                price_drop_percent = round(100 * (1 - price / previous_price), 2)
                logger.warning(f"!!! DUMP DETECTED !!! sig: {signature} Price: {price:.8f}. Change: -{price_drop_percent}%")
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO token_lifecycle (
                            token_address, first_dump_signature, first_dump_time, first_dump_price_drop_percent, last_processed_signature
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        token_address,
                        signature,
                        block_time,
                        price_drop_percent,
                        signature
                    ))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Ошибка при записи в token_lifecycle: {e}")
                result = {
                    'signature': signature,
                    'block_time': block_time,
                    'price_drop_percent': price_drop_percent,
                    'previous_price': previous_price,
                    'dump_price': price
                }
                conn.close()
                return result
            previous_price = price
            previous_signature = signature
            previous_block_time = block_time
        logger.info(f"Дамп не найден для токена {token_address}")
        conn.close()
        return None
    # Подменяем метод на verbose-версию
    engine.find_first_dump = find_first_dump_verbose.__get__(engine)
    result = engine.find_first_dump(args.token_address)
    if result:
        print("\n=== FIRST DUMP DETECTED ===")
        print(f"Signature:        {result['signature']}")
        print(f"Block time:       {result['block_time']}")
        print(f"Price drop:       {result['price_drop_percent']}%")
        print(f"Previous price:   {result['previous_price']}")
        print(f"Dump price:       {result['dump_price']}")
    else:
        print("\nNo dump detected for this token (or already present in token_lifecycle). See logs for details.") 