import sqlite3
import json
import sys
from typing import Optional

DB_PATH = 'db/solana_db.sqlite'

LOG_FILE = 'result_token_price_around_signature.txt'

def get_block_time(conn, signature: str) -> Optional[int]:
    cursor = conn.cursor()
    cursor.execute("SELECT block_time FROM transactions WHERE signature = ?", (signature,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_tx_by_block_time(conn, token_address: str, block_time: int, direction: str) -> Optional[dict]:
    cursor = conn.cursor()
    if direction == 'prev':
        cursor.execute("SELECT signature, raw_json FROM transactions WHERE block_time < ? ORDER BY block_time DESC LIMIT 1", (block_time,))
    elif direction == 'next':
        cursor.execute("SELECT signature, raw_json FROM transactions WHERE block_time > ? ORDER BY block_time ASC LIMIT 1", (block_time,))
    else:
        cursor.execute("SELECT signature, raw_json FROM transactions WHERE block_time = ? LIMIT 1", (block_time,))
    row = cursor.fetchone()
    if row:
        return {'signature': row[0], 'raw_json': row[1]}
    return None

def extract_price_from_tx(raw_json: str, token_address: str, debug_lines: list) -> Optional[float]:
    try:
        tx = json.loads(raw_json)
        enriched = tx.get('enriched_data') or tx.get('enriched')
        if not enriched:
            debug_lines.append('  [debug] enriched_data отсутствует')
            return None
        if isinstance(enriched, str):
            try:
                enriched = json.loads(enriched)
            except Exception as e:
                debug_lines.append(f'  [debug] enriched_data не парсится: {e}')
                return None
        if isinstance(enriched, list):
            found_swap = False
            for event in enriched:
                if event.get('event_type') == 'SWAP' and (event.get('token_a_mint') == token_address or event.get('token_b_mint') == token_address):
                    found_swap = True
                    price = event.get('price')
                    if price:
                        return price
                    else:
                        debug_lines.append('  [debug] SWAP-событие найдено, но поле price отсутствует')
            if not found_swap:
                debug_lines.append('  [debug] Нет SWAP-события для этого токена')
        else:
            debug_lines.append('  [debug] enriched_data не является списком')
        return None
    except Exception as e:
        debug_lines.append(f"  [error] Не удалось извлечь цену: {e}")
        return None

def main(signature: str, token_address: str):
    conn = sqlite3.connect(DB_PATH)
    block_time = get_block_time(conn, signature)
    output_lines = []
    if not block_time:
        output_lines.append(f"[error] Не найден block_time для сигнатуры {signature}")
    else:
        output_lines.append(f"Текущая транзакция: {signature} (block_time={block_time})")
        for label, direction in [('Предыдущая', 'prev'), ('Текущая', 'cur'), ('Следующая', 'next')]:
            debug_lines = []
            if direction == 'cur':
                tx = get_tx_by_block_time(conn, token_address, block_time, None)
            else:
                tx = get_tx_by_block_time(conn, token_address, block_time, direction)
            if not tx:
                output_lines.append(f"{label}: транзакция не найдена")
                continue
            price = extract_price_from_tx(tx['raw_json'], token_address, debug_lines)
            output_lines.append(f"{label}: signature={tx['signature']} | price={price}")
            if debug_lines:
                output_lines.extend([f"    {line}" for line in debug_lines])
    conn.close()
    print('\n'.join(output_lines))
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))
    print(f"[info] Подробный результат сохранён в {LOG_FILE}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python show_token_price_around_signature.py <signature> <token_address>")
    else:
        main(sys.argv[1], sys.argv[2]) 