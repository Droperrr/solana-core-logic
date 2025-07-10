import argparse
import json
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Импортируем нужные функции
from parser.transaction_parser_core import parse_transaction_instructions
from parser.universal_parser import UniversalParser
from db import db_manager
from rpc.client import RPCClient

# TODO: Для --signature теперь реализован поиск в БД и fallback на RPC

def load_tx_from_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_raw_json_from_db(signature):
    engine = db_manager.get_engine()
    with engine.connect() as conn:
        result = conn.execute(db_manager.text("SELECT raw_json FROM transactions WHERE signature = :sig"), {"sig": signature})
        row = result.fetchone()
        if row:
            return row[0] if isinstance(row[0], dict) else json.loads(row[0])
        return None

def get_raw_json_from_rpc(signature):
    client = RPCClient()
    tx = client.get_transaction(signature)
    if tx and tx.raw_json:
        return tx.raw_json
    return None

def main():
    parser = argparse.ArgumentParser(description="Diagnose Solana transaction pipeline by layer.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file', type=str, help='Path to raw.json file')
    group.add_argument('--signature', type=str, help='Transaction signature (fetch from DB, fallback to RPC)')
    parser.add_argument('--layer', type=str, choices=['parser', 'universal'], required=True, help='Pipeline layer to diagnose')
    args = parser.parse_args()

    # Загрузка транзакции
    if args.file:
        tx = load_tx_from_file(args.file)
    elif args.signature:
        tx = get_raw_json_from_db(args.signature)
        if tx is not None:
            print(f"[INFO] Транзакция {args.signature} найдена в локальной базе данных.")
        else:
            print(f"[WARN] Транзакция {args.signature} не найдена в локальной базе. Пробуем RPC...")
            tx = get_raw_json_from_rpc(args.signature)
            if tx is not None:
                print(f"[INFO] Транзакция {args.signature} успешно получена через RPC.")
            else:
                print(f"[ERROR] Транзакция {args.signature} не найдена ни в базе, ни через RPC.", file=sys.stderr)
                sys.exit(1)
    else:
        print("[ERROR] Не указан источник транзакции.", file=sys.stderr)
        sys.exit(1)

    # Диагностика слоя
    if args.layer == 'parser':
        print("[INFO] Layer: parser (parse_transaction_instructions)")
        try:
            parsed = parse_transaction_instructions(tx)
            print(json.dumps(parsed, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"[ERROR] Parser failed: {e}", file=sys.stderr)
            sys.exit(2)
    elif args.layer == 'universal':
        print("[INFO] Layer: universal (UniversalParser.parse_raw_transaction)")
        try:
            parser = UniversalParser()
            result = parser.parse_raw_transaction(tx)
            print(json.dumps(result, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"[ERROR] UniversalParser failed: {e}", file=sys.stderr)
            sys.exit(2)
    else:
        print(f"[ERROR] Unknown layer: {args.layer}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 