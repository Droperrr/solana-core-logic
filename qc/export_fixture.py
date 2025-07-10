import argparse
import json
import sys
from pathlib import Path
from typing import Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

# --- Константы ---
DEFAULT_OUTPUT_DIR = "tests/fixtures/enrichment/"
PLATFORM_KEYS = [
    "raydium_enrich",
    "jupiter_enrich",
    "orca_enrich",
    # Добавьте другие платформы по мере необходимости
]

# --- Функция для подключения к БД ---
def get_db_connection():
    load_dotenv(dotenv_path='config/.env') # Загружаем переменные
    required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    if not all(os.getenv(var) for var in required_vars):
        print("ERROR: Not all database environment variables are set.", file=sys.stderr)
        print(f"Please check your config/.env file. Required: {', '.join(required_vars)}", file=sys.stderr)
        sys.exit(1)
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            cursor_factory=RealDictCursor
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"ERROR: Could not connect to the database: {e}", file=sys.stderr)
        sys.exit(1)

# --- Извлечение enrichment из БД ---
def fetch_enrichment_from_db(conn, signature: str, platform: str) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT additional_context ->> %s AS enrich_json
            FROM transactions
            WHERE signature = %s
            """,
            (platform, signature)
        )
        row = cur.fetchone()
        if not row or not row["enrich_json"]:
            return None
        try:
            return json.loads(row["enrich_json"])
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON for signature {signature}, platform {platform}: {e}", file=sys.stderr)
            return None

# --- Извлечение raw транзакции из БД ---
def fetch_raw_from_db(conn, signature: str) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM transactions WHERE signature = %s", (signature,))
        row = cur.fetchone()
        return row

# --- Сохранение в файл ---
def save_json(data: dict, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4, sort_keys=True)
    print(f"INFO: Saved {path}")

def main():
    parser = argparse.ArgumentParser(description="Export enrichment fixture(s) and raw tx from DB for QC.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--signature", type=str, help="Transaction signature to export")
    group.add_argument("--file", type=str, help="Path to file with list of signatures (one per line)")
    parser.add_argument("--platform", type=str, required=True, choices=PLATFORM_KEYS + ["all"], help="Enrichment platform key (e.g. raydium_enrich, or 'all')")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Output directory for fixtures")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    conn = get_db_connection()

    def process_signature(sig: str):
        platforms = PLATFORM_KEYS if args.platform == "all" else [args.platform]
        for platform in platforms:
            subdir = output_dir / f"{sig}_{platform}"
            subdir.mkdir(parents=True, exist_ok=True)
            # Сохраняем raw транзакцию
            raw = fetch_raw_from_db(conn, sig)
            if raw:
                save_json(raw, subdir / "raw.json")
            else:
                print(f"WARNING: No raw transaction found for {sig}")
            # Сохраняем enrichment
            enrich = fetch_enrichment_from_db(conn, sig, platform)
            if enrich:
                save_json(enrich, subdir / "enrich.json")
            else:
                print(f"WARNING: No enrichment found for {sig} and {platform}")

    if args.signature:
        process_signature(args.signature)
    else:
        # Batch mode
        try:
            from tqdm import tqdm
        except ImportError:
            tqdm = lambda x: x
        with open(args.file, "r", encoding="utf-8") as f:
            sigs = [line.strip() for line in f if line.strip()]
        success, fail = 0, 0
        for sig in tqdm(sigs):
            try:
                process_signature(sig)
                success += 1
            except Exception as e:
                print(f"ERROR: Failed to process {sig}: {e}", file=sys.stderr)
                fail += 1
        print(f"INFO: Export complete. Success: {success}, Failed: {fail}")

if __name__ == "__main__":
    main() 