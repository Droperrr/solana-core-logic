import argparse
import json
import sys
from .etalon_generator import generate_from_reparse
from .diff_engine import diff_etalon_vs_enrich

try:
    from termcolor import colored
except ImportError:
    def colored(text, color=None):
        return text

# --- Новый импорт для работы с БД ---
import os
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import db.db_manager as dbm


def main():
    parser = argparse.ArgumentParser(description="Validate enriched transaction against etalon.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--signature", type=str, help="Transaction signature to fetch from DB")
    group.add_argument("--raw", type=str, help="Path to raw transaction JSON (fixture)")
    # enrich обязателен только если выбран raw
    parser.add_argument("--enrich", type=str, help="Path to enrichment JSON (from DB or pipeline)")
    parser.add_argument("--format", type=str, choices=["json"], default=None, help="Output format: json (for machine parsing)")
    args = parser.parse_args()

    if args.signature:
        # --- Новый режим: извлечение из БД по сигнатуре ---
        conn = dbm.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT raw_json, additional_context
                    FROM transactions
                    WHERE signature = %s
                """, (args.signature,))
                row = cur.fetchone()
                if not row:
                    print(f"ERROR: Transaction with signature {args.signature} not found in the database.", file=sys.stderr)
                    sys.exit(1)
                raw_json_str = row[0]
                enrich_json_str = row[1]
                # --- BEGIN NEW ERROR HANDLING ---
                missing_fields = []
                if not raw_json_str:
                    missing_fields.append('raw_json')
                if not enrich_json_str:
                    missing_fields.append('additional_context')
                if missing_fields:
                    print(f"ERROR: Data is incomplete for signature {args.signature}. Missing required fields for validation: {', '.join(missing_fields)}.", file=sys.stderr)
                    sys.exit(1)
                # --- END NEW ERROR HANDLING ---
                try:
                    raw_json = json.loads(raw_json_str)
                    enrich_json = json.loads(enrich_json_str)
                except json.JSONDecodeError as e:
                    print(f"ERROR: Failed to decode JSON data for signature {args.signature}. Error: {e}", file=sys.stderr)
                    sys.exit(1)
        finally:
            dbm.release_connection(conn)
    else:
        if not args.raw or not args.enrich:
            print("Укажите оба файла: --raw <raw.json> --enrich <enrich.json>")
            return
        with open(args.raw, "r", encoding="utf-8") as f:
            raw_json = json.load(f)
        with open(args.enrich, "r", encoding="utf-8") as f:
            enrich_json = json.load(f)

    etalon = generate_from_reparse(raw_json)
    diff = diff_etalon_vs_enrich(etalon, enrich_json)

    if args.format == "json":
        # Выводим только JSON-отчет
        print(json.dumps(diff, ensure_ascii=False, indent=2))
        return

    signature = etalon.signature
    print(colored(f"\nVALIDATION REPORT FOR: {signature}", "cyan"))
    print(f"Helius Explorer: https://explorer.helius.xyz/tx/{signature}")
    print(f"Solscan: https://solscan.io/tx/{signature}")
    print("-" * 60)
    for d in diff["diffs"]:
        sev = d["severity"]
        color = "red" if sev == "CRITICAL" else ("yellow" if sev == "WARNING" else "white")
        print(colored(f"[{sev}] Field: {d['field']}", color))
        print(colored(f"  - Etalon:  {d['etalon']}", color))
        print(colored(f"  - Enrich:  {d['enrich']}", color))
        print("-" * 60)
    print(colored(f"CRITICAL: {diff['summary']['critical']}  WARNING: {diff['summary']['warning']}  INFO: {diff['summary']['info']}", "magenta"))

if __name__ == "__main__":
    main() 