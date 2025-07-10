import argparse
import os
import psycopg2
import json
from typing import List, Dict, Any, Optional
from tabulate import tabulate

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'solana_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'Droper80moper!'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
}

def fetch_transactions(limit: int = 100, signature: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    if signature:
        cur.execute('''
            SELECT signature, additional_context, shadow_enriched_data, block_time
            FROM transactions
            WHERE signature = %s AND additional_context IS NOT NULL AND shadow_enriched_data IS NOT NULL
            ORDER BY block_time DESC
            LIMIT 1
        ''', (signature,))
    else:
        cur.execute('''
            SELECT signature, additional_context, shadow_enriched_data, block_time
            FROM transactions
            WHERE additional_context IS NOT NULL AND shadow_enriched_data IS NOT NULL
            ORDER BY block_time DESC
            LIMIT %s
        ''', (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    txs = []
    for row in rows:
        txs.append({
            'signature': row[0],
            'additional_context': row[1],
            'shadow_enriched_data': row[2],
            'block_time': row[3],
        })
    return txs

def parse_json_field(field) -> Any:
    if field is None:
        return None
    if isinstance(field, dict) or isinstance(field, list):
        return field
    try:
        return json.loads(field)
    except Exception:
        return None

def extract_legacy_swap(additional_context: dict) -> Optional[dict]:
    # Пример: ищем swap_summary
    return additional_context.get('swap_summary')

def extract_new_swap(enriched_events: list) -> Optional[dict]:
    for event in enriched_events:
        if event.get('event_type') == 'SWAP':
            return event
    return None

def compare_swap(legacy_swap: dict, new_swap: dict) -> List[str]:
    mismatches = []
    matches = []
    if not legacy_swap and not new_swap:
        mismatches.append('❌ MISMATCH: SWAP event not found in both pipelines')
        return mismatches
    if not legacy_swap:
        mismatches.append('❌ MISMATCH: Legacy swap_summary not found')
        return mismatches
    if not new_swap:
        mismatches.append('❌ MISMATCH: New SWAP event not found')
        return mismatches
    # Сравниваем ключевые поля
    for key_legacy, key_new, label in [
        ('mint_in', 'token_a_mint', 'mint_in'),
        ('amount_in', 'token_a_amount_change', 'amount_in'),
        ('mint_out', 'token_b_mint', 'mint_out'),
        ('amount_out', 'token_b_amount_change', 'amount_out'),
    ]:
        legacy_val = legacy_swap.get(key_legacy)
        new_val = new_swap.get(key_new)
        if str(legacy_val) == str(new_val):
            matches.append(f'✅ MATCH: {label} ({legacy_val})')
        else:
            mismatches.append(f'❌ MISMATCH: {label}\n   - Legacy: {legacy_val}\n   - New   : {new_val}')
    return matches + mismatches

def extract_new_transfers(enriched_events: list) -> List[dict]:
    return [e for e in enriched_events if e.get('event_type') == 'TRANSFER']

def compare_transfers(legacy_context: dict, new_transfers: List[dict]) -> List[str]:
    # Пример: если в legacy нет transfer-деталей, просто информируем
    if not new_transfers:
        return ['❌ MISMATCH: No TRANSFER events in new pipeline']
    return [f'- Found {len(new_transfers)} transfer events in New Pipeline.']

def print_report(signature: str, swap_report: List[str], transfer_report: List[str], show_matches: bool):
    print('='*70)
    print(f'Comparing Signature: {signature}')
    print('='*70)
    print('\n[SWAP]')
    for line in swap_report:
        if show_matches or line.startswith('❌'):
            print('  ' + line)
    print('\n[TRANSFERS]')
    for line in transfer_report:
        if show_matches or line.startswith('❌'):
            print('  ' + line)
    print('-'*70)
    mismatch_count = sum(1 for l in swap_report+transfer_report if l.startswith('❌'))
    match_count = sum(1 for l in swap_report+transfer_report if l.startswith('✅'))
    print(f'Summary for {signature}: {mismatch_count} MISMATCHES, {match_count} MATCHES')
    print('='*70)

def main():
    parser = argparse.ArgumentParser(description='Compare legacy and shadow pipeline results.')
    parser.add_argument('--limit', type=int, default=100, help='Number of transactions to compare (default: 100)')
    parser.add_argument('--signature', type=str, help='Compare a specific transaction by signature')
    parser.add_argument('--show-matches', action='store_true', help='Show matches as well as mismatches')
    args = parser.parse_args()

    txs = fetch_transactions(limit=args.limit, signature=args.signature)
    if not txs:
        print('No transactions found for comparison.')
        return
    for tx in txs:
        signature = tx['signature']
        legacy_context = parse_json_field(tx['additional_context'])
        shadow_data = parse_json_field(tx['shadow_enriched_data'])
        if not legacy_context or not shadow_data:
            print(f"[WARN] Skipping {signature}: missing data.")
            continue
        legacy_swap = extract_legacy_swap(legacy_context)
        new_swap = extract_new_swap(shadow_data)
        swap_report = compare_swap(legacy_swap, new_swap)
        new_transfers = extract_new_transfers(shadow_data)
        transfer_report = compare_transfers(legacy_context, new_transfers)
        print_report(signature, swap_report, transfer_report, args.show_matches)

if __name__ == '__main__':
    main() 