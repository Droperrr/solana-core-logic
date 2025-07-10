"""
run_audit.py
Скрипт для аудита состояния данных: QC-проверки, аудит wallet_links и link_events.
"""
import sys
import psycopg2
from pathlib import Path
from qc import checks

# --- Чтение параметров подключения из config/env.md ---
def parse_env_md(path):
    env = {}
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
    return env

ENV_PATH = Path(__file__).parent.parent / 'config' / 'env.md'

def audit_wallet_links(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM wallet_links;')
        total = cur.fetchone()[0]
        cur.execute('SELECT COUNT(DISTINCT tx_signature) FROM wallet_links;')
        unique_tx = cur.fetchone()[0]
        cur.execute('SELECT COUNT(*) FROM link_events;')
        total_events = cur.fetchone()[0]
    return {
        'wallet_links_total': total,
        'wallet_links_unique_tx': unique_tx,
        'link_events_total': total_events
    }

def main():
    env = parse_env_md(ENV_PATH)
    conn = psycopg2.connect(
        host=env['DB_HOST'],
        port=env['DB_PORT'],
        dbname=env['DB_NAME'],
        user=env['DB_USER'],
        password=env['DB_PASSWORD']
    )
    print('QC Audit:')
    # --- QC Checks ---
    for check_fn in [
        checks.check_missing_fields,
        checks.check_duplicate_signatures,
        checks.check_orphan_token_transfers,
        checks.check_enrich_errors,
        checks.check_orphan_wallet_links,
    ]:
        result = check_fn(conn)
        print(result)
    # --- Wallet Links Audit ---
    wl_stats = audit_wallet_links(conn)
    print('Wallet Links Stats:', wl_stats)
    conn.close()

if __name__ == '__main__':
    main() 