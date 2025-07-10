"""
report_dashboard.py
Генерирует HTML-дашборд по QC-метрикам и состоянию wallet_links/link_events.
"""
import sys
import psycopg2
from pathlib import Path
from qc import checks
from datetime import datetime

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
DASHBOARD_PATH = Path(__file__).parent.parent / 'logs' / 'qc_dashboard.html'

QC_CHECKS = [
    checks.check_missing_fields,
    checks.check_duplicate_signatures,
    checks.check_orphan_token_transfers,
    checks.check_enrich_errors,
    checks.check_orphan_wallet_links,
]

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
    qc_results = [check(conn) for check in QC_CHECKS]
    wl_stats = audit_wallet_links(conn)
    conn.close()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    html = f"""
    <html><head><title>Solana Pipeline QC Dashboard</title></head>
    <body>
    <h1>Solana Pipeline QC Dashboard</h1>
    <p>Last updated: {now}</p>
    <h2>QC Checks</h2>
    <ul>
    """
    for res in qc_results:
        html += f'<li><b>{res["check"]}</b>: {res}</li>'
    html += """
    </ul>
    <h2>Wallet Links Stats</h2>
    <ul>
    """
    for k, v in wl_stats.items():
        html += f'<li><b>{k}</b>: {v}</li>'
    html += """
    </ul>
    </body></html>
    """
    DASHBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DASHBOARD_PATH, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"QC Dashboard saved to {DASHBOARD_PATH}")

if __name__ == '__main__':
    main() 