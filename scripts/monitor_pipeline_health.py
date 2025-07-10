import os
import time
import datetime
import psycopg2
from utils.alerter import send_alert

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'solana_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'Droper80moper!'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
}

LOG_DIR = 'logs'
RPC_FAIL_LOG = os.path.join(LOG_DIR, 'failed_receive_signatures.log')
CONSOLE_LOG = os.path.join(LOG_DIR, 'console_run.log')

CRITICAL_KEYWORDS = ["CRITICAL", "UNIVERSAL PARSER ERROR", "DB SAVE ERROR"]
PIPELINE_LAG_THRESHOLD_MINUTES = 15
CONSOLE_LOG_LOOKBACK_HOURS = 6

def check_rpc_failures():
    if os.path.exists(RPC_FAIL_LOG):
        with open(RPC_FAIL_LOG, 'r', encoding='utf-8') as f:
            lines = [line for line in f if line.strip()]
        if lines:
            send_alert("RPC Failures Detected", f"RPCClient не смог получить данные для {len(lines)} сигнатур. См. {RPC_FAIL_LOG}")
            return True
    return False

def check_parsing_errors():
    if not os.path.exists(CONSOLE_LOG):
        return False
    now = time.time()
    cutoff = now - CONSOLE_LOG_LOOKBACK_HOURS * 3600
    found = False
    with open(CONSOLE_LOG, 'r', encoding='utf-8') as f:
        for line in f:
            if any(kw in line for kw in CRITICAL_KEYWORDS):
                # Пробуем извлечь timestamp (если есть)
                try:
                    ts = None
                    if '[' in line and ']' in line:
                        ts_str = line.split('[')[1].split(']')[0]
                        dt = datetime.datetime.fromisoformat(ts_str)
                        ts = dt.timestamp()
                    if ts is None or ts >= cutoff:
                        send_alert("Critical Parsing Error", f"Обнаружена критическая ошибка в {CONSOLE_LOG}:\n{line.strip()}")
                        found = True
                except Exception:
                    send_alert("Critical Parsing Error", f"Обнаружена критическая ошибка в {CONSOLE_LOG}:\n{line.strip()}")
                    found = True
    return found

def check_pipeline_lag():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT MAX(block_time) FROM transactions;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row or not row[0]:
            send_alert("Pipeline Lag Detected", "Не удалось получить block_time из БД.")
            return True
        last_block_time = int(row[0])
        now_utc = int(time.time())
        lag_minutes = (now_utc - last_block_time) / 60
        if lag_minutes > PIPELINE_LAG_THRESHOLD_MINUTES:
            send_alert("Pipeline Lag Detected", f"Пайплайн отстал от реального времени на {lag_minutes:.1f} минут.")
            return True
    except Exception as e:
        send_alert("Pipeline Lag Check Error", f"Ошибка при проверке отставания пайплайна: {e}")
        return True
    return False

def main():
    print("Running pipeline health check...")
    any_alert = False
    if check_rpc_failures():
        any_alert = True
    if check_parsing_errors():
        any_alert = True
    if check_pipeline_lag():
        any_alert = True
    if not any_alert:
        print("[OK] Pipeline health: no critical issues detected.")
    print("Health check finished.")

if __name__ == "__main__":
    main() 