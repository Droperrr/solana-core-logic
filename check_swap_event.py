import sqlite3
import json

SIGNATURE = "3TZ15bGjHmoUST4eHATy68XiDkjg3P7ct1xmknpSisNv3D5awF4GqHXTjV53Qxue1WLo4YrC5e3UaL4PNx4fuEok"
DB_PATH = "db/solana_db.sqlite"

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT event_data_raw FROM ml_ready_events WHERE signature = ? AND event_type = ? LIMIT 1', (SIGNATURE, 'SWAP'))
    row = c.fetchone()
    if row:
        try:
            details = json.loads(row[0])
            print(json.dumps(details, ensure_ascii=False, indent=2))
        except Exception as e:
            print(f"Ошибка при разборе JSON: {e}\nСырой event_data_raw: {row[0]}")
    else:
        print("No SWAP event found.")
    conn.close()

if __name__ == "__main__":
    main() 