import sys
import sqlite3
import json

if len(sys.argv) != 2:
    print("Usage: python scripts/get_enriched_data.py <SIGNATURE>")
    sys.exit(1)

SIGNATURE = sys.argv[1]
DB_PATH = "db/solana_db.sqlite"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()
c.execute('SELECT enriched_data FROM transactions WHERE signature = ? LIMIT 1', (SIGNATURE,))
row = c.fetchone()
if row and row[0]:
    try:
        data = json.loads(row[0])
        print(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"Ошибка при разборе JSON: {e}\nСырой enriched_data: {row[0]}")
else:
    print("No enriched_data found for this signature.")
conn.close() 