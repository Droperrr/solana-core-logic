import sqlite3
import json

TOKEN = "AGmXzgFQw8bLtsJvr5dgnHtyP6rP6GLwmZwgVqjFL6Q8"
DB_PATH = "db/solana_db.sqlite"
OUT_PATH = "dump_token_raw.jsonl"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute(
    "SELECT signature, block_time, raw_json FROM transactions WHERE source_query_address = ? ORDER BY block_time ASC",
    (TOKEN,)
)
with open(OUT_PATH, "w", encoding="utf-8") as f:
    for sig, block_time, raw_json in cursor.fetchall():
        f.write(json.dumps({
            "signature": sig,
            "block_time": block_time,
            "raw_json": json.loads(raw_json) if raw_json else None
        }, ensure_ascii=False) + "\n")
print(f"Done! Saved to {OUT_PATH}") 