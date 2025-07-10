#!/usr/bin/env python3
import sqlite3
import json

DB_PATH = "db/solana_db.sqlite"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("SELECT id, signature, reason, failed_at, length(payload) as payload_len, retry_count, status FROM failed_processing_log ORDER BY failed_at DESC LIMIT 5")
rows = cur.fetchall()

print("Последние записи в failed_processing_log:")
for row in rows:
    print(f"id={row['id']} | signature={row['signature']} | reason={row['reason']} | failed_at={row['failed_at']} | payload_len={row['payload_len']} | retry_count={row['retry_count']} | status={row['status']}")

conn.close() 