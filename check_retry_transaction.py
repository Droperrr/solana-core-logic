#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect('db/solana_db.sqlite')
cursor = conn.cursor()

signature = "3TZ15bGjHmoUST4eHATy68XiDkjg3P7ct1xmknpSisNv3D5awF4GqHXTjV53Qxue1WLo4YrC5e3UaL4PNx4fuEok"

cursor.execute('''
    SELECT signature, source_query_type, source_query_address, parser_version, updated_at 
    FROM transactions 
    WHERE signature = ?
''', (signature,))

result = cursor.fetchone()

if result:
    print(f"✅ Транзакция найдена:")
    print(f"  Signature: {result[0]}")
    print(f"  Source Query Type: {result[1]}")
    print(f"  Source Query Address: {result[2]}")
    print(f"  Parser Version: {result[3]}")
    print(f"  Updated At: {result[4]}")
else:
    print("❌ Транзакция не найдена")

conn.close() 