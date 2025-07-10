import psycopg2

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "solana_db"
DB_USER = "postgres"
DB_PASSWORD = "Droper80moper!"

TYPES = [
    "buy",
    "sell",
    "transfer",
    "failed_transaction",
    "account_management"
]

if __name__ == "__main__":
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    print("Тип транзакции | Signature")
    print("----------------|------------------------------------------")
    for tx_type in TYPES:
        cur.execute("SELECT signature FROM transactions WHERE transaction_type = %s LIMIT 1;", (tx_type,))
        row = cur.fetchone()
        if row:
            print(f"{tx_type:16} | {row[0]}")
        else:
            print(f"{tx_type:16} | [нет записей]")
    cur.close()
    conn.close() 