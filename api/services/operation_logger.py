import time
import db.db_manager as dbm

def log_operation_step(operation_id: str, level: str, message: str):
    """
    Записывает лог в таблицу operation_logs для указанной операции.
    """
    timestamp = time.time()
    conn = dbm.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO operation_logs (operation_id, timestamp, level, message)
            VALUES (?, ?, ?, ?)
            """,
            (operation_id, timestamp, level, message)
        )
        conn.commit()
    finally:
        dbm.release_connection(conn) 