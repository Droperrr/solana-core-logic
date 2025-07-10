import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '../db/solana_db.sqlite')

def ensure_progress_columns():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(group_task_status);")
    columns = [row[1] for row in cursor.fetchall()]
    changed = False

    if 'progress_percent' not in columns:
        print("[fix] Добавляю колонку progress_percent...")
        cursor.execute("ALTER TABLE group_task_status ADD COLUMN progress_percent REAL DEFAULT 0.0;")
        changed = True
    else:
        print("[fix] Колонка progress_percent уже есть.")

    if 'current_step_description' not in columns:
        print("[fix] Добавляю колонку current_step_description...")
        cursor.execute("ALTER TABLE group_task_status ADD COLUMN current_step_description TEXT;")
        changed = True
    else:
        print("[fix] Колонка current_step_description уже есть.")

    if changed:
        print("[fix] Создаю индекс для progress_percent...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_progress ON group_task_status(progress_percent);")
        print("[fix] Индекс создан.")
        conn.commit()
        print("[fix] Изменения применены.")
    else:
        print("[fix] Изменения не требуются.")
    conn.close()

if __name__ == "__main__":
    ensure_progress_columns() 