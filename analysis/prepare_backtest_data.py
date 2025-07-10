import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "db/solana_db.sqlite"
EVENTS_TABLE = "enhanced_ml_events"
PRICES_TABLE = "historical_prices"
OUTPUT_PATH = "analysis/backtest_input_data.pkl"

# Параметры window для join (секунд)
PRICE_WINDOW_SEC = 60  # ищем ближайшую цену в пределах 1 минуты

def load_events(conn):
    # Используем block_time как временную метку
    query = f"""
        SELECT *, block_time as timestamp FROM {EVENTS_TABLE}
    """
    df = pd.read_sql(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df

def load_prices(conn):
    query = f"""
        SELECT * FROM {PRICES_TABLE}
    """
    df = pd.read_sql(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df

def join_events_prices(events, prices):
    # Для каждого события ищем ближайшую цену по token_address и времени
    events = events.sort_values(['token_a_mint', 'timestamp'])
    prices = prices.sort_values(['token_address', 'timestamp'])
    merged = pd.merge_asof(
        events,
        prices,
        left_by='token_a_mint',
        right_by='token_address',
        left_on='timestamp',
        right_on='timestamp',
        direction='backward',
        tolerance=pd.Timedelta(seconds=PRICE_WINDOW_SEC)
    )
    # Оставляем только события, для которых нашлась цена
    merged = merged.dropna(subset=['open', 'close'])
    # Формируем итоговый DataFrame для Backtester
    result = pd.DataFrame({
        'timestamp': merged['timestamp'],
        'price': merged['close'],
        'event': merged.apply(lambda row: row.to_dict(), axis=1),
        'token': merged['token_a_mint']
    })
    result = result.sort_values('timestamp').reset_index(drop=True)
    return result

def main():
    conn = sqlite3.connect(DB_PATH)
    events = load_events(conn)
    prices = load_prices(conn)
    conn.close()
    backtest_data = join_events_prices(events, prices)
    backtest_data.to_pickle(OUTPUT_PATH)
    print(f"Backtest input data saved to {OUTPUT_PATH}. Rows: {len(backtest_data)}")

if __name__ == "__main__":
    main() 