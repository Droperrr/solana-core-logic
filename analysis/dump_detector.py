import sqlite3
import json
import argparse
from typing import List, Dict, Any, Optional

DB_PATH = "db/solana_db.sqlite"

class DumpDetector:
    def __init__(self, db_path: str = DB_PATH):
        """
        Инициализирует детектор дампов.

        Args:
            db_path: Путь к базе данных SQLite.
        """
        self.db_path = db_path

    def _fetch_swap_events(self, token_address: str) -> List[Dict[str, Any]]:
        """
        Извлекает все события SWAP для указанного токена из таблицы `transactions`,
        распаковывая и фильтруя данные из поля `enriched_data`.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = """
        SELECT
            signature,
            block_time,
            enriched_data
        FROM
            transactions
        WHERE
            source_query_address = ?
        ORDER BY
            block_time ASC;
        """
        
        swap_events = []
        try:
            cursor.execute(query, (token_address,))
            rows = cursor.fetchall()
            
            for row in rows:
                signature, block_time, enriched_data_json = row
                if not enriched_data_json:
                    continue
                
                enriched_data = json.loads(enriched_data_json)
                
                for event in enriched_data:
                    if event.get('event_type') == 'SWAP':
                        price_data = event.get('enrichment_data', {}).get('price_enricher')
                        if price_data and price_data.get('price_in_sol'):
                            swap_events.append({
                                'signature': signature,
                                'timestamp': block_time,
                                'price_sol': price_data['price_in_sol'],
                                'volume_sol': price_data['volume_in_sol']
                            })
            
            return swap_events
        finally:
            conn.close()

    def find_dumps_for_token(
        self,
        token_address: str,
        price_drop_threshold: float = 0.5,
        time_window_seconds: int = 3600,
        volume_threshold_sol: float = 10.0
    ) -> List[Dict[str, Any]]:
        """
        Анализирует временной ряд цен и объемов для токена и обнаруживает дампы.

        Args:
            token_address: Адрес токена для анализа.
            price_drop_threshold: Порог падения цены (например, 0.5 для 50%).
            time_window_seconds: Временное окно в секундах для поиска падения.
            volume_threshold_sol: Минимальный совокупный объем в SOL за период, 
                                  чтобы считать событие значимым.

        Returns:
            Список словарей, где каждый словарь описывает обнаруженный дамп.
        """
        
        swap_events = self._fetch_swap_events(token_address)
        
        if not swap_events or len(swap_events) < 2:
            print(f"No usable SWAP events found for token {token_address}. Cannot analyze for dumps.")
            return []

        print(f"Analyzing {len(swap_events)} swap events for token {token_address}...")
        found_dumps = []
        
        for i in range(len(swap_events)):
            start_event = swap_events[i]
            start_price = start_event.get('price_sol')
            start_timestamp = start_event.get('timestamp')

            if not start_price or not start_timestamp:
                continue

            cumulative_volume_in_window = start_event.get('volume_sol', 0)
            
            for j in range(i + 1, len(swap_events)):
                current_event = swap_events[j]
                current_timestamp = current_event.get('timestamp')
                
                if not current_timestamp:
                    continue
                
                if current_timestamp - start_timestamp > time_window_seconds:
                    break
                
                current_price = current_event.get('price_sol')
                if not current_price:
                    continue

                cumulative_volume_in_window += current_event.get('volume_sol', 0)

                price_drop = (start_price - current_price) / start_price
                
                if price_drop >= price_drop_threshold and cumulative_volume_in_window >= volume_threshold_sol:
                    dump_info = {
                        "start_signature": start_event['signature'],
                        "end_signature": current_event['signature'],
                        "start_timestamp": start_timestamp,
                        "end_timestamp": current_timestamp,
                        "duration_seconds": current_timestamp - start_timestamp,
                        "start_price_sol": start_price,
                        "end_price_sol": current_price,
                        "price_drop_percentage": f"{price_drop * 100:.2f}%",
                        "cumulative_volume_sol": cumulative_volume_in_window
                    }
                    found_dumps.append(dump_info)
                    break 

        print(f"Found {len(found_dumps)} potential dumps.")
        return found_dumps

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Find price dumps for a given Solana token.")
    parser.add_argument("--token", required=True, help="The token address to analyze.")
    args = parser.parse_args()

    detector = DumpDetector()
    
    dumps = detector.find_dumps_for_token(
        token_address=args.token,
        price_drop_threshold=0.3,
        time_window_seconds=60 * 10 # 10 минут
    )
    
    if dumps:
        print(f"\n--- Found {len(dumps)} Dumps ---")
        for i, dump in enumerate(dumps, 1):
            print(f"\n--- Dump #{i} ---")
            for key, value in dump.items():
                print(f"{key}: {value}")
    else:
        print("\n--- No Dumps Found ---")
        print("No significant price dumps were found for the given criteria.") 