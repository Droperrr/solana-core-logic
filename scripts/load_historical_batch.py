import os
import argparse
import glob
import time
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Set
from processing.transaction_processor import process_single_transaction
import db.db_writer as dbw
import db.db_manager as dbm

def load_signature_tasks(input_dir: str) -> List[Tuple[str, str]]:
    tasks = []
    for path in glob.glob(os.path.join(input_dir, '*.txt')):
        mint = os.path.splitext(os.path.basename(path))[0]
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                sig = line.strip()
                if sig:
                    tasks.append((sig, mint))
    return tasks

def filter_processed(tasks: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    # Получаем уже обработанные сигнатуры из БД
    sigs = [sig for sig, _ in tasks]
    existing: Set[str] = set()
    conn = dbm.get_connection()
    try:
        existing = dbm.check_existing_signatures(conn, sigs)
    finally:
        dbm.release_connection(conn)
    return [(sig, mint) for sig, mint in tasks if sig not in existing]

def worker(task_queue: Queue, stats: dict, lock: threading.Lock):
    conn = dbm.get_connection()
    while True:
        try:
            item = task_queue.get(timeout=2)
        except Exception:
            break
        sig, mint = item
        try:
            result = process_single_transaction(sig, mint)
            if result:
                dbw.save_parsed_transaction(
                    conn=conn,
                    tx_data=result,
                    current_token_mint=mint,
                    transaction_type=None,
                    source_query_type='token',
                    source_query_address=mint,
                    detected_patterns=None,
                    involved_platforms=None,
                    net_token_flows_json=None,
                    additional_context=None,
                    parsed_tx_version=None,
                    enriched_data=result.get('enriched_events'),
                    parser_version=result.get('parser_version'),
                    shadow_enriched_data=None
                )
                with lock:
                    stats['processed'] += 1
            else:
                with lock:
                    stats['errors'] += 1
        except Exception as e:
            print(f'[ERROR] {sig} ({mint}): {e}')
            with lock:
                stats['errors'] += 1
        finally:
            task_queue.task_done()
    dbm.release_connection(conn)

def main():
    parser = argparse.ArgumentParser(description='Batch historical load for PoC tokens.')
    parser.add_argument('--threads', type=int, default=4, help='Number of parallel worker threads (default: 4)')
    parser.add_argument('--input-dir', type=str, default='data/poc_signature_lists/', help='Directory with signature lists')
    parser.add_argument('--resume', action='store_true', help='Skip already processed signatures')
    args = parser.parse_args()

    print(f'Loading signature tasks from {args.input_dir}...')
    all_tasks = load_signature_tasks(args.input_dir)
    print(f'Total signature-token pairs found: {len(all_tasks)}')
    if args.resume:
        print('Filtering already processed signatures (resume mode)...')
        tasks = filter_processed(all_tasks)
        print(f'Tasks to process after resume-filter: {len(tasks)}')
    else:
        tasks = all_tasks
    if not tasks:
        print('No tasks to process. Exiting.')
        return

    task_queue = Queue()
    for t in tasks:
        task_queue.put(t)

    stats = {'processed': 0, 'errors': 0}
    lock = threading.Lock()
    start_time = time.time()
    print(f'Starting batch processing with {args.threads} threads...')
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        for _ in range(args.threads):
            executor.submit(worker, task_queue, stats, lock)
        task_queue.join()
    elapsed = time.time() - start_time
    print('\nBatch load complete.')
    print(f'Total tasks planned: {len(tasks)}')
    print(f'Successfully processed: {stats["processed"]}')
    print(f'Errors: {stats["errors"]}')
    print(f'Time elapsed: {elapsed/60:.2f} min')

if __name__ == '__main__':
    main() 