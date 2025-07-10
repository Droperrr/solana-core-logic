import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import os
from pathlib import Path

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from decoder.decoder import TransactionDecoder

def process_line(line, decoder):
    try:
        tx = json.loads(line)
        raw_json = tx.get('raw_json', tx)  # поддержка как {raw_json: ...}, так и просто raw
        enriched = decoder.decode_transaction(raw_json)
        return json.dumps({
            'signature': tx.get('signature'),
            'block_time': tx.get('block_time'),
            'enriched': enriched
        }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({'error': str(e), 'line': line[:200]})

def process_dump_file(input_path: str, output_path: str, workers: int = 4):
    """
    Обогащает транзакции из JSONL-файла и сохраняет результат в output_path.
    input_path: путь к входному JSONL-файлу (одна транзакция на строку)
    output_path: путь к выходному JSONL-файлу
    workers: количество потоков
    """
    decoder = TransactionDecoder()
    with open(input_path, 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
    results = [None] * len(lines)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_line, line, decoder): i for i, line in enumerate(lines)}
        for f in tqdm(as_completed(futures), total=len(futures), desc='Enriching'):
            i = futures[f]
            try:
                results[i] = f.result()
            except Exception as e:
                results[i] = json.dumps({'error': str(e)})
    with open(output_path, 'w', encoding='utf-8') as fout:
        for res in results:
            if res:
                fout.write(res + '\n')

def main():
    parser = argparse.ArgumentParser(description="Batch enrichment of raw Solana transactions from JSONL file.")
    parser.add_argument('--input-file', required=True, help='Path to input raw JSONL file')
    parser.add_argument('--output-file', required=True, help='Path to output enriched JSONL file')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    args = parser.parse_args()
    process_dump_file(args.input_file, args.output_file, args.workers)

if __name__ == '__main__':
    main() 