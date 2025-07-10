#!/usr/bin/env python3
"""
Скрипт для управления фикстурами регрессионного тестирования.
Позволяет перегенерировать эталонные данные с использованием нового декодера.

Использование:
    python scripts/manage_fixtures.py --add <signature>                 # Добавить новую фикстуру
    python scripts/manage_fixtures.py --update-from-list <file_path>    # Обновить фикстуры из списка
    python scripts/manage_fixtures.py --re-enrich <signature>           # Перегенерировать эталон для фикстуры
    python scripts/manage_fixtures.py --re-enrich-all                   # Перегенерировать все эталоны
"""
import argparse
import os
import sys
import json
import time
import requests
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from rpc.client import RPCClient
from parser.universal_parser import UniversalParser

FIXTURES_ROOT = Path('tests/regression/fixtures')
GOLDEN_SIGNATURES = Path('tests/regression/golden_signatures.txt')

def ensure_fixture_dir(signature):
    d = FIXTURES_ROOT / signature
    d.mkdir(parents=True, exist_ok=True)
    return d

def save_json(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def fetch_and_save_raw(signature, rpc_client):
    raw_obj = rpc_client.get_transaction(signature, encoding='json')
    if not raw_obj:
        raise RuntimeError(f'Не удалось получить данные для эталонной сигнатуры {signature}. Проверьте доступность транзакции вручную.')
    d = ensure_fixture_dir(signature)
    save_json(raw_obj.raw_json, d / 'raw.json')
    return raw_obj.raw_json

def enrich_and_save(signature, raw=None, force=False):
    """
    Генерирует эталонный файл обогащения для указанной фикстуры.
    
    Args:
        signature: Сигнатура транзакции (имя директории с фикстурой)
        raw: Уже загруженная транзакция или None для загрузки из файла
        force: Если True, перезаписывает эталон даже если он уже существует
    """
    d = ensure_fixture_dir(signature)
    etalon_path = d / "enrich.etalon.json"
    
    if not force and etalon_path.exists():
        print(f"Предупреждение: Эталонный файл {etalon_path} уже существует. Используйте --force для перезаписи.")
        return
    
    if raw is None:
        raw_path = d / 'raw.json'
        if not raw_path.exists():
            raise FileNotFoundError(f'raw.json для {signature} не найден. Сначала скачайте raw.')
        raw = load_json(raw_path)
    
    print(f"Обрабатываю фикстуру {signature}...")
    
    parser = UniversalParser()
    result = parser.parse_raw_transaction(raw)
    
    save_json(result, etalon_path)
    print(f"Эталон успешно обновлен: {etalon_path}")

def add_signature(signature, rpc_client, force=False):
    d = ensure_fixture_dir(signature)
    raw_path = d / 'raw.json'
    if not raw_path.exists():
        print(f'Скачиваю raw для {signature}...')
        raw = fetch_and_save_raw(signature, rpc_client)
    else:
        print(f'raw.json для {signature} уже существует, пропускаю скачивание.')
        raw = load_json(raw_path)
    print(f'Обогащаю транзакцию {signature}...')
    enrich_and_save(signature, raw, force)
    print(f'Готово: {signature}')

def update_from_list(list_path, rpc_client, force=False):
    with open(list_path, 'r', encoding='utf-8') as f:
        sigs = [line.strip() for line in f if line.strip()]
    for idx, sig in enumerate(sigs):
        add_signature(sig, rpc_client, force)

def re_enrich_all_fixtures(force=False):
    """
    Перегенерирует эталоны для всех фикстур.
    
    Args:
        force: Если True, перезаписывает все эталоны
    """
    signatures = [d.name for d in FIXTURES_ROOT.iterdir() if d.is_dir()]
    if not signatures:
        print("Не найдены директории с фикстурами в", FIXTURES_ROOT)
        return
    
    print(f"Найдено {len(signatures)} фикстур для обновления.")
    for signature in signatures:
        enrich_and_save(signature, force=force)
    
    print(f"Обновление завершено для {len(signatures)} фикстур.")

def main():
    parser = argparse.ArgumentParser(description="Утилита для управления фикстурами регрессионного тестирования")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--add", metavar="SIGNATURE", help="Добавить новую фикстуру, скачав транзакцию и сгенерировав эталон")
    group.add_argument("--update-from-list", metavar="FILE_PATH", help="Обновить фикстуры из файла со списком сигнатур")
    group.add_argument("--re-enrich", metavar="SIGNATURE", help="Перегенерировать эталон для указанной сигнатуры")
    group.add_argument("--re-enrich-all", action="store_true", help="Перегенерировать эталоны для всех фикстур")
    
    parser.add_argument("--force", action="store_true", help="Принудительная перезапись существующих эталонов")
    
    args = parser.parse_args()
    
    rpc_client = RPCClient()

    if args.add:
        add_signature(args.add, rpc_client, args.force)
    elif args.update_from_list:
        update_from_list(args.update_from_list, rpc_client, args.force)
    elif args.re_enrich:
        enrich_and_save(args.re_enrich, force=args.force)
    elif args.re_enrich_all:
        re_enrich_all_fixtures(args.force)
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 