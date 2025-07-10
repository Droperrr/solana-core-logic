#!/usr/bin/env python3
"""
Финальная верификация функции полного подсчета транзакций (включая ATA)
"""

from utils.signature_handler import fetch_signatures_for_token
import time
import logging

logging.basicConfig(level=logging.INFO)

TARGET_TOKEN = "AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC"

if __name__ == "__main__":
    print("🧪 Финальная верификация функции полного подсчета транзакций (включая ATA)")
    print(f"Токен: {TARGET_TOKEN}")
    start = time.time()
    print("Запуск fetch_signatures_for_token... (это может занять несколько минут)")
    try:
        signatures = fetch_signatures_for_token(
            token_mint_address=TARGET_TOKEN,
            fetch_limit_per_call=1000,
            total_tx_limit=None,
            direction='e'
        )
        elapsed = time.time() - start
        if signatures is None:
            print("❌ Ошибка: функция вернула None")
        else:
            count = len(signatures)
            print(f"\n✅ Итоговое количество уникальных сигнатур: {count}")
            print(f"⏱️ Время выполнения: {elapsed:.1f} секунд")
            if count < 1000:
                print("⚠️ ВНИМАНИЕ: Количество транзакций подозрительно мало. Проверьте RPC лимиты или корректность работы fetch_signatures_for_token.")
            elif count > 2000:
                print("⚠️ ВНИМАНИЕ: Количество транзакций подозрительно велико. Проверьте на дубликаты.")
            else:
                print("✅ Количество транзакций выглядит реалистично.")
    except Exception as e:
        print(f"❌ Исключение при вызове функции: {e}")
        import traceback
        traceback.print_exc() 