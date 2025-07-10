#!/usr/bin/env python3
"""
Анализатор паттернов подгрупп токенов на основе гипотезы о первых трех покупках за SOL.

Ключевая гипотеза: токены, управляемые одной и той же алгоритмической группой, 
можно идентифицировать по сумме SOL, потраченных на первые три покупки после листинга.
"""
import sqlite3
import json
import logging
from typing import Dict, List, Tuple, Optional
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SubgroupPatternAnalyzer:
    """Анализатор паттернов подгрупп токенов"""
    
    def __init__(self, db_path: str = "db/solana_db.sqlite"):
        """
        Инициализация анализатора
        
        Args:
            db_path: Путь к базе данных SQLite
        """
        self.db_path = db_path
        self.results = {}
        
    def get_connection(self) -> sqlite3.Connection:
        """Получение соединения с базой данных"""
        return sqlite3.connect(self.db_path)

def find_creator_and_sells(token_address: str, db_path: str = "db/solana_db.sqlite") -> Dict:
    """
    Анализирует один токен для определения создателя, дампера и суммы первых 3 покупок.
    
    Алгоритм:
    1. Находит все покупки за SOL для данного токена
    2. Сортирует их по времени (timestamp)
    3. Берет первые 3 покупки и суммирует потраченные SOL
    4. Определяет создателя как автора крупнейшей покупки
    5. Определяет дампера как автора крупнейшей продажи
    
    Args:
        token_address: Адрес токена для анализа
        db_path: Путь к базе данных
        
    Returns:
        Dict с результатами анализа:
        {
            'token_address': str,
            'creator_wallet': str,
            'dumper_wallet': str,
            'first_three_buys_sum': float,
            'creator_buy_amount': float,
            'dumper_sell_amount': float,
            'total_buys': int,
            'total_sells': int,
            'analysis_timestamp': str
        }
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    logger.info(f"Анализ токена: {token_address}")
    
    result = {
        'token_address': token_address,
        'creator_wallet': None,
        'dumper_wallet': None,
        'first_three_buys_sum': 0.0,
        'creator_buy_amount': 0.0,
        'dumper_sell_amount': 0.0,
        'total_buys': 0,
        'total_sells': 0,
        'analysis_timestamp': datetime.now().isoformat()
    }
    
    try:
        # Получаем все торговые события для токена из enhanced_ml_events
        cursor.execute("""
        SELECT signature, block_time, token_a_mint, token_b_mint, 
               from_amount, to_amount, from_wallet, to_wallet,
               semantic_event_type
        FROM enhanced_ml_events 
        WHERE (token_a_mint = ? OR token_b_mint = ?)
        AND is_trading_related = 1
        AND semantic_event_type = 'SWAP'
        ORDER BY block_time ASC
        """, (token_address, token_address))
        
        events = cursor.fetchall()
        logger.info(f"Найдено {len(events)} торговых событий для токена {token_address}")
        
        # Собираем покупки за SOL
        buys_for_sol = []
        sells_for_sol = []
        max_buy = {'amount': 0, 'wallet': None}
        max_sell = {'amount': 0, 'wallet': None}
        
        # SOL mint address
        SOL_MINT = "So11111111111111111111111111111111111111112"
        
        for signature, block_time, token_a_mint, token_b_mint, from_amount, to_amount, from_wallet, to_wallet, event_type in events:
            try:
                # Определяем направление торговли
                if token_a_mint == SOL_MINT and token_b_mint == token_address:
                    # Покупка токена за SOL (SOL -> TOKEN)
                    sol_amount = from_amount if from_amount else 0
                    wallet = from_wallet
                    
                    # Фильтруем очень маленькие суммы (меньше 0.000001 SOL)
                    if sol_amount > 0.000001 and wallet:
                        buys_for_sol.append({
                            'timestamp': block_time,
                            'sol_amount': sol_amount,
                            'wallet': wallet,
                            'signature': signature
                        })
                        
                        # Отслеживаем крупнейшую покупку
                        if sol_amount > max_buy['amount']:
                            max_buy = {'amount': sol_amount, 'wallet': wallet}
                
                elif token_a_mint == token_address and token_b_mint == SOL_MINT:
                    # Продажа токена за SOL (TOKEN -> SOL)
                    sol_amount = to_amount if to_amount else 0
                    wallet = to_wallet
                    
                    # Фильтруем очень маленькие суммы (меньше 0.000001 SOL)
                    if sol_amount > 0.000001 and wallet:
                        sells_for_sol.append({
                            'timestamp': block_time,
                            'sol_amount': sol_amount,
                            'wallet': wallet,
                            'signature': signature
                        })
                        
                        # Отслеживаем крупнейшую продажу
                        if sol_amount > max_sell['amount']:
                            max_sell = {'amount': sol_amount, 'wallet': wallet}
                            
            except Exception as e:
                logger.debug(f"Ошибка при обработке события {signature}: {str(e)}")
                continue
        
        # Сортируем покупки по времени и берем первые 3
        buys_for_sol.sort(key=lambda x: x['timestamp'])
        first_three_buys = buys_for_sol[:3]
        
        # Вычисляем сумму первых 3 покупок
        first_three_sum = sum(buy['sol_amount'] for buy in first_three_buys)
        
        # Заполняем результаты
        result.update({
            'creator_wallet': max_buy['wallet'],
            'dumper_wallet': max_sell['wallet'],
            'first_three_buys_sum': first_three_sum,
            'creator_buy_amount': max_buy['amount'],
            'dumper_sell_amount': max_sell['amount'],
            'total_buys': len(buys_for_sol),
            'total_sells': len(sells_for_sol)
        })
        
        logger.info(f"Токен {token_address}: первые 3 покупки = {first_three_sum:.6f} SOL")
        logger.info(f"  Создатель: {max_buy['wallet']} (покупка {max_buy['amount']:.6f} SOL)")
        logger.info(f"  Дампер: {max_sell['wallet']} (продажа {max_sell['amount']:.6f} SOL)")
        logger.info(f"  Всего покупок/продаж: {len(buys_for_sol)}/{len(sells_for_sol)}")
        
    except Exception as e:
        logger.error(f"Ошибка при анализе токена {token_address}: {str(e)}")
    
    finally:
        conn.close()
    
    return result

def analyze_all_tokens(db_path: str = "db/solana_db.sqlite") -> List[Dict]:
    """
    Анализирует все токены в базе данных и возвращает сводную таблицу.
    
    Args:
        db_path: Путь к базе данных
        
    Returns:
        List[Dict]: Список результатов анализа для каждого токена
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    logger.info("Получение списка всех токенов из базы данных...")
    
    # Получаем все уникальные токены
    cursor.execute("""
    SELECT DISTINCT source_query_address, COUNT(*) as tx_count
    FROM transactions 
    GROUP BY source_query_address
    ORDER BY tx_count DESC
    """)
    
    tokens = cursor.fetchall()
    conn.close()
    
    logger.info(f"Найдено {len(tokens)} токенов для анализа")
    
    results = []
    for token_address, tx_count in tokens:
        logger.info(f"Анализ токена {token_address} ({tx_count} транзакций)")
        token_result = find_creator_and_sells(token_address, db_path)
        token_result['tx_count'] = tx_count
        results.append(token_result)
    
    return results

def print_analysis_table(results: List[Dict]):
    """
    Выводит результаты анализа в виде таблицы в консоль.
    
    Args:
        results: Список результатов анализа токенов
    """
    print("\n" + "="*120)
    print("АНАЛИЗ ПОДГРУПП ТОКЕНОВ: ПЕРВЫЕ ТРИ ПОКУПКИ ЗА SOL")
    print("="*120)
    
    # Заголовок таблицы
    print(f"{'Токен':<45} {'Сумма 3 покупок':<15} {'Создатель':<45} {'Дампер':<45}")
    print("-" * 120)
    
    # Сортируем по сумме первых трех покупок
    results_sorted = sorted(results, key=lambda x: x['first_three_buys_sum'], reverse=True)
    
    for result in results_sorted:
        token = result['token_address'][:42] + "..." if len(result['token_address']) > 42 else result['token_address']
        sum_sol = f"{result['first_three_buys_sum']:.6f}"
        creator = result['creator_wallet'][:42] + "..." if result['creator_wallet'] and len(result['creator_wallet']) > 42 else (result['creator_wallet'] or "Не найден")
        dumper = result['dumper_wallet'][:42] + "..." if result['dumper_wallet'] and len(result['dumper_wallet']) > 42 else (result['dumper_wallet'] or "Не найден")
        
        print(f"{token:<45} {sum_sol:<15} {creator:<45} {dumper:<45}")
    
    print("-" * 120)
    print(f"Всего проанализировано токенов: {len(results)}")
    
    # Статистика по суммам
    sums = [r['first_three_buys_sum'] for r in results if r['first_three_buys_sum'] > 0]
    if sums:
        print(f"Статистика сумм первых 3 покупок:")
        print(f"  Минимум: {min(sums):.6f} SOL")
        print(f"  Максимум: {max(sums):.6f} SOL")
        print(f"  Среднее: {sum(sums)/len(sums):.6f} SOL")
        print(f"  Медиана: {sorted(sums)[len(sums)//2]:.6f} SOL")

def classify_tokens(results: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Автоматическая классификация токенов на основе сигнатурных сумм.
    
    Args:
        results: Список результатов анализа токенов
        
    Returns:
        Dict с классификацией токенов по группам
    """
    from collections import Counter
    
    # Собираем все суммы первых 3 покупок (исключая нули)
    sums = [r['first_three_buys_sum'] for r in results if r['first_three_buys_sum'] > 0]
    
    if not sums:
        logger.warning("Не найдено токенов с покупками за SOL")
        return {"no_data": results}
    
    # Определяем сигнатурную сумму (наиболее частое значение)
    # Округляем до 6 знаков для группировки похожих значений
    rounded_sums = [round(s, 6) for s in sums]
    sum_counter = Counter(rounded_sums)
    
    if sum_counter:
        signature_sum = sum_counter.most_common(1)[0][0]
        logger.info(f"Сигнатурная сумма: {signature_sum:.6f} SOL")
    else:
        signature_sum = 0
    
    # Классифицируем токены
    high_confidence = []    # ✅ Основная группа
    medium_confidence = []  # ⚠️ Похожий паттерн  
    low_confidence = []     # ❌ Другой паттерн
    
    for result in results:
        sum_val = result['first_three_buys_sum']
        
        if sum_val == 0:
            # Токены без значимых покупок
            result['group_classification'] = "❌ Нет данных (No Trading Data)"
            low_confidence.append(result)
        elif abs(sum_val - signature_sum) < 0.000001:
            # Точное совпадение с сигнатурной суммой
            result['group_classification'] = "✅ Основная группа (High Confidence)"
            high_confidence.append(result)
        elif abs(sum_val - signature_sum) < 0.00001:
            # Близко к сигнатурной сумме
            result['group_classification'] = "⚠️ Похожий паттерн (Medium Confidence)"
            medium_confidence.append(result)
        else:
            # Существенно отличается
            result['group_classification'] = "❌ Другой паттерн (Low Confidence)"
            low_confidence.append(result)
    
    return {
        "high_confidence": high_confidence,
        "medium_confidence": medium_confidence,
        "low_confidence": low_confidence,
        "signature_sum": signature_sum
    }

def create_visualization(results: List[Dict], save_path: str = "subgroup_analysis_histogram.png"):
    """
    Создает визуализацию распределения сумм первых покупок.
    
    Args:
        results: Список результатов анализа
        save_path: Путь для сохранения графика
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np
    
    # Собираем данные для визуализации
    sums = [r['first_three_buys_sum'] for r in results if r['first_three_buys_sum'] > 0]
    token_names = [r['token_address'][:8] + "..." for r in results if r['first_three_buys_sum'] > 0]
    
    if not sums:
        logger.warning("Нет данных для визуализации")
        return
    
    # Настройка стиля
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Создаем фигуру с подграфиками
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # График 1: Гистограмма распределения
    ax1.hist(sums, bins=min(len(sums), 10), alpha=0.7, color='skyblue', edgecolor='navy')
    ax1.set_xlabel('Сумма первых 3 покупок (SOL)')
    ax1.set_ylabel('Количество токенов')
    ax1.set_title('Распределение сумм первых покупок за SOL')
    ax1.grid(True, alpha=0.3)
    
    # Добавляем статистики
    mean_sum = np.mean(sums)
    median_sum = np.median(sums)
    ax1.axvline(mean_sum, color='red', linestyle='--', label=f'Среднее: {mean_sum:.6f}')
    ax1.axvline(median_sum, color='green', linestyle='--', label=f'Медиана: {median_sum:.6f}')
    ax1.legend()
    
    # График 2: Столбчатая диаграмма по токенам
    bars = ax2.bar(range(len(sums)), sums, color='lightcoral', alpha=0.8)
    ax2.set_xlabel('Токены')
    ax2.set_ylabel('Сумма первых 3 покупок (SOL)')
    ax2.set_title('Суммы по токенам')
    ax2.set_xticks(range(len(token_names)))
    ax2.set_xticklabels(token_names, rotation=45)
    ax2.grid(True, alpha=0.3)
    
    # Добавляем значения на столбцы
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{height:.6f}', ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    logger.info(f"График сохранен: {save_path}")
    
    return save_path

def print_enhanced_analysis_table(results: List[Dict], classification: Dict):
    """
    Выводит расширенную таблицу результатов с классификацией.
    
    Args:
        results: Список результатов анализа
        classification: Результаты классификации
    """
    print("\n" + "="*140)
    print("РАСШИРЕННЫЙ АНАЛИЗ ПОДГРУПП ТОКЕНОВ: ПЕРВЫЕ ТРИ ПОКУПКИ ЗА SOL")
    print("="*140)
    
    # Заголовок таблицы
    print(f"{'Токен':<45} {'Сумма 3 покупок':<15} {'Квалификация группы':<35} {'Создатель':<45}")
    print("-" * 140)
    
    # Сортируем по классификации и сумме
    high_conf = classification.get("high_confidence", [])
    medium_conf = classification.get("medium_confidence", [])
    low_conf = classification.get("low_confidence", [])
    
    all_results = high_conf + medium_conf + low_conf
    
    for result in all_results:
        token = result['token_address'][:42] + "..." if len(result['token_address']) > 42 else result['token_address']
        sum_sol = f"{result['first_three_buys_sum']:.6f}"
        classification_str = result.get('group_classification', 'Не классифицирован')
        creator = result['creator_wallet'][:42] + "..." if result['creator_wallet'] and len(result['creator_wallet']) > 42 else (result['creator_wallet'] or "Не найден")
        
        print(f"{token:<45} {sum_sol:<15} {classification_str:<35} {creator:<45}")
    
    print("-" * 140)
    print(f"Всего проанализировано токенов: {len(results)}")
    
    # Сводка по классификации
    signature_sum = classification.get("signature_sum", 0)
    print(f"\n📊 СВОДКА ПО КЛАССИФИКАЦИИ:")
    print(f"  Сигнатурная сумма: {signature_sum:.6f} SOL")
    print(f"  ✅ Основная группа: {len(high_conf)} токенов")
    print(f"  ⚠️ Похожий паттерн: {len(medium_conf)} токенов")
    print(f"  ❌ Другой паттерн: {len(low_conf)} токенов")
    
    # Статистика по суммам
    sums = [r['first_three_buys_sum'] for r in results if r['first_three_buys_sum'] > 0]
    if sums:
        print(f"\n📈 СТАТИСТИКА СУММ:")
        print(f"  Минимум: {min(sums):.6f} SOL")
        print(f"  Максимум: {max(sums):.6f} SOL")
        print(f"  Среднее: {sum(sums)/len(sums):.6f} SOL")
        print(f"  Медиана: {sorted(sums)[len(sums)//2]:.6f} SOL")

def main():
    """Основная функция для запуска анализа"""
    print("🔍 АНАЛИЗАТОР ПАТТЕРНОВ ПОДГРУПП ТОКЕНОВ")
    print("Проверка гипотезы: токены одной алгоритмической группы имеют схожие суммы первых 3 покупок за SOL")
    print("-" * 80)
    
    try:
        # Анализируем все токены
        results = analyze_all_tokens()
        
        # Классифицируем токены
        classification = classify_tokens(results)
        
        # Выводим расширенную таблицу результатов
        print_enhanced_analysis_table(results, classification)
        
        # Создаем визуализацию
        try:
            viz_path = create_visualization(results)
            print(f"\n📊 График сохранен: {viz_path}")
        except Exception as viz_error:
            logger.error(f"Ошибка при создании визуализации: {viz_error}")
        
        print(f"\n✅ Анализ завершен. Результаты сохранены в переменной results.")
        return results, classification
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении анализа: {str(e)}")
        return None

if __name__ == "__main__":
    results = main() 