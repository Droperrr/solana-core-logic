#!/usr/bin/env python3
"""
Полный анализ токена: поиск координированной активности и корреляция с дампами.

Этот скрипт является основной точкой входа для анализа токенов.
Он объединяет:
1. Поиск "горячих" минут с координированной активностью
2. Поиск дампов цены
3. Корреляционный анализ

Использование:
    python analysis/run_full_analysis.py --token <TOKEN_ADDRESS>
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Добавляем корневую директорию в путь
sys.path.append('.')

from analysis.find_coordinated_activity import find_coordinated_activity
from analysis.dump_detector import DumpDetector
from db.db_manager import get_connection

# Настройка логирования
logger = logging.getLogger(__name__)

def analyze_token_correlation(token_address: str, 
                            activity_window_minutes: int = 60,
                            dump_threshold_percent: float = 10.0,
                            correlation_window_hours: int = 24) -> Dict:
    """
    Выполняет полный анализ токена: координированная активность + дампы + корреляция.
    
    Args:
        token_address: Адрес токена для анализа
        activity_window_minutes: Окно для поиска координированной активности (минуты)
        dump_threshold_percent: Порог для определения дампа (%)
        correlation_window_hours: Окно корреляции (часы)
    
    Returns:
        Словарь с результатами анализа
    """
    logger.info(f"🚀 Начинаем полный анализ токена: {token_address}")
    
    try:
        # Подключение к БД
        conn = get_connection()
        
        # Шаг 1: Поиск координированной активности
        logger.info("📊 Шаг 1: Поиск координированной активности...")
        activity_results = find_coordinated_activity(
            conn=conn,
            threshold=3  # Минимум 3 транзакции в минуту
        )
        
        if not activity_results:
            logger.warning("❌ Координированная активность не найдена")
            return {
                "token_address": token_address,
                "analysis_timestamp": datetime.now().isoformat(),
                "coordinated_activity": None,
                "dumps": None,
                "correlation": None,
                "status": "no_activity_found"
            }
        
        # Конвертируем кортежи в словари для единообразия
        activity_dicts = []
        for minute, tx_count in activity_results:
            # Парсим минуту в timestamp
            dt = datetime.strptime(minute, '%Y-%m-%d %H:%M')
            timestamp = int(dt.timestamp())
            
            activity_dicts.append({
                "timestamp": timestamp,
                "minute": minute,
                "transaction_count": tx_count,
                "unique_wallets": 0  # Пока не реализовано
            })
        
        logger.info(f"✅ Найдено {len(activity_dicts)} периодов координированной активности")
        
        # Шаг 2: Поиск дампов
        logger.info("📉 Шаг 2: Поиск дампов цены...")
        dump_detector = DumpDetector()
        dump_results = dump_detector.find_dumps_for_token(
            token_address=token_address,
            threshold_percent=dump_threshold_percent,
            conn=conn
        )
        
        if not dump_results:
            logger.warning("❌ Дампы не найдены")
            return {
                "token_address": token_address,
                "analysis_timestamp": datetime.now().isoformat(),
                "coordinated_activity": activity_dicts,
                "dumps": None,
                "correlation": None,
                "status": "no_dumps_found"
            }
        
        logger.info(f"✅ Найдено {len(dump_results)} дампов")
        
        # Шаг 3: Корреляционный анализ
        logger.info("🔍 Шаг 3: Корреляционный анализ...")
        correlation_results = analyze_correlation(
            activity_results=activity_dicts,
            dump_results=dump_results,
            correlation_window_hours=correlation_window_hours
        )
        
        # Формируем итоговый отчет
        final_report = {
            "token_address": token_address,
            "analysis_timestamp": datetime.now().isoformat(),
            "parameters": {
                "activity_window_minutes": activity_window_minutes,
                "dump_threshold_percent": dump_threshold_percent,
                "correlation_window_hours": correlation_window_hours
            },
            "coordinated_activity": {
                "total_periods": len(activity_dicts),
                "periods": activity_dicts
            },
            "dumps": {
                "total_dumps": len(dump_results),
                "dumps": dump_results
            },
            "correlation": correlation_results,
            "status": "success"
        }
        
        logger.info("✅ Полный анализ завершен успешно")
        return final_report
        
    except Exception as e:
        logger.error(f"❌ Ошибка при анализе токена {token_address}: {e}", exc_info=True)
        return {
            "token_address": token_address,
            "analysis_timestamp": datetime.now().isoformat(),
            "error": str(e),
            "status": "error"
        }
    finally:
        if 'conn' in locals():
            conn.close()

def analyze_correlation(activity_results: List[Dict], 
                       dump_results: List[Dict],
                       correlation_window_hours: int) -> Dict:
    """
    Анализирует корреляцию между координированной активностью и дампами.
    
    Args:
        activity_results: Результаты поиска координированной активности
        dump_results: Результаты поиска дампов
        correlation_window_hours: Окно корреляции в часах
    
    Returns:
        Словарь с результатами корреляционного анализа
    """
    logger.info("🔍 Анализ корреляции между активностью и дампами...")
    
    correlations = []
    correlation_window = timedelta(hours=correlation_window_hours)
    
    for dump in dump_results:
        dump_time = datetime.fromtimestamp(dump['timestamp'])
        
        # Ищем активность в окне корреляции перед дампом
        related_activities = []
        for activity in activity_results:
            activity_time = datetime.fromtimestamp(activity['timestamp'])
            time_diff = dump_time - activity_time
            
            # Активность должна быть в окне корреляции и до дампа
            if timedelta(0) <= time_diff <= correlation_window:
                related_activities.append({
                    "activity": activity,
                    "time_before_dump_hours": time_diff.total_seconds() / 3600
                })
        
        if related_activities:
            correlations.append({
                "dump": dump,
                "related_activities": related_activities,
                "activity_count": len(related_activities)
            })
    
    # Статистика
    total_dumps = len(dump_results)
    dumps_with_activity = len(correlations)
    correlation_rate = (dumps_with_activity / total_dumps * 100) if total_dumps > 0 else 0
    
    # Находим наиболее подозрительные случаи
    suspicious_cases = []
    for corr in correlations:
        if corr['activity_count'] >= 2:  # Множественная активность
            suspicious_cases.append(corr)
    
    return {
        "total_dumps": total_dumps,
        "dumps_with_activity": dumps_with_activity,
        "correlation_rate_percent": round(correlation_rate, 2),
        "suspicious_cases_count": len(suspicious_cases),
        "correlations": correlations,
        "suspicious_cases": suspicious_cases
    }

def print_analysis_report(report: Dict):
    """
    Выводит красивый отчет по результатам анализа.
    """
    print("\n" + "="*80)
    print(f"📊 ОТЧЕТ ПО АНАЛИЗУ ТОКЕНА: {report['token_address']}")
    print("="*80)
    
    if report['status'] == 'error':
        print(f"❌ ОШИБКА: {report.get('error', 'Неизвестная ошибка')}")
        return
    
    if report['status'] in ['no_activity_found', 'no_dumps_found']:
        print(f"⚠️  {report['status'].replace('_', ' ').title()}")
        return
    
    # Параметры анализа
    params = report['parameters']
    print(f"🔧 Параметры анализа:")
    print(f"   • Окно активности: {params['activity_window_minutes']} мин")
    print(f"   • Порог дампа: {params['dump_threshold_percent']}%")
    print(f"   • Окно корреляции: {params['correlation_window_hours']} ч")
    
    # Координированная активность
    activity = report['coordinated_activity']
    print(f"\n📈 Координированная активность:")
    print(f"   • Всего периодов: {activity['total_periods']}")
    
    if activity['periods']:
        print(f"   • Последние 3 периода:")
        for i, period in enumerate(activity['periods'][:3]):
            timestamp = datetime.fromtimestamp(period['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            print(f"     {i+1}. {timestamp} - {period['transaction_count']} tx, {period['unique_wallets']} кошельков")
    
    # Дампы
    dumps = report['dumps']
    print(f"\n📉 Дампы цены:")
    print(f"   • Всего дампов: {dumps['total_dumps']}")
    
    if dumps['dumps']:
        print(f"   • Последние 3 дампа:")
        for i, dump in enumerate(dumps['dumps'][:3]):
            timestamp = datetime.fromtimestamp(dump['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            print(f"     {i+1}. {timestamp} - падение на {dump['price_drop_percent']:.1f}%")
    
    # Корреляция
    correlation = report['correlation']
    print(f"\n🔍 Корреляционный анализ:")
    print(f"   • Дампов с активностью: {correlation['dumps_with_activity']}/{correlation['total_dumps']}")
    print(f"   • Процент корреляции: {correlation['correlation_rate_percent']}%")
    print(f"   • Подозрительных случаев: {correlation['suspicious_cases_count']}")
    
    if correlation['suspicious_cases']:
        print(f"\n🚨 ПОДОЗРИТЕЛЬНЫЕ СЛУЧАИ:")
        for i, case in enumerate(correlation['suspicious_cases'][:5]):
            dump = case['dump']
            dump_time = datetime.fromtimestamp(dump['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            print(f"   {i+1}. Дамп {dump_time} (-{dump['price_drop_percent']:.1f}%)")
            print(f"      Активность: {case['activity_count']} периодов")
            for act in case['related_activities'][:2]:
                act_time = datetime.fromtimestamp(act['activity']['timestamp']).strftime('%H:%M:%S')
                hours_before = act['time_before_dump_hours']
                print(f"      • {act_time} ({hours_before:.1f}ч до дампа)")
    
    print("\n" + "="*80)

def main():
    """Основная функция."""
    parser = argparse.ArgumentParser(description='Полный анализ токена: активность + дампы + корреляция')
    parser.add_argument('--token', required=True, help='Адрес токена для анализа')
    parser.add_argument('--activity-window', type=int, default=60, 
                       help='Окно для поиска координированной активности (минуты, по умолчанию: 60)')
    parser.add_argument('--dump-threshold', type=float, default=10.0,
                       help='Порог для определения дампа (%, по умолчанию: 10.0)')
    parser.add_argument('--correlation-window', type=int, default=24,
                       help='Окно корреляции (часы, по умолчанию: 24)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Подробный вывод')
    
    args = parser.parse_args()
    
    # Настройка логирования
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Выполнение анализа
    report = analyze_token_correlation(
        token_address=args.token,
        activity_window_minutes=args.activity_window,
        dump_threshold_percent=args.dump_threshold,
        correlation_window_hours=args.correlation_window
    )
    
    # Вывод отчета
    print_analysis_report(report)
    
    # Возвращаем код выхода
    if report['status'] == 'error':
        sys.exit(1)
    elif report['status'] in ['no_activity_found', 'no_dumps_found']:
        sys.exit(2)
    else:
        sys.exit(0)

if __name__ == '__main__':
    main() 