"""
Модуль feature_library.py

Содержит набор чистых функций для расчёта аналитических признаков (features) по событиям токенов.
Каждая функция принимает на вход DataFrame с событиями и возвращает рассчитанное значение признака.
"""

import os
import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Set

logger = logging.getLogger("feature_library")

def load_internal_wallets(file_path: str = 'data/internal_wallets.txt') -> Set[str]:
    """
    Загружает список внутренних (алгоритмических) кошельков из файла.
    
    Args:
        file_path: Путь к файлу с адресами кошельков
        
    Returns:
        Множество адресов внутренних кошельков
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Файл с внутренними кошельками не найден: {file_path}")
            return set()
            
        with open(file_path, 'r', encoding='utf-8') as f:
            wallets = set()
            for line in f:
                wallet = line.strip()
                if wallet and not wallet.startswith('#'):
                    wallets.add(wallet)
            logger.info(f"Загружено {len(wallets)} внутренних кошельков из файла {file_path}")
            return wallets
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла с внутренними кошельками {file_path}: {str(e)}")
        return set()

def calculate_daily_volume(events_df: pd.DataFrame, token_address: str, date: Any) -> float:
    """
    Рассчитывает общий объём торгов (в нативных единицах токена) для указанного токена за конкретную дату.
    Суммирует все значения из from_amount и to_amount, где token_address встречается как token_a_mint или token_b_mint.
    
    :param events_df: DataFrame с событиями (обязательные колонки: block_time, token_a_mint, token_b_mint, from_amount, to_amount)
    :param token_address: адрес токена (str)
    :param date: дата (datetime.date или совместимый формат)
    :return: float, общий объём торгов за день
    """
    # Фильтрация по дате (предполагается, что block_time уже приведён к datetime)
    day_events = events_df[(events_df['block_time'].dt.date == date) & (
        (events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)
    )]
    # Суммируем объёмы, где токен был либо отдан, либо получен
    volume = day_events['from_amount'].sum() + day_events['to_amount'].sum()
    return float(volume)

def calculate_transaction_count(events_df: pd.DataFrame, token_address: str, date: Any) -> int:
    """
    Считает количество уникальных транзакций (signature) для указанного токена за конкретную дату.
    :param events_df: DataFrame с событиями (обязательные колонки: block_time, token_a_mint, token_b_mint, signature)
    :param token_address: адрес токена (str)
    :param date: дата (datetime.date или совместимый формат)
    :return: int, количество уникальных транзакций
    """
    day_events = events_df[(events_df['block_time'].dt.date == date) & (
        (events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)
    )]
    return day_events['signature'].nunique()

def calculate_unique_wallets(events_df: pd.DataFrame, token_address: str, date: Any) -> int:
    """
    Считает количество уникальных кошельков, взаимодействовавших с токеном за конкретную дату.
    Учитываются оба поля: from_wallet и to_wallet.
    :param events_df: DataFrame с событиями (обязательные колонки: block_time, token_a_mint, token_b_mint, from_wallet, to_wallet)
    :param token_address: адрес токена (str)
    :param date: дата (datetime.date или совместимый формат)
    :return: int, количество уникальных кошельков
    """
    day_events = events_df[(events_df['block_time'].dt.date == date) & (
        (events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)
    )]
    wallets = pd.concat([day_events['from_wallet'], day_events['to_wallet']]).unique()
    return len(wallets)

def calculate_price_change(events_df: pd.DataFrame, token_address: str, date: str) -> float:
    """
    Рассчитывает изменение цены за день для указанного токена.
    
    Args:
        events_df: DataFrame с событиями из ml_ready_events
        token_address: Адрес токена
        date: Дата (строка в формате YYYY-MM-DD)
        
    Returns:
        Процент изменения цены за день
    """
    try:
        # Фильтруем SWAP события по токену и дате
        filtered_events = events_df[
            ((events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)) &
            (events_df['event_type'] == 'SWAP') &
            (events_df['block_time'].dt.strftime('%Y-%m-%d') == date) &
            (events_df['to_amount'] > 0) & (events_df['from_amount'] > 0)
        ].copy()
        
        if filtered_events.empty:
            return 0.0
        
        # Рассчитываем цену для каждого SWAP
        filtered_events['price'] = np.where(
            filtered_events['token_a_mint'] == token_address,
            filtered_events['to_amount'] / filtered_events['from_amount'],
            filtered_events['from_amount'] / filtered_events['to_amount']
        )
        
        # Сортируем по времени
        filtered_events = filtered_events.sort_values('block_time')
        
        if len(filtered_events) < 2:
            return 0.0
        
        # Берем первую и последнюю цену дня
        first_price = filtered_events.iloc[0]['price']
        last_price = filtered_events.iloc[-1]['price']
        
        if first_price > 0:
            return ((last_price - first_price) / first_price) * 100
        
        return 0.0
    except Exception as e:
        logger.error(f"Ошибка при расчете изменения цены для токена {token_address} за {date}: {str(e)}")
        return 0.0

def calculate_buy_sell_ratio(events_df: pd.DataFrame, token_address: str, date: Any) -> float:
    """
    Рассчитывает соотношение количества покупок к продажам для токена за конкретную дату.
    Покупка — если токен является token_b_mint (его получают), продажа — если token_a_mint (его отдают).
    :param events_df: DataFrame с событиями (обязательные колонки: block_time, token_a_mint, token_b_mint)
    :param token_address: адрес токена (str)
    :param date: дата (datetime.date или совместимый формат)
    :return: float, отношение количества покупок к продажам (buy/sell). Если продаж нет, возвращает float('inf').
    """
    day_events = events_df[(events_df['block_time'].dt.date == date) & (
        (events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)
    )]
    buy_count = (day_events['token_b_mint'] == token_address).sum()
    sell_count = (day_events['token_a_mint'] == token_address).sum()
    if sell_count == 0:
        return float('inf') if buy_count > 0 else 0.0
    return buy_count / sell_count

def calculate_gini_coefficient(wallet_balances: list[float]) -> float:
    """
    Рассчитывает коэффициент Джини для списка балансов кошельков.
    0 = все владеют поровну, 1 = один владеет всем.
    :param wallet_balances: список балансов (List[float])
    :return: float, коэффициент Джини (0-1)
    """
    n = len(wallet_balances)
    if n == 0:
        return 0.0
    sorted_balances = sorted(wallet_balances)
    total = sum(sorted_balances)
    if total == 0:
        return 0.0
    cum = 0.0
    for i, x in enumerate(sorted_balances, 1):
        cum += i * x
    gini = (2 * cum) / (n * total) - (n + 1) / n
    return gini

def calculate_concentration_top5(wallet_balances: List[float]) -> float:
    """
    Рассчитывает концентрацию токенов в топ-5 кошельках (в процентах).
    
    Args:
        wallet_balances: Список балансов кошельков
        
    Returns:
        Процент токенов, сосредоточенных в топ-5 кошельках
    """
    try:
        # Фильтруем положительные балансы и сортируем по убыванию
        balances = sorted([float(b) for b in wallet_balances if b > 0], reverse=True)
        
        if not balances:
            return 0.0
        
        total_balance = sum(balances)
        if total_balance == 0:
            return 0.0
        
        # Берем топ-5 кошельков
        top5_balance = sum(balances[:5])
        
        return (top5_balance / total_balance) * 100
    except Exception as e:
        logger.error(f"Ошибка при расчете концентрации в топ-5 кошельках: {str(e)}")
        return 0.0

def calculate_concentration_top_N(wallet_balances: list[float], n: int = 5) -> float:
    """
    Рассчитывает, какой процент токенов сосредоточен у N крупнейших холдеров.
    :param wallet_balances: список балансов (List[float])
    :param n: количество топ-холдеров (по умолчанию 5)
    :return: float, доля токенов у N крупнейших холдеров (0-1)
    """
    if not wallet_balances:
        return 0.0
    sorted_balances = sorted(wallet_balances, reverse=True)
    total = sum(sorted_balances)
    if total == 0:
        return 0.0
    top_sum = sum(sorted_balances[:n])
    return top_sum / total

def calculate_external_to_internal_volume_ratio(events_df: pd.DataFrame, token_address: str, date: str, 
                                              internal_wallets: Optional[Set[str]] = None) -> float:
    """
    Рассчитывает соотношение объема торгов внешних трейдеров к внутренним.
    
    Args:
        events_df: DataFrame с событиями из ml_ready_events
        token_address: Адрес токена
        date: Дата (строка в формате YYYY-MM-DD)
        internal_wallets: Множество адресов внутренних кошельков
        
    Returns:
        Соотношение объема внешних к внутренним трейдерам
    """
    try:
        # Загружаем список внутренних кошельков если не передан
        if internal_wallets is None:
            internal_wallets = load_internal_wallets()
        
        # Фильтруем SWAP события по токену и дате
        filtered_events = events_df[
            ((events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)) &
            (events_df['event_type'] == 'SWAP') &
            (events_df['block_time'].dt.strftime('%Y-%m-%d') == date) &
            (events_df['from_amount'].notna())
        ]
        
        internal_volume = 0.0
        external_volume = 0.0
        
        for _, tx in filtered_events.iterrows():
            from_wallet = tx['from_wallet']
            to_wallet = tx['to_wallet']
            amount = float(tx['from_amount']) if tx['from_amount'] else 0.0
            
            # Классифицируем транзакцию по кошелькам
            if from_wallet in internal_wallets or to_wallet in internal_wallets:
                internal_volume += amount
            else:
                external_volume += amount
        
        # Рассчитываем соотношение
        if internal_volume > 0:
            return external_volume / internal_volume
        elif external_volume > 0:
            return float('inf')  # Только внешний объем
        else:
            return 0.0
            
    except Exception as e:
        logger.error(f"Ошибка при расчете соотношения объемов для токена {token_address} за {date}: {str(e)}")
        return 0.0

def calculate_external_traders_ratio_by_tag(events_df: pd.DataFrame, token_address: str, date: str) -> float:
    """
    Резервная функция для расчета доли внешних трейдеров по тегам кошельков.
    
    Args:
        events_df: DataFrame с событиями из ml_ready_events
        token_address: Адрес токена
        date: Дата (строка в формате YYYY-MM-DD)
        
    Returns:
        Доля внешних трейдеров (от 0 до 1)
    """
    try:
        # Фильтруем события по токену и дате
        filtered_events = events_df[
            ((events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)) &
            (events_df['block_time'].dt.strftime('%Y-%m-%d') == date)
        ]
        
        # Получаем все уникальные кошельки
        from_wallets = filtered_events['from_wallet'].dropna()
        to_wallets = filtered_events['to_wallet'].dropna()
        all_wallets = pd.concat([from_wallets, to_wallets]).unique()
        
        # Получаем кошельки с тегами (алгоритмические)
        tagged_from = filtered_events[filtered_events['wallet_tag'].notna()]['from_wallet'].dropna()
        tagged_to = filtered_events[filtered_events['wallet_tag'].notna()]['to_wallet'].dropna()
        tagged_wallets = pd.concat([tagged_from, tagged_to]).unique()
        
        total_wallets = len(all_wallets)
        tagged_wallets_count = len(tagged_wallets)
        
        if total_wallets > 0:
            # Рассчитываем долю внешних трейдеров
            external_traders = total_wallets - tagged_wallets_count
            return external_traders / total_wallets
        
        return 0.0
    except Exception as e:
        logger.error(f"Ошибка при расчете доли внешних трейдеров для токена {token_address} за {date}: {str(e)}")
        return 0.0

def calculate_liquidity_change_velocity(events_df: pd.DataFrame, token_address: str, date: str) -> float:
    """
    Рассчитывает скорость изменения ликвидности в пуле.
    Поскольку прямых операций ADD_LIQUIDITY/REMOVE_LIQUIDITY нет, используем 
    изменение объемов SWAP операций как прокси-метрику ликвидности.
    
    Args:
        events_df: DataFrame с событиями из ml_ready_events
        token_address: Адрес токена
        date: Дата (строка в формате YYYY-MM-DD)
        
    Returns:
        Скорость изменения ликвидности (первая производная объема по времени)
    """
    try:
        target_date = pd.to_datetime(date).date()
        prev_date = (pd.to_datetime(date) - pd.Timedelta(days=1)).date()
        
        # Получаем объемы за текущий и предыдущий день
        current_volume = calculate_daily_volume(events_df, token_address, date)
        
        # Для предыдущего дня фильтруем события
        prev_events = events_df[
            ((events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)) &
            (events_df['event_type'] == 'SWAP') &
            (events_df['block_time'].dt.date == prev_date)
        ]
        prev_volume = prev_events['from_amount'].fillna(0).sum()
        
        # Рассчитываем скорость изменения как разность объемов
        velocity = current_volume - float(prev_volume)
        return velocity
        
    except Exception as e:
        logger.error(f"Ошибка при расчете скорости изменения ликвидности для токена {token_address} за {date}: {str(e)}")
        return 0.0

def get_wallet_balances_for_token(events_df: pd.DataFrame, token_address: str, date: str) -> List[float]:
    """
    Вспомогательная функция для получения балансов кошельков по токену на указанную дату.
    Упрощенный расчет на основе SWAP операций.
    
    Args:
        events_df: DataFrame с событиями из ml_ready_events
        token_address: Адрес токена
        date: Дата (строка в формате YYYY-MM-DD)
        
    Returns:
        Список балансов кошельков
    """
    try:
        # Фильтруем SWAP события по токену до указанной даты включительно
        filtered_events = events_df[
            ((events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address)) &
            (events_df['event_type'] == 'SWAP') &
            (events_df['block_time'].dt.strftime('%Y-%m-%d') <= date)
        ]
        
        # Рассчитываем изменения балансов
        wallet_changes = {}
        
        for _, tx in filtered_events.iterrows():
            from_wallet = tx['from_wallet']
            to_wallet = tx['to_wallet']
            from_amount = float(tx['from_amount']) if tx['from_amount'] else 0.0
            to_amount = float(tx['to_amount']) if tx['to_amount'] else 0.0
            
            # Исходящие переводы (отрицательное изменение баланса)
            if from_wallet:
                if from_wallet not in wallet_changes:
                    wallet_changes[from_wallet] = 0.0
                wallet_changes[from_wallet] -= from_amount
            
            # Входящие переводы (положительное изменение баланса)
            if to_wallet:
                if to_wallet not in wallet_changes:
                    wallet_changes[to_wallet] = 0.0
                wallet_changes[to_wallet] += to_amount
        
        # Возвращаем только положительные балансы
        positive_balances = [balance for balance in wallet_changes.values() if balance > 0]
        return positive_balances
        
    except Exception as e:
        logger.error(f"Ошибка при получении балансов кошельков для токена {token_address} за {date}: {str(e)}")
        return []

def calculate_volatility(price_changes: List[float]) -> float:
    """
    Рассчитывает волатильность на основе списка изменений цен.
    
    Args:
        price_changes: Список процентных изменений цен
        
    Returns:
        Волатильность (стандартное отклонение изменений цен)
    """
    try:
        if not price_changes:
            return 0.0
        
        # Преобразуем в проценты и рассчитываем стандартное отклонение
        changes_array = np.array([abs(change) / 100 for change in price_changes])
        return float(np.std(changes_array))
        
    except Exception as e:
        logger.error(f"Ошибка при расчете волатильности: {str(e)}")
        return 0.0

def find_largest_sol_buy(events_df: pd.DataFrame, token_address: str) -> Dict[str, Any]:
    """
    Находит самую крупную покупку токена за SOL на основе семантического анализа.
    Использует enrichment_data.sol_trades для определения реальных покупок за SOL.
    
    Args:
        events_df: DataFrame с событиями из ml_ready_events
        token_address: Адрес токена
        
    Returns:
        Словарь с информацией о крупнейшей покупке за SOL
    """
    try:
        # Фильтруем события по токену
        token_events = events_df[
            ((events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address))
        ].copy()
        
        if token_events.empty:
            return {"error": "No events found for token"}
        
        largest_buy = None
        max_sol_amount = 0
        
        for _, event in token_events.iterrows():
            try:
                # Парсим enrichment_data
                enrichment_data = event.get('enrichment_data')
                if not enrichment_data:
                    continue
                
                # Ищем в строковом представлении или парсим JSON
                if isinstance(enrichment_data, str):
                    import json
                    try:
                        enrichment_data = json.loads(enrichment_data)
                    except:
                        continue
                
                sol_trades = enrichment_data.get('sol_trades', {})
                
                # Проверяем, является ли это покупкой за SOL
                if (sol_trades.get('trade_type') == 'BUY_WITH_SOL' and 
                    sol_trades.get('primary_token') == token_address):
                    
                    # Размер покупки в SOL (абсолютное значение)
                    sol_amount = abs(sol_trades.get('net_sol_change_ui', 0))
                    
                    if sol_amount > max_sol_amount:
                        max_sol_amount = sol_amount
                        largest_buy = {
                            'signature': event.get('signature'),
                            'fee_payer': sol_trades.get('fee_payer'),
                            'sol_amount': sol_amount,
                            'token_amount': sol_trades.get('token_amount_change', 0),
                            'block_time': event.get('block_time'),
                            'transaction_fee': sol_trades.get('transaction_fee', 0) / 1e9  # В SOL
                        }
                        
            except Exception as e:
                logger.debug(f"Ошибка при обработке события {event.get('signature', 'unknown')}: {str(e)}")
                continue
        
        if largest_buy:
            return largest_buy
        else:
            return {"error": "No SOL purchases found for this token"}
            
    except Exception as e:
        logger.error(f"Ошибка при поиске крупнейшей покупки за SOL для токена {token_address}: {str(e)}")
        return {"error": str(e)}

def find_largest_sol_sell(events_df: pd.DataFrame, token_address: str) -> Dict[str, Any]:
    """
    Находит самую крупную продажу токена за SOL на основе семантического анализа.
    Использует enrichment_data.sol_trades для определения реальных продаж за SOL.
    
    Args:
        events_df: DataFrame с событиями из ml_ready_events
        token_address: Адрес токена
        
    Returns:
        Словарь с информацией о крупнейшей продаже за SOL
    """
    try:
        # Фильтруем события по токену
        token_events = events_df[
            ((events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address))
        ].copy()
        
        if token_events.empty:
            return {"error": "No events found for token"}
        
        largest_sell = None
        max_sol_amount = 0
        
        for _, event in token_events.iterrows():
            try:
                # Парсим enrichment_data
                enrichment_data = event.get('enrichment_data')
                if not enrichment_data:
                    continue
                
                # Ищем в строковом представлении или парсим JSON
                if isinstance(enrichment_data, str):
                    import json
                    try:
                        enrichment_data = json.loads(enrichment_data)
                    except:
                        continue
                
                sol_trades = enrichment_data.get('sol_trades', {})
                
                # Проверяем, является ли это продажей за SOL
                if (sol_trades.get('trade_type') == 'SELL_FOR_SOL' and 
                    sol_trades.get('primary_token') == token_address):
                    
                    # Размер продажи в SOL (полученный SOL)
                    sol_amount = abs(sol_trades.get('net_sol_change_ui', 0))
                    
                    if sol_amount > max_sol_amount:
                        max_sol_amount = sol_amount
                        largest_sell = {
                            'signature': event.get('signature'),
                            'fee_payer': sol_trades.get('fee_payer'),
                            'sol_amount': sol_amount,
                            'token_amount': sol_trades.get('token_amount_change', 0),
                            'block_time': event.get('block_time'),
                            'transaction_fee': sol_trades.get('transaction_fee', 0) / 1e9  # В SOL
                        }
                        
            except Exception as e:
                logger.debug(f"Ошибка при обработке события {event.get('signature', 'unknown')}: {str(e)}")
                continue
        
        if largest_sell:
            return largest_sell
        else:
            return {"error": "No SOL sales found for this token"}
            
    except Exception as e:
        logger.error(f"Ошибка при поиске крупнейшей продажи за SOL для токена {token_address}: {str(e)}")
        return {"error": str(e)}

def analyze_sol_trading_pattern(events_df: pd.DataFrame, token_address: str) -> Dict[str, Any]:
    """
    Анализирует паттерны торговли за SOL для токена.
    
    Args:
        events_df: DataFrame с событиями из ml_ready_events
        token_address: Адрес токена
        
    Returns:
        Словарь с аналитикой торговли за SOL
    """
    try:
        # Получаем крупнейшие операции
        largest_buy = find_largest_sol_buy(events_df, token_address)
        largest_sell = find_largest_sol_sell(events_df, token_address)
        
        # Подсчитываем общую статистику SOL торгов
        token_events = events_df[
            ((events_df['token_a_mint'] == token_address) | (events_df['token_b_mint'] == token_address))
        ].copy()
        
        sol_buys = []
        sol_sells = []
        
        for _, event in token_events.iterrows():
            try:
                enrichment_data = event.get('enrichment_data')
                if isinstance(enrichment_data, str):
                    import json
                    enrichment_data = json.loads(enrichment_data)
                
                sol_trades = enrichment_data.get('sol_trades', {})
                trade_type = sol_trades.get('trade_type')
                
                if trade_type == 'BUY_WITH_SOL' and sol_trades.get('primary_token') == token_address:
                    sol_buys.append(abs(sol_trades.get('net_sol_change_ui', 0)))
                elif trade_type == 'SELL_FOR_SOL' and sol_trades.get('primary_token') == token_address:
                    sol_sells.append(abs(sol_trades.get('net_sol_change_ui', 0)))
                    
            except Exception:
                continue
        
        return {
            'largest_buy': largest_buy,
            'largest_sell': largest_sell,
            'total_sol_buys': len(sol_buys),
            'total_sol_sells': len(sol_sells),
            'total_sol_buy_volume': sum(sol_buys) if sol_buys else 0,
            'total_sol_sell_volume': sum(sol_sells) if sol_sells else 0,
            'avg_sol_buy_size': sum(sol_buys) / len(sol_buys) if sol_buys else 0,
            'avg_sol_sell_size': sum(sol_sells) / len(sol_sells) if sol_sells else 0,
            'buy_sell_ratio': len(sol_buys) / len(sol_sells) if len(sol_sells) > 0 else float('inf') if len(sol_buys) > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Ошибка при анализе паттернов SOL торговли для токена {token_address}: {str(e)}")
        return {"error": str(e)} 