#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Фаза 3: Анализ аномалий в feature store
"""

import sqlite3
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_feature_store_data(db_path="db/solana_db.sqlite"):
    """Загружает данные из feature_store_daily"""
    logger.info("Загружаем данные из feature_store_daily...")
    
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT 
        token_address,
        date,
        volume_24h,
        transaction_count_24h,
        unique_wallets_24h,
        price_change_pct_24h,
        volatility_24h,
        buy_sell_ratio_24h,
        gini_coefficient,
        concentration_top5_pct,
        external_to_internal_volume_ratio,
        liquidity_change_velocity,
        largest_sol_buy_amount,
        largest_sol_sell_amount,
        total_sol_buy_volume,
        total_sol_sell_volume,
        sol_buy_sell_ratio,
        created_at
    FROM feature_store_daily
    WHERE volume_24h IS NOT NULL 
    ORDER BY date DESC, token_address
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    logger.info(f"Загружено {len(df)} записей из feature store")
    return df

def analyze_anomalies(df):
    """Анализ аномалий в данных"""
    print("ФАЗА 3: ПОИСК АНОМАЛИЙ В FEATURE STORE")
    print("=" * 60)
    
    if len(df) == 0:
        print("Нет данных для анализа")
        return
        
    print(f"Анализируем {len(df)} записей")
    print(f"Уникальных токенов: {df['token_address'].nunique()}")
    print(f"Период: {df['date'].min()} - {df['date'].max()}")
    
    # Статистика по основным признакам
    print("\nСТАТИСТИКА ПО ПРИЗНАКАМ:")
    print("-" * 40)
    
    key_features = ['volume_24h', 'transaction_count_24h', 'unique_wallets_24h', 
                   'price_change_pct_24h', 'largest_sol_sell_amount']
    
    for feature in key_features:
        if feature in df.columns:
            stats = df[feature].describe()
            print(f"{feature}:")
            print(f"  Среднее: {stats['mean']:.4f}")
            print(f"  Медиана: {stats['50%']:.4f}")
            print(f"  Максимум: {stats['max']:.4f}")
            print(f"  95-й процентиль: {df[feature].quantile(0.95):.4f}")
            print()
    
    # Поиск аномальных дней
    print("ПОИСК АНОМАЛЬНЫХ ДНЕЙ:")
    print("-" * 40)
    
    # Высокие объемы
    if 'volume_24h' in df.columns:
        high_volume_threshold = df['volume_24h'].quantile(0.95)
        high_volume_days = df[df['volume_24h'] > high_volume_threshold]
        print(f"Дней с высоким объемом (>95%): {len(high_volume_days)}")
        
        if len(high_volume_days) > 0:
            top_volume = high_volume_days.nlargest(3, 'volume_24h')
            print("  Топ-3 по объему:")
            for _, row in top_volume.iterrows():
                token_short = row['token_address'][:15] + "..."
                print(f"    {token_short} [{row['date']}]: {row['volume_24h']:.2f}")
    
    # Высокая активность транзакций
    if 'transaction_count_24h' in df.columns:
        high_tx_threshold = df['transaction_count_24h'].quantile(0.95)
        high_tx_days = df[df['transaction_count_24h'] > high_tx_threshold]
        print(f"\nДней с высокой активностью (>95%): {len(high_tx_days)}")
        
        if len(high_tx_days) > 0:
            top_tx = high_tx_days.nlargest(3, 'transaction_count_24h')
            print("  Топ-3 по транзакциям:")
            for _, row in top_tx.iterrows():
                token_short = row['token_address'][:15] + "..."
                print(f"    {token_short} [{row['date']}]: {row['transaction_count_24h']}")
    
    # Крупные SOL продажи (потенциальные дампы)
    if 'largest_sol_sell_amount' in df.columns:
        big_sells = df[df['largest_sol_sell_amount'] > 0.1]  # > 0.1 SOL
        print(f"\nДней с крупными SOL продажами (>0.1): {len(big_sells)}")
        
        if len(big_sells) > 0:
            top_sells = big_sells.nlargest(5, 'largest_sol_sell_amount')
            print("  Топ-5 крупнейших продаж:")
            for _, row in top_sells.iterrows():
                token_short = row['token_address'][:15] + "..."
                print(f"    {token_short} [{row['date']}]: {row['largest_sol_sell_amount']:.6f} SOL")
    
    # Аномальная концентрация
    if 'concentration_top5_pct' in df.columns:
        high_concentration = df[df['concentration_top5_pct'] > 80]
        print(f"\nДней с высокой концентрацией (>80%): {len(high_concentration)}")
        
        if len(high_concentration) > 0:
            top_concentration = high_concentration.nlargest(3, 'concentration_top5_pct')
            print("  Топ-3 по концентрации:")
            for _, row in top_concentration.iterrows():
                token_short = row['token_address'][:15] + "..."
                print(f"    {token_short} [{row['date']}]: {row['concentration_top5_pct']:.1f}%")
    
    print("\nАнализ аномалий завершен")

def main():
    """Основная функция"""
    try:
        df = load_feature_store_data()
        analyze_anomalies(df)
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 