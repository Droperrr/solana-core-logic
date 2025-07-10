#!/usr/bin/env python3
"""
Скрипт для валидации паттерна "первые 3 покупки" для Группы Б.

Анализирует первые 3 SOL->token транзакции для каждого токена из group_b_tokens.txt
и проверяет гипотезу о константной сумме SOL как идентификационном маркере.
"""

import os
import sys
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("group_b_entry_pattern")

@dataclass
class FirstBuyEvent:
    """Структура для хранения данных о первой покупке"""
    token_address: str
    signature: str
    timestamp: int
    sol_amount: float
    buyer_wallet: str
    rank: int  # 1, 2, или 3 для первых трех покупок

@dataclass
class TokenAnalysis:
    """Результат анализа токена"""
    token_address: str
    first_buys: List[FirstBuyEvent]
    total_sol_first_3: float
    unique_buyers: int
    is_valid_pattern: bool
    notes: str

class GroupBEntryPatternValidator:
    """Анализатор паттернов входа для Группы Б"""
    
    def __init__(self, db_path: str = "solana_db.sqlite"):
        self.db_path = db_path
        self.results: List[TokenAnalysis] = []
    
    def load_group_b_tokens(self, tokens_file: str = "data/group_b_tokens.txt") -> List[str]:
        """Загружает список токенов Группы Б"""
        try:
            with open(tokens_file, 'r') as f:
                tokens = [line.strip() for line in f if line.strip()]
            logger.info(f"Загружено {len(tokens)} токенов Группы Б из {tokens_file}")
            return tokens
        except FileNotFoundError:
            logger.error(f"Файл {tokens_file} не найден")
            return []
    
    def get_first_3_sol_buys(self, token_address: str) -> List[FirstBuyEvent]:
        """
        Находит первые 3 SOL->token покупки для указанного токена
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {str(e)}")
            return []
        
        # Запрос для поиска SOL->token операций через enrichment_data
        query = """
        SELECT 
            signature,
            timestamp,
            enrichment_data
        FROM ml_ready_events 
        WHERE (token_a_mint = ? OR token_b_mint = ?)
        AND enrichment_data IS NOT NULL
        AND enrichment_data != ''
        ORDER BY timestamp ASC
        """
        
        try:
            cursor.execute(query, (token_address, token_address))
            events = cursor.fetchall()
            
            first_buys = []
            processed_sigs = set()
            
            for signature, timestamp, enrichment_data in events:
                if len(first_buys) >= 3:
                    break
                    
                if signature in processed_sigs:
                    continue
                    
                try:
                    # Парсим enrichment_data
                    if isinstance(enrichment_data, str):
                        enrichment_data = json.loads(enrichment_data)
                    
                    sol_trades = enrichment_data.get('sol_trades', {})
                    
                    # Проверяем, что это покупка токена за SOL
                    if (sol_trades.get('trade_type') == 'BUY_WITH_SOL' and 
                        sol_trades.get('primary_token') == token_address):
                        
                        sol_amount = abs(sol_trades.get('net_sol_change_ui', 0))
                        buyer_wallet = sol_trades.get('fee_payer', 'unknown')
                        
                        if sol_amount > 0:  # Только значимые покупки
                            first_buy = FirstBuyEvent(
                                token_address=token_address,
                                signature=signature,
                                timestamp=timestamp,
                                sol_amount=sol_amount,
                                buyer_wallet=buyer_wallet,
                                rank=len(first_buys) + 1
                            )
                            first_buys.append(first_buy)
                            processed_sigs.add(signature)
                            
                            logger.debug(f"Найдена покупка #{len(first_buys)} для {token_address}: "
                                       f"{sol_amount:.6f} SOL от {buyer_wallet}")
                    
                except Exception as e:
                    logger.debug(f"Ошибка при обработке {signature}: {str(e)}")
                    continue
            
        finally:
            if 'conn' in locals():
                conn.close()
        
        return first_buys
    
    def analyze_token(self, token_address: str) -> TokenAnalysis:
        """Анализирует паттерн входа для конкретного токена"""
        logger.info(f"Анализ паттерна входа для токена: {token_address}")
        
        try:
            first_buys = self.get_first_3_sol_buys(token_address)
        except Exception as e:
            logger.error(f"Ошибка при анализе токена {token_address}: {str(e)}")
            first_buys = []
        
        # Вычисляем метрики
        total_sol = sum(buy.sol_amount for buy in first_buys)
        unique_buyers = len(set(buy.buyer_wallet for buy in first_buys))
        
        # Определяем валидность паттерна
        is_valid = len(first_buys) >= 3
        notes = []
        
        if len(first_buys) < 3:
            notes.append(f"Найдено только {len(first_buys)} покупок из 3")
        
        if unique_buyers < 3:
            notes.append(f"Только {unique_buyers} уникальных покупателей")
        
        # Проверяем на подозрительно одинаковые суммы
        amounts = [buy.sol_amount for buy in first_buys]
        if len(set(f"{amt:.6f}" for amt in amounts)) == 1:
            notes.append("Подозрительно одинаковые суммы покупок")
        
        analysis = TokenAnalysis(
            token_address=token_address,
            first_buys=first_buys,
            total_sol_first_3=total_sol,
            unique_buyers=unique_buyers,
            is_valid_pattern=is_valid,
            notes="; ".join(notes) if notes else "OK"
        )
        
        return analysis
    
    def validate_group_pattern(self) -> Dict:
        """
        Валидирует паттерн для всей группы и ищет общие характеристики
        """
        tokens = self.load_group_b_tokens()
        
        if not tokens:
            logger.error("Не удалось загрузить токены для анализа")
            return {}
        
        # Анализируем каждый токен
        for token in tokens:
            analysis = self.analyze_token(token)
            self.results.append(analysis)
        
        # Анализ результатов группы
        valid_tokens = [r for r in self.results if r.is_valid_pattern]
        
        if not valid_tokens:
            logger.warning("Ни один токен не имеет валидного паттерна первых 3 покупок")
            return self._generate_report()
        
        # Анализ сумм SOL
        total_sols = [r.total_sol_first_3 for r in valid_tokens]
        
        group_stats = {
            'total_tokens_analyzed': len(tokens),
            'valid_pattern_tokens': len(valid_tokens),
            'sol_amounts': total_sols,
            'mean_sol_amount': sum(total_sols) / len(total_sols) if total_sols else 0,
            'min_sol_amount': min(total_sols) if total_sols else 0,
            'max_sol_amount': max(total_sols) if total_sols else 0,
            'unique_sol_amounts': len(set(f"{amt:.6f}" for amt in total_sols)),
        }
        
        # Проверяем константность сумм (с допуском 0.1%)
        if group_stats['unique_sol_amounts'] == 1:
            group_stats['pattern_type'] = "EXACT_CONSTANT"
            group_stats['constant_value'] = total_sols[0]
        elif group_stats['max_sol_amount'] - group_stats['min_sol_amount'] < group_stats['mean_sol_amount'] * 0.001:
            group_stats['pattern_type'] = "NEAR_CONSTANT"
            group_stats['constant_value'] = group_stats['mean_sol_amount']
        else:
            group_stats['pattern_type'] = "VARIABLE"
            group_stats['constant_value'] = None
        
        # Анализ кошельков
        all_buyers = []
        for result in valid_tokens:
            for buy in result.first_buys:
                all_buyers.append(buy.buyer_wallet)
        
        unique_buyers = set(all_buyers)
        buyer_frequency = defaultdict(int)
        for buyer in all_buyers:
            buyer_frequency[buyer] += 1
        
        group_stats['total_buyer_events'] = len(all_buyers)
        group_stats['unique_buyers'] = len(unique_buyers)
        group_stats['recurring_buyers'] = len([b for b, count in buyer_frequency.items() if count > 1])
        
        return self._generate_report(group_stats)
    
    def _generate_report(self, group_stats: Optional[Dict] = None) -> Dict:
        """Генерирует итоговый отчет анализа"""
        timestamp = datetime.now().isoformat()
        
        report = {
            'analysis_timestamp': timestamp,
            'analysis_type': 'Group B Entry Pattern Validation',
            'tokens_analyzed': len(self.results),
            'individual_results': []
        }
        
        # Добавляем результаты по каждому токену
        for result in self.results:
            token_report = {
                'token_address': result.token_address,
                'first_buys_found': len(result.first_buys),
                'total_sol_first_3': result.total_sol_first_3,
                'unique_buyers': result.unique_buyers,
                'is_valid_pattern': result.is_valid_pattern,
                'notes': result.notes,
                'first_buys_details': [
                    {
                        'rank': buy.rank,
                        'signature': buy.signature,
                        'timestamp': buy.timestamp,
                        'sol_amount': buy.sol_amount,
                        'buyer_wallet': buy.buyer_wallet
                    }
                    for buy in result.first_buys
                ]
            }
            report['individual_results'].append(token_report)
        
        # Добавляем групповую статистику если есть
        if group_stats:
            report['group_analysis'] = group_stats
            
            # Делаем выводы
            if group_stats.get('pattern_type') == 'EXACT_CONSTANT':
                report['conclusion'] = f"✅ ПОДТВЕРЖДЕНО: Группа Б использует константную сумму {group_stats['constant_value']:.6f} SOL для первых 3 покупок"
            elif group_stats.get('pattern_type') == 'NEAR_CONSTANT':
                report['conclusion'] = f"⚠️ ЧАСТИЧНО ПОДТВЕРЖДЕНО: Группа Б использует примерно константную сумму ~{group_stats['constant_value']:.6f} SOL"
            else:
                report['conclusion'] = "❌ НЕ ПОДТВЕРЖДЕНО: Суммы первых 3 покупок варьируются значительно"
        
        return report
    
    def save_report(self, report: Dict, filename: str = None):
        """Сохраняет отчет в файл"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"output/group_b_entry_pattern_analysis_{timestamp}.json"
        
        # Создаем директорию если нужно
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Отчет сохранен в {filename}")


def main():
    """Главная функция для запуска анализа"""
    validator = GroupBEntryPatternValidator()
    
    logger.info("🚀 Запуск анализа паттерна входа для Группы Б")
    
    # Выполняем валидацию
    report = validator.validate_group_pattern()
    
    # Сохраняем отчет
    validator.save_report(report)
    
    # Выводим краткие результаты
    print("\n" + "="*60)
    print("📊 РЕЗУЛЬТАТЫ АНАЛИЗА ГРУППЫ Б")
    print("="*60)
    
    if 'group_analysis' in report:
        stats = report['group_analysis']
        print(f"Токенов проанализировано: {stats['total_tokens_analyzed']}")
        print(f"Токенов с валидным паттерном: {stats['valid_pattern_tokens']}")
        
        if stats['valid_pattern_tokens'] > 0:
            print(f"Средняя сумма первых 3 покупок: {stats['mean_sol_amount']:.6f} SOL")
            print(f"Диапазон: {stats['min_sol_amount']:.6f} - {stats['max_sol_amount']:.6f} SOL")
            print(f"Уникальных кошельков: {stats['unique_buyers']}")
            print(f"Повторяющихся кошельков: {stats['recurring_buyers']}")
    
    if 'conclusion' in report:
        print(f"\n🎯 ЗАКЛЮЧЕНИЕ:")
        print(report['conclusion'])
    
    print("\n" + "="*60)
    
    return report

if __name__ == "__main__":
    main() 