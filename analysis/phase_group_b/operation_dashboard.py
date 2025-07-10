#!/usr/bin/env python3
"""
🚀 ОПЕРАЦИЯ: АДАПТАЦИЯ - Командная Dashboard
Красивый интерфейс для отслеживания всех фаз операции
"""

import os
import sys
import sqlite3
import json
from datetime import datetime
from typing import Dict, List

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

class OperationAdaptationDashboard:
    """Dashboard для мониторинга Операции: Адаптация"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.tokens = self._load_tokens()
        
    def _load_tokens(self) -> List[str]:
        """Загружает токены Группы Б"""
        try:
            with open("data/group_b_tokens.txt", 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []
    
    def get_phase_status(self) -> Dict:
        """Определяет статус каждой фазы операции"""
        
        # Фаза 1: Data Acquisition
        phase1_status = self._check_data_acquisition()
        
        # Фаза 2: Pattern Discovery  
        phase2_status = self._check_pattern_discovery()
        
        # Фаза 3: Model Generalization
        phase3_status = self._check_model_generalization()
        
        # Фаза 4: Backtesting
        phase4_status = self._check_backtesting()
        
        return {
            'phase1': phase1_status,
            'phase2': phase2_status, 
            'phase3': phase3_status,
            'phase4': phase4_status
        }
    
    def _check_data_acquisition(self) -> Dict:
        """Проверяет статус сбора данных"""
        
        # Проверка файла токенов
        tokens_ready = len(self.tokens) == 20
        
        # Проверка БД
        db_exists = os.path.exists("solana_db.sqlite")
        
        if not db_exists:
            return {
                'status': 'NOT_STARTED',
                'progress': 0,
                'details': 'База данных не создана',
                'next_action': 'Ожидание запуска сбора данных'
            }
        
        # Подсчет собранных данных
        try:
            conn = sqlite3.connect("solana_db.sqlite")
            cursor = conn.cursor()
            
            tokens_collected = 0
            total_transactions = 0
            
            for token in self.tokens:
                cursor.execute("""
                    SELECT COUNT(*) FROM transactions 
                    WHERE token_a_mint = ? OR token_b_mint = ?
                """, (token, token))
                count = cursor.fetchone()[0]
                if count > 0:
                    tokens_collected += 1
                    total_transactions += count
            
            conn.close()
            
            progress = (tokens_collected / len(self.tokens)) * 100
            
            if progress == 0:
                status = 'IN_PROGRESS'
                details = 'Сбор данных запущен, ожидание первых результатов'
            elif progress < 100:
                status = 'IN_PROGRESS'
                details = f'{tokens_collected}/{len(self.tokens)} токенов, {total_transactions:,} транзакций'
            else:
                status = 'COMPLETED'
                details = f'Все {len(self.tokens)} токенов собраны, {total_transactions:,} транзакций'
            
            return {
                'status': status,
                'progress': progress,
                'details': details,
                'next_action': 'Ожидание завершения' if status == 'IN_PROGRESS' else 'Переход к анализу'
            }
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'progress': 0,
                'details': f'Ошибка БД: {str(e)}',
                'next_action': 'Проверить процесс сбора'
            }
    
    def _check_pattern_discovery(self) -> Dict:
        """Проверяет статус анализа паттернов"""
        
        # Проверка результатов анализа
        pattern_files = [
            'output/group_b_entry_pattern_analysis_*.json'
        ]
        
        # Проверяем готовность данных
        phase1_ready = self._check_data_acquisition()['status'] == 'COMPLETED'
        
        if not phase1_ready:
            return {
                'status': 'WAITING',
                'progress': 0,
                'details': 'Ожидание завершения сбора данных',
                'next_action': 'Дождаться Phase 1'
            }
        
        # Проверяем наличие анализа
        analysis_exists = any(os.path.exists(f.replace('*', '')) for f in pattern_files)
        
        if not analysis_exists:
            return {
                'status': 'READY',
                'progress': 0,
                'details': 'Данные готовы, анализ не запущен',
                'next_action': 'Запустить validate_entry_pattern.py'
            }
        
        return {
            'status': 'IN_PROGRESS',
            'progress': 50,
            'details': 'Анализ паттернов выполняется',
            'next_action': 'Дождаться результатов'
        }
    
    def _check_model_generalization(self) -> Dict:
        """Проверяет статус универсализации модели"""
        
        # Проверяем конфигурации
        config_files = [
            'configs/triggers/group_a_trigger.json',
            'configs/triggers/group_b_trigger.json'
        ]
        
        configs_ready = all(os.path.exists(f) for f in config_files)
        
        if not configs_ready:
            return {
                'status': 'READY',
                'progress': 30,
                'details': 'Частичная готовность конфигураций',
                'next_action': 'Завершить создание конфигураций'
            }
        
        return {
            'status': 'IN_PROGRESS',
            'progress': 70,
            'details': 'Конфигурации созданы',
            'next_action': 'Интегрировать с финальной моделью'
        }
    
    def _check_backtesting(self) -> Dict:
        """Проверяет готовность к бэктестингу"""
        
        return {
            'status': 'PENDING',
            'progress': 0,
            'details': 'Ожидание завершения предыдущих фаз',
            'next_action': 'Дождаться Phase 2 и 3'
        }
    
    def print_beautiful_dashboard(self):
        """Выводит красивую dashboard"""
        
        phases = self.get_phase_status()
        
        # Заголовок
        print("\n" + "█"*80)
        print("🚀 ОПЕРАЦИЯ: АДАПТАЦИЯ - КОМАНДНАЯ DASHBOARD")
        print("█"*80)
        print(f"⏰ Время запуска: {self.start_time.strftime('%H:%M:%S')}")
        print(f"🎯 Цель: Анализ Группы Б ({len(self.tokens)} токенов)")
        print("█"*80)
        
        # Статус фаз
        phase_info = {
            'phase1': ('📥 ФАЗА 1: СБОР ДАННЫХ', 'Data Acquisition & Verification'),
            'phase2': ('🔍 ФАЗА 2: АНАЛИЗ ПАТТЕРНОВ', 'Pattern Discovery'),
            'phase3': ('⚙️ ФАЗА 3: УНИВЕРСАЛИЗАЦИЯ', 'Model Generalization'),
            'phase4': ('📊 ФАЗА 4: БЭКТЕСТИНГ', 'Backtesting & Trading Prep')
        }
        
        for phase_key, (title, subtitle) in phase_info.items():
            phase = phases[phase_key]
            status_emoji = {
                'NOT_STARTED': '⏳',
                'WAITING': '⏸️',
                'READY': '✅',
                'IN_PROGRESS': '⚙️',
                'COMPLETED': '✅',
                'ERROR': '❌',
                'PENDING': '⏳'
            }
            
            emoji = status_emoji.get(phase['status'], '❓')
            progress_bar = self._create_progress_bar(phase['progress'])
            
            print(f"\n{title}")
            print(f"   {subtitle}")
            print(f"   Статус: {emoji} {phase['status']} | {progress_bar} {phase['progress']:.0f}%")
            print(f"   Детали: {phase['details']}")
            print(f"   Действие: {phase['next_action']}")
        
        # Общий прогресс
        overall_progress = sum(p['progress'] for p in phases.values()) / 4
        overall_bar = self._create_progress_bar(overall_progress)
        
        print("\n" + "█"*80)
        print(f"📈 ОБЩИЙ ПРОГРЕСС ОПЕРАЦИИ: {overall_bar} {overall_progress:.1f}%")
        
        # Рекомендации
        print(f"\n💡 СЛЕДУЮЩИЕ ШАГИ:")
        ready_actions = [phase['next_action'] for phase in phases.values() 
                        if 'Запустить' in phase['next_action']]
        
        if ready_actions:
            for i, action in enumerate(ready_actions, 1):
                print(f"   {i}. {action}")
        else:
            print("   ⏳ Дождаться завершения текущих процессов")
        
        print("█"*80)
    
    def _create_progress_bar(self, progress: float, length: int = 20) -> str:
        """Создает прогресс-бар"""
        filled = int(length * progress / 100)
        bar = '█' * filled + '░' * (length - filled)
        return f"[{bar}]"
    
    def save_status_report(self):
        """Сохраняет отчет о статусе операции"""
        phases = self.get_phase_status()
        
        report = {
            'operation': 'АДАПТАЦИЯ',
            'timestamp': datetime.now().isoformat(),
            'start_time': self.start_time.isoformat(),
            'tokens_count': len(self.tokens),
            'phases': phases,
            'overall_progress': sum(p['progress'] for p in phases.values()) / 4
        }
        
        os.makedirs('output', exist_ok=True)
        filename = f"output/operation_adaptation_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Отчет сохранен: {filename}")

def main():
    """Главная функция dashboard"""
    dashboard = OperationAdaptationDashboard()
    
    # Выводим dashboard
    dashboard.print_beautiful_dashboard()
    
    # Сохраняем отчет
    dashboard.save_status_report()

if __name__ == "__main__":
    main() 