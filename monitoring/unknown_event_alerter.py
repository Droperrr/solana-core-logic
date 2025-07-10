#!/usr/bin/env python3
"""
🚨 СИСТЕМА ОБНАРУЖЕНИЯ АНОМАЛЬНЫХ ПАТТЕРНОВ (СОАП)
🎯 Модуль: Алертинг Unknown Events в Реальном Времени

КРИТИЧЕСКАЯ ГИПОТЕЗА H-007:
Основным триггером скоординированных действий являются вызовы кастомных смарт-контрактов,
которые наша система классифицирует как UNKNOWN. Эти операции - источник настоящей "альфы".

АЛГОРИТМ ДЕТЕКЦИИ:
1. ✅ Сканирование новых UNKNOWN событий каждые 5 минут
2. ✅ Фильтрация по критериям повышенного интереса
3. ✅ Кросс-токен корреляция одинаковых program_id
4. ✅ Временной анализ близости к SWAP операциям
5. ✅ Мгновенная отправка критических алертов

РЕЗУЛЬТАТ: Превращение "шума" UNKNOWN в четкие торговые сигналы
"""

import sqlite3
import json
import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass
from collections import defaultdict
import logging
import os

# Настройка логирования
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/unknown_alerter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class UnknownEvent:
    """Структура для хранения данных UNKNOWN события."""
    signature: str
    program_id: str
    instruction_name: str
    unknown_classification: str
    unknown_reason: str
    block_time: int
    token_address: Optional[str]
    instruction_index: int
    details: Dict
    fingerprint: str

@dataclass 
class AlertCriteria:
    """Критерии для триггера алерта."""
    min_program_occurrences: int = 2  # Минимум вхождений program_id для алерта
    time_window_minutes: int = 15     # Временное окно для поиска корреляций
    swap_proximity_minutes: int = 10  # Близость к SWAP операциям
    valid_classifications: Set[str] = None
    
    def __post_init__(self):
        if self.valid_classifications is None:
            self.valid_classifications = {
                'valid_program',
                'defi_related', 
                'nft_related'
            }

class UnknownEventAlerter:
    """
    🚨 КРИТИЧЕСКИЙ КОМПОНЕНТ СОАП
    
    Мониторит UNKNOWN события и генерирует алерты при обнаружении
    потенциальных скрытых торговых сигналов.
    """
    
    def __init__(self, db_path: str = 'solana_db.sqlite'):
        self.db_path = db_path
        self.criteria = AlertCriteria()
        self.last_check_time = None
        self.alert_history = []
        
        # Инициализация состояния
        self._initialize_state()
        
    def _initialize_state(self):
        """Инициализирует состояние алертера."""
        try:
            with open('monitoring/alerter_state.json', 'r') as f:
                state = json.load(f)
                self.last_check_time = state.get('last_check_time')
        except (FileNotFoundError, json.JSONDecodeError):
            logger.info("🔄 Инициализация с нуля - previous state не найден")
            self.last_check_time = None
    
    def _save_state(self):
        """Сохраняет текущее состояние алертера."""
        os.makedirs('monitoring', exist_ok=True)
        
        state = {
            'last_check_time': self.last_check_time,
            'last_save': datetime.now().isoformat()
        }
        
        with open('monitoring/alerter_state.json', 'w') as f:
            json.dump(state, f, indent=2)
    
    def _create_fingerprint(self, program_id: str, instruction_data: Dict) -> str:
        """
        Создает уникальный отпечаток для UNKNOWN операции.
        
        Отпечаток = program_id + хэш ключевых данных инструкции
        Это позволяет находить одинаковые операции у разных токенов.
        """
        # Извлекаем ключевые элементы данных
        key_elements = []
        
        if isinstance(instruction_data, dict):
            # Берем первые несколько байт data если есть
            if 'data' in instruction_data:
                data = instruction_data['data']
                if isinstance(data, str) and len(data) > 8:
                    key_elements.append(data[:16])  # Первые 8 байт в hex
            
            # Добавляем количество аккаунтов
            if 'accounts' in instruction_data:
                accounts = instruction_data['accounts']
                if isinstance(accounts, list):
                    key_elements.append(f"acc_count:{len(accounts)}")
        
        # Создаем строку для хэширования
        fingerprint_str = f"{program_id}|{';'.join(key_elements)}"
        
        # Возвращаем короткий хэш
        return hashlib.sha256(fingerprint_str.encode()).hexdigest()[:16]
    
    def _get_new_unknown_events(self) -> List[UnknownEvent]:
        """
        Извлекает новые UNKNOWN события из базы данных.
        """
        conn = sqlite3.connect(self.db_path)
        
        # Определяем временной фильтр
        if self.last_check_time:
            time_filter = "AND e.created_at > ?"
            params = [self.last_check_time]
        else:
            # Первый запуск - берем события за последний час
            one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
            time_filter = "AND e.created_at > ?"
            params = [one_hour_ago]
        
        query = f"""
        SELECT 
            e.signature,
            e.program_id,
            e.instruction_name,
            e.unknown_classification,
            e.unknown_reason,
            e.block_time,
            e.token_address,
            e.instruction_index,
            e.details,
            e.created_at
        FROM enhanced_ml_events e
        WHERE e.event_type = 'UNKNOWN'
        {time_filter}
        ORDER BY e.created_at DESC
        """
        
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        events = []
        for row in rows:
            details = json.loads(row[8]) if row[8] else {}
            
            event = UnknownEvent(
                signature=row[0],
                program_id=row[1],
                instruction_name=row[2],
                unknown_classification=row[3],
                unknown_reason=row[4],
                block_time=row[5],
                token_address=row[6],
                instruction_index=row[7],
                details=details,
                fingerprint=self._create_fingerprint(row[1], details)
            )
            events.append(event)
        
        logger.info(f"🔍 Найдено {len(events)} новых UNKNOWN событий")
        return events
    
    def _analyze_cross_token_patterns(self, events: List[UnknownEvent]) -> Dict[str, List[UnknownEvent]]:
        """
        Анализирует паттерны UNKNOWN событий между токенами.
        
        Возвращает словарь: fingerprint -> список событий с этим отпечатком
        """
        fingerprint_groups = defaultdict(list)
        
        for event in events:
            # Фильтруем только интересные события
            if event.unknown_classification in self.criteria.valid_classifications:
                fingerprint_groups[event.fingerprint].append(event)
        
        # Фильтруем группы с достаточным количеством вхождений
        significant_patterns = {
            fp: events_list 
            for fp, events_list in fingerprint_groups.items()
            if len(events_list) >= self.criteria.min_program_occurrences
        }
        
        logger.info(f"📊 Найдено {len(significant_patterns)} значимых кросс-токен паттернов")
        return significant_patterns
    
    def _check_swap_proximity(self, unknown_event: UnknownEvent) -> bool:
        """
        Проверяет, происходил ли SWAP близко по времени к UNKNOWN событию.
        """
        conn = sqlite3.connect(self.db_path)
        
        # Ищем SWAP события в том же временном окне
        time_start = unknown_event.block_time - (self.criteria.swap_proximity_minutes * 60)
        time_end = unknown_event.block_time + (self.criteria.swap_proximity_minutes * 60)
        
        query = """
        SELECT COUNT(*)
        FROM enhanced_ml_events e
        WHERE e.event_type = 'SWAP'
        AND e.block_time BETWEEN ? AND ?
        AND (e.token_address = ? OR e.signature = ?)
        """
        
        cursor = conn.cursor()
        cursor.execute(query, [time_start, time_end, unknown_event.token_address, unknown_event.signature])
        swap_count = cursor.fetchone()[0]
        conn.close()
        
        return swap_count > 0
    
    def _generate_alert(self, pattern_fingerprint: str, events: List[UnknownEvent]) -> Dict:
        """
        Генерирует структурированный алерт для обнаруженного паттерна.
        """
        # Анализируем события в паттерне
        tokens_involved = set(e.token_address for e in events if e.token_address)
        programs_involved = set(e.program_id for e in events)
        time_span = max(e.block_time for e in events) - min(e.block_time for e in events)
        
        # Подсчитываем события с близкими SWAP
        swap_correlated_count = sum(1 for e in events if self._check_swap_proximity(e))
        
        # Определяем критичность алерта
        criticality = "HIGH" if swap_correlated_count >= 2 else "MEDIUM"
        if len(tokens_involved) >= 3 and swap_correlated_count >= 3:
            criticality = "CRITICAL"
        
        alert = {
            "alert_id": f"UNKNOWN_{pattern_fingerprint}_{int(time.time())}",
            "timestamp": datetime.now().isoformat(),
            "criticality": criticality,
            "pattern_fingerprint": pattern_fingerprint,
            "hypothesis": "H-007: Скрытый триггер в UNKNOWN операции",
            "summary": {
                "events_count": len(events),
                "tokens_involved": len(tokens_involved),
                "programs_involved": list(programs_involved),
                "time_span_minutes": time_span // 60,
                "swap_correlated_events": swap_correlated_count
            },
            "events": [
                {
                    "signature": e.signature,
                    "program_id": e.program_id,
                    "token": e.token_address,
                    "block_time": e.block_time,
                    "classification": e.unknown_classification,
                    "has_swap_proximity": self._check_swap_proximity(e)
                }
                for e in events
            ],
            "recommended_action": self._get_recommended_action(criticality, events),
            "solana_explorer_links": [
                f"https://solscan.io/tx/{e.signature}" for e in events
            ]
        }
        
        return alert
    
    def _get_recommended_action(self, criticality: str, events: List[UnknownEvent]) -> str:
        """Возвращает рекомендуемое действие в зависимости от критичности."""
        if criticality == "CRITICAL":
            return (
                "🚨 НЕМЕДЛЕННО: Ручной анализ транзакций. "
                "Возможен скоординированный триггер для множественных токенов. "
                "Проверить позиции и подготовиться к возможным движениям."
            )
        elif criticality == "HIGH":
            return (
                "⚠️ ПРИОРИТЕТ: Детальный анализ program_id и данных инструкций. "
                "Сравнить с историческими дампами. Мониторить аналогичные события."
            )
        else:
            return (
                "📋 СТАНДАРТ: Зафиксировать паттерн для дальнейшего анализа. "
                "Добавить в базу известных сигналов."
            )
    
    def _send_alert(self, alert: Dict):
        """
        Отправляет алерт (пока просто логирует и сохраняет в файл).
        В будущем можно добавить Telegram, email, Discord интеграцию.
        """
        alert_msg = (
            f"\n🚨 СОАП АЛЕРТ: {alert['criticality']}\n"
            f"📊 Паттерн: {alert['pattern_fingerprint']}\n"
            f"🎯 События: {alert['summary']['events_count']} у {alert['summary']['tokens_involved']} токенов\n"
            f"⏱️ Временной диапазон: {alert['summary']['time_span_minutes']} минут\n"
            f"💱 SWAP корреляций: {alert['summary']['swap_correlated_events']}\n"
            f"🔗 Программы: {', '.join(alert['summary']['programs_involved'])}\n"
            f"💡 Действие: {alert['recommended_action']}\n"
        )
        
        logger.warning(alert_msg)
        
        # Сохраняем алерт в файл
        os.makedirs('monitoring/alerts', exist_ok=True)
        
        alert_file = f"monitoring/alerts/alert_{alert['alert_id']}.json"
        with open(alert_file, 'w') as f:
            json.dump(alert, f, indent=2)
        
        self.alert_history.append(alert)
        logger.info(f"💾 Алерт сохранен: {alert_file}")
    
    def run_scan(self) -> Dict:
        """
        Основной метод сканирования UNKNOWN событий.
        """
        scan_start = datetime.now()
        logger.info(f"🔍 СОАП: Начало сканирования в {scan_start}")
        
        try:
            # 1. Получаем новые UNKNOWN события
            unknown_events = self._get_new_unknown_events()
            
            if not unknown_events:
                logger.info("✅ Новые UNKNOWN события не найдены")
                self._save_state()
                return {"status": "success", "events_found": 0, "alerts_generated": 0}
            
            # 2. Анализируем кросс-токен паттерны
            significant_patterns = self._analyze_cross_token_patterns(unknown_events)
            
            alerts_generated = 0
            
            # 3. Генерируем алерты для значимых паттернов
            for fingerprint, pattern_events in significant_patterns.items():
                alert = self._generate_alert(fingerprint, pattern_events)
                self._send_alert(alert)
                alerts_generated += 1
            
            # 4. Обновляем время последней проверки
            self.last_check_time = scan_start.isoformat()
            self._save_state()
            
            scan_result = {
                "status": "success",
                "scan_time": scan_start.isoformat(),
                "events_found": len(unknown_events),
                "patterns_detected": len(significant_patterns),
                "alerts_generated": alerts_generated,
                "scan_duration_seconds": (datetime.now() - scan_start).total_seconds()
            }
            
            logger.info(f"✅ СОАП: Сканирование завершено - {scan_result}")
            return scan_result
            
        except Exception as e:
            logger.error(f"❌ СОАП: Ошибка сканирования - {e}")
            import traceback
            traceback.print_exc()
            return {"status": "error", "error": str(e)}

def main():
    """Основная функция для запуска алертера."""
    print("🚨 СОАП: Система Обнаружения Аномальных Паттернов")
    print("🎯 Инициализация Unknown Event Alerter...")
    
    alerter = UnknownEventAlerter()
    result = alerter.run_scan()
    
    if result["status"] == "success":
        print(f"✅ Сканирование завершено успешно:")
        print(f"   📊 События: {result['events_found']}")
        print(f"   🔍 Паттерны: {result['patterns_detected']}")
        print(f"   🚨 Алерты: {result['alerts_generated']}")
    else:
        print(f"❌ Ошибка сканирования: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main() 