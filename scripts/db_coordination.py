#!/usr/bin/env python3
"""
Координатор доступа к базе данных
Управляет конкурентным доступом между различными процессами системы
"""

import sqlite3
import time
import os
import fcntl
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from contextlib import contextmanager
from enum import Enum

logger = logging.getLogger(__name__)

class ProcessType(Enum):
    """Типы процессов, работающих с БД"""
    DATA_COLLECTOR = "data_collector"
    RE_ENRICHMENT = "re_enrichment"
    ANALYSIS = "analysis"
    MIGRATION = "migration"

class DBCoordinator:
    """
    Координатор доступа к базе данных
    Использует файловые блокировки для координации между процессами
    """
    
    def __init__(self, db_path: str, lock_dir: str = "locks"):
        self.db_path = db_path
        self.lock_dir = Path(lock_dir)
        self.lock_dir.mkdir(exist_ok=True)
        
        # Файлы блокировок для разных операций
        self.lock_files = {
            ProcessType.DATA_COLLECTOR: self.lock_dir / "data_collector.lock",
            ProcessType.RE_ENRICHMENT: self.lock_dir / "re_enrichment.lock",
            ProcessType.ANALYSIS: self.lock_dir / "analysis.lock",
            ProcessType.MIGRATION: self.lock_dir / "migration.lock"
        }
        
    @contextmanager
    def acquire_lock(self, process_type: ProcessType, timeout: int = 60):
        """
        Получает эксклюзивную блокировку для указанного типа процесса
        """
        lock_file_path = self.lock_files[process_type]
        lock_file = None
        
        try:
            # Создаем lock файл
            lock_file = open(lock_file_path, 'w')
            
            # Пытаемся получить блокировку
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    
                    # Записываем информацию о процессе
                    lock_info = {
                        'process_type': process_type.value,
                        'pid': os.getpid(),
                        'start_time': time.time(),
                        'db_path': self.db_path
                    }
                    lock_file.write(json.dumps(lock_info))
                    lock_file.flush()
                    
                    logger.info(f"Получена блокировка для {process_type.value}")
                    yield lock_file
                    return
                    
                except BlockingIOError:
                    # Блокировка занята, ждем
                    time.sleep(1)
                    continue
                    
            raise TimeoutError(f"Не удалось получить блокировку для {process_type.value} за {timeout} секунд")
            
        finally:
            if lock_file:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()
                    # Удаляем lock файл
                    if lock_file_path.exists():
                        lock_file_path.unlink()
                except:
                    pass
    
    def check_active_processes(self) -> Dict[str, Any]:
        """
        Проверяет, какие процессы активно работают с БД
        """
        active = {}
        
        for process_type, lock_file in self.lock_files.items():
            if lock_file.exists():
                try:
                    with open(lock_file, 'r') as f:
                        info = json.loads(f.read())
                        active[process_type.value] = info
                except:
                    # Файл поврежден или пустой
                    pass
        
        return active
    
    def wait_for_safe_access(self, requesting_process: ProcessType, max_wait: int = 300) -> bool:
        """
        Ожидает безопасного доступа для указанного процесса
        Учитывает приоритеты и совместимость операций
        """
        logger.info(f"Ожидание безопасного доступа для {requesting_process.value}")
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            active = self.check_active_processes()
            
            if not active:
                logger.info("База данных свободна")
                return True
            
            # Проверяем совместимость операций
            can_proceed = self._check_compatibility(requesting_process, active)
            
            if can_proceed:
                logger.info(f"Доступ разрешен для {requesting_process.value}")
                return True
            
            logger.info(f"Ожидание... Активные процессы: {list(active.keys())}")
            time.sleep(5)
        
        logger.warning(f"Таймаут ожидания для {requesting_process.value}")
        return False
    
    def _check_compatibility(self, requesting: ProcessType, active: Dict[str, Any]) -> bool:
        """
        Проверяет совместимость операций
        """
        # Правила совместимости:
        # 1. Анализ может работать параллельно с чтением
        # 2. Сбор данных несовместим с переобработкой
        # 3. Миграции блокируют все
        
        if ProcessType.MIGRATION.value in active:
            # Миграции блокируют все
            return False
        
        if requesting == ProcessType.MIGRATION:
            # Миграция требует эксклюзивного доступа
            return len(active) == 0
        
        if requesting == ProcessType.DATA_COLLECTOR:
            # Сбор данных несовместим с переобработкой
            return ProcessType.RE_ENRICHMENT.value not in active
        
        if requesting == ProcessType.RE_ENRICHMENT:
            # Переобработка несовместима со сбором данных
            return ProcessType.DATA_COLLECTOR.value not in active
        
        if requesting == ProcessType.ANALYSIS:
            # Анализ может работать параллельно, если нет записей
            heavy_writers = [ProcessType.DATA_COLLECTOR.value, ProcessType.RE_ENRICHMENT.value]
            return not any(writer in active for writer in heavy_writers)
        
        return True

def get_coordinated_connection(
    db_path: str, 
    process_type: ProcessType,
    enable_wal: bool = True,
    timeout: int = 30
) -> sqlite3.Connection:
    """
    Получает координированное соединение с базой данных
    """
    coordinator = DBCoordinator(db_path)
    
    # Ожидаем безопасного доступа
    if not coordinator.wait_for_safe_access(process_type):
        raise TimeoutError(f"Не удалось получить доступ к БД для {process_type.value}")
    
    # Подключаемся к БД
    conn = sqlite3.connect(db_path, timeout=timeout)
    
    if enable_wal:
        # Настраиваем оптимизации
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
    
    return conn

@contextmanager
def coordinated_db_access(
    db_path: str,
    process_type: ProcessType,
    enable_wal: bool = True,
    lock_timeout: int = 60
):
    """
    Контекстный менеджер для координированного доступа к БД
    """
    coordinator = DBCoordinator(db_path)
    
    with coordinator.acquire_lock(process_type, timeout=lock_timeout):
        conn = get_coordinated_connection(db_path, process_type, enable_wal)
        try:
            yield conn
        finally:
            conn.close()

def check_db_status(db_path: str) -> Dict[str, Any]:
    """
    Проверяет текущий статус базы данных
    """
    coordinator = DBCoordinator(db_path)
    active_processes = coordinator.check_active_processes()
    
    status = {
        'db_path': db_path,
        'db_exists': os.path.exists(db_path),
        'active_processes': active_processes,
        'is_locked': len(active_processes) > 0,
        'timestamp': time.time()
    }
    
    return status

if __name__ == "__main__":
    # Пример использования
    import argparse
    
    parser = argparse.ArgumentParser(description="Координатор доступа к базе данных")
    parser.add_argument('--db-path', required=True, help='Путь к базе данных')
    parser.add_argument('--check-status', action='store_true', help='Проверить статус БД')
    parser.add_argument('--wait-unlock', action='store_true', help='Ожидать освобождения БД')
    
    args = parser.parse_args()
    
    if args.check_status:
        status = check_db_status(args.db_path)
        print(json.dumps(status, indent=2))
    
    if args.wait_unlock:
        coordinator = DBCoordinator(args.db_path)
        if coordinator.wait_for_safe_access(ProcessType.ANALYSIS):
            print("База данных доступна")
        else:
            print("Таймаут ожидания") 