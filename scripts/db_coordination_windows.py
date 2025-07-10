#!/usr/bin/env python3
"""
Координатор доступа к базе данных (Windows-совместимая версия)
Управляет конкурентным доступом между различными процессами системы
"""

import sqlite3
import time
import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from contextlib import contextmanager
from enum import Enum
import msvcrt
import threading

logger = logging.getLogger(__name__)

class ProcessType(Enum):
    """Типы процессов, работающих с БД"""
    DATA_COLLECTOR = "data_collector"
    RE_ENRICHMENT = "re_enrichment"
    ANALYSIS = "analysis"
    MIGRATION = "migration"

class WindowsDBCoordinator:
    """
    Windows-совместимый координатор доступа к базе данных
    Использует файловые блокировки Windows для координации между процессами
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
        
    def _try_acquire_file_lock(self, lock_file_path: Path, timeout: int = 60) -> Optional[Any]:
        """
        Пытается получить эксклюзивную блокировку файла на Windows
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Пытаемся создать файл эксклюзивно
                if not lock_file_path.exists():
                    lock_file = open(lock_file_path, 'w')
                    
                    # Записываем информацию о процессе
                    lock_info = {
                        'pid': os.getpid(),
                        'start_time': time.time(),
                        'db_path': self.db_path
                    }
                    lock_file.write(json.dumps(lock_info))
                    lock_file.flush()
                    
                    return lock_file
                else:
                    # Проверяем, не "мертвый" ли это lock
                    if self._is_stale_lock(lock_file_path):
                        # Удаляем устаревший lock
                        try:
                            lock_file_path.unlink()
                            continue
                        except:
                            pass
                    
                    # Ждем освобождения
                    time.sleep(1)
                    continue
                    
            except (OSError, PermissionError):
                # Файл занят другим процессом
                time.sleep(1)
                continue
                
        return None
    
    def _is_stale_lock(self, lock_file_path: Path, max_age: int = 3600) -> bool:
        """
        Проверяет, является ли блокировка устаревшей (процесс завершился)
        """
        try:
            if not lock_file_path.exists():
                return True
                
            # Проверяем возраст файла
            file_age = time.time() - lock_file_path.stat().st_mtime
            if file_age > max_age:
                return True
                
            # Проверяем, существует ли процесс
            with open(lock_file_path, 'r') as f:
                info = json.loads(f.read())
                pid = info.get('pid')
                
                if pid:
                    try:
                        # На Windows проверяем существование процесса
                        import psutil
                        return not psutil.pid_exists(pid)
                    except ImportError:
                        # Если psutil недоступен, используем возраст файла
                        return file_age > 300  # 5 минут
                        
        except Exception:
            # Если не можем прочитать - считаем устаревшим
            return True
            
        return False
    
    @contextmanager
    def acquire_lock(self, process_type: ProcessType, timeout: int = 60):
        """
        Получает эксклюзивную блокировку для указанного типа процесса
        """
        lock_file_path = self.lock_files[process_type]
        lock_file = None
        
        try:
            lock_file = self._try_acquire_file_lock(lock_file_path, timeout)
            
            if lock_file is None:
                raise TimeoutError(f"Не удалось получить блокировку для {process_type.value} за {timeout} секунд")
            
            logger.info(f"Получена блокировка для {process_type.value}")
            yield lock_file
            
        finally:
            if lock_file:
                try:
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
            if lock_file.exists() and not self._is_stale_lock(lock_file):
                try:
                    with open(lock_file, 'r') as f:
                        info = json.loads(f.read())
                        info['process_type'] = process_type.value
                        active[process_type.value] = info
                except:
                    # Файл поврежден или недоступен
                    pass
        
        return active
    
    def wait_for_safe_access(self, requesting_process: ProcessType, max_wait: int = 300) -> bool:
        """
        Ожидает безопасного доступа для указанного процесса
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
        if ProcessType.MIGRATION.value in active:
            return False
        
        if requesting == ProcessType.MIGRATION:
            return len(active) == 0
        
        if requesting == ProcessType.DATA_COLLECTOR:
            return ProcessType.RE_ENRICHMENT.value not in active
        
        if requesting == ProcessType.RE_ENRICHMENT:
            return ProcessType.DATA_COLLECTOR.value not in active
        
        if requesting == ProcessType.ANALYSIS:
            heavy_writers = [ProcessType.DATA_COLLECTOR.value, ProcessType.RE_ENRICHMENT.value]
            return not any(writer in active for writer in heavy_writers)
        
        return True

def get_optimized_db_connection(db_path: str, enable_wal: bool = True, timeout: int = 30) -> sqlite3.Connection:
    """
    Получает оптимизированное соединение с базой данных
    """
    conn = sqlite3.connect(db_path, timeout=timeout)
    
    if enable_wal:
        # Настраиваем оптимизации для SQLite
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL") 
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB
    
    return conn

@contextmanager 
def coordinated_db_access(
    db_path: str,
    process_type: ProcessType,
    enable_wal: bool = True,
    lock_timeout: int = 60
):
    """
    Контекстный менеджер для координированного доступа к БД (Windows)
    """
    coordinator = WindowsDBCoordinator(db_path)
    
    with coordinator.acquire_lock(process_type, timeout=lock_timeout):
        conn = get_optimized_db_connection(db_path, enable_wal)
        try:
            yield conn
        finally:
            conn.close()

def check_db_status(db_path: str) -> Dict[str, Any]:
    """
    Проверяет текущий статус базы данных
    """
    coordinator = WindowsDBCoordinator(db_path)
    active_processes = coordinator.check_active_processes()
    
    # Проверяем доступность БД
    db_accessible = False
    try:
        with sqlite3.connect(db_path, timeout=5) as test_conn:
            test_conn.execute("SELECT 1").fetchone()
            db_accessible = True
    except Exception as e:
        db_accessible = False
    
    status = {
        'db_path': db_path,
        'db_exists': os.path.exists(db_path),
        'db_accessible': db_accessible,
        'active_processes': active_processes,
        'is_locked': len(active_processes) > 0,
        'timestamp': time.time()
    }
    
    return status

if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description="Windows-совместимый координатор доступа к базе данных")
    parser.add_argument('--db-path', required=True, help='Путь к базе данных')
    parser.add_argument('--check-status', action='store_true', help='Проверить статус БД')
    parser.add_argument('--wait-unlock', action='store_true', help='Ожидать освобождения БД')
    parser.add_argument('--clean-locks', action='store_true', help='Очистить устаревшие блокировки')
    
    args = parser.parse_args()
    
    if args.check_status:
        status = check_db_status(args.db_path)
        print(json.dumps(status, indent=2))
    
    if args.wait_unlock:
        coordinator = WindowsDBCoordinator(args.db_path)
        if coordinator.wait_for_safe_access(ProcessType.ANALYSIS):
            print("База данных доступна")
        else:
            print("Таймаут ожидания")
    
    if args.clean_locks:
        coordinator = WindowsDBCoordinator(args.db_path)
        for process_type, lock_file in coordinator.lock_files.items():
            if lock_file.exists() and coordinator._is_stale_lock(lock_file):
                try:
                    lock_file.unlink()
                    print(f"Удален устаревший lock: {process_type.value}")
                except:
                    pass 