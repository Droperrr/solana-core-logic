# api/services/progress_service.py

import time
import logging
import subprocess
import asyncio
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from analysis.data_provider import get_db_connection
from api.services.groups_service import get_group_tokens
from utils.signature_handler import fetch_signatures_for_token
from services.onchain_price_engine import OnChainPriceEngine

# Настройка форматирования логгера для вывода времени
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("progress_service")

# Global dictionary to track active tasks
ACTIVE_TASKS: Dict[str, str] = {}

def set_group_task_status(group_name: str, status: str, progress_percent: Optional[float] = None, current_step_description: Optional[str] = None):
    """
    Устанавливает статус задачи для группы в БД.
    
    Args:
        group_name: Имя группы
        status: Статус задачи ('refreshing', 'collecting', 'idle')
        progress_percent: Процент выполнения (0.0-100.0)
        current_step_description: Описание текущего шага
    """
    global ACTIVE_TASKS
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Сначала обновляем в памяти для быстрого доступа
        if status == 'idle':
            ACTIVE_TASKS.pop(group_name, None)
        else:
            ACTIVE_TASKS[group_name] = status
        
        # Затем сохраняем в БД
        current_time = int(time.time())
        
        # Проверяем, есть ли уже запись
        cursor.execute("""
            SELECT * FROM group_task_status 
            WHERE group_name = ?
        """, (group_name,))
        existing = cursor.fetchone()
        
        if existing:
            if status == 'idle':
                # Удаляем запись из БД при idle статусе
                cursor.execute("""
                    DELETE FROM group_task_status 
                    WHERE group_name = ?
                """, (group_name,))
            else:
                # Обновляем существующую запись
                cursor.execute("""
                    UPDATE group_task_status 
                    SET status = ?, updated_at = ?, progress_percent = ?, current_step_description = ?
                    WHERE group_name = ?
                """, (status, current_time, progress_percent, current_step_description, group_name))
        else:
            if status != 'idle':
                # Создаем новую запись только если не idle
                cursor.execute("""
                    INSERT INTO group_task_status (group_name, status, updated_at, progress_percent, current_step_description)
                    VALUES (?, ?, ?, ?, ?)
                """, (group_name, status, current_time, progress_percent, current_step_description))
        
        conn.commit()
        conn.close()
        logger.info(f"Task status for group '{group_name}': {status} ({progress_percent}% - {current_step_description})")
        
    except Exception as e:
        logger.error(f"Ошибка при установке статуса группы {group_name}: {e}")
        # Создаем таблицу если её нет
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS group_task_status (
                    group_name TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    updated_at INTEGER NOT NULL,
                    progress_percent REAL DEFAULT 0.0,
                    current_step_description TEXT
                )
            """)
            conn.commit()
            conn.close()
            logger.info("Создана таблица group_task_status")
            # Повторяем попытку
            set_group_task_status(group_name, status, progress_percent, current_step_description)
        except Exception as e2:
            logger.error(f"Не удалось создать таблицу group_task_status: {e2}")

def get_group_task_status(group_name: str) -> str:
    """
    Получает текущий статус задачи для группы из БД.
    
    Args:
        group_name: Имя группы
        
    Returns:
        str: Статус ('refreshing', 'collecting', 'idle')
    """
    # Сначала проверяем в памяти
    if group_name in ACTIVE_TASKS:
        return ACTIVE_TASKS[group_name]
    
    # Затем проверяем в БД
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT status FROM group_task_status 
            WHERE group_name = ?
        """, (group_name,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            status = result['status']
            # Обновляем кэш в памяти
            ACTIVE_TASKS[group_name] = status
            return status
        
    except Exception as e:
        logger.error(f"Ошибка при получении статуса группы {group_name}: {e}")
    
    return 'idle'

def get_group_task_progress(group_name: str) -> Dict:
    """
    Получает полную информацию о прогрессе задачи для группы из БД.
    
    Args:
        group_name: Имя группы
        
    Returns:
        dict: Информация о прогрессе {status, progress_percent, current_step_description, updated_at}
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT status, progress_percent, current_step_description, updated_at
            FROM group_task_status 
            WHERE group_name = ?
        """, (group_name,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'status': result['status'],
                'progress_percent': result['progress_percent'],
                'current_step_description': result['current_step_description'],
                'updated_at': result['updated_at']
            }
        
    except Exception as e:
        logger.error(f"Ошибка при получении прогресса группы {group_name}: {e}")
    
    return {
        'status': 'idle',
        'progress_percent': 0.0,
        'current_step_description': None,
        'updated_at': None
    }

def get_db_transaction_count(token_address: str) -> int:
    """
    Получает количество транзакций для токена из БД.
    
    Args:
        token_address: Адрес токена
        
    Returns:
        int: Количество транзакций в БД
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM transactions 
            WHERE source_query_address = ?
        """, (token_address,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result['count'] if result else 0
        
    except Exception as e:
        logger.error(f"Ошибка при получении количества транзакций из БД для {token_address}: {e}")
        return 0

def get_on_chain_transaction_count(token_address: str) -> int:
    """
    Получает количество транзакций для токена из сети Solana.
    ВНИМАНИЕ: Эта операция может занять время и расходует RPC лимиты!
    
    Args:
        token_address: Адрес токена
        
    Returns:
        int: Количество транзакций в сети
    """
    try:
        logger.info(f"Запрашиваем количество транзакций из сети для токена {token_address[:8]}...")
        
        # Используем fetch_signatures_for_token для получения всех сигнатур
        signatures_info = fetch_signatures_for_token(
            token_mint_address=token_address,
            fetch_limit_per_call=1000,
            total_tx_limit=None,  # Без лимита - получаем все
            direction='e'  # Сначала новые
        )
        
        if not signatures_info:
            logger.warning(f"Не найдено транзакций для токена {token_address}")
            return 0
        
        # Дедуплицируем сигнатуры (на всякий случай)
        unique_signatures = set()
        for sig_info in signatures_info:
            if isinstance(sig_info, dict) and 'signature' in sig_info:
                unique_signatures.add(sig_info['signature'])
        
        count = len(unique_signatures)
        logger.info(f"Найдено {count} уникальных транзакций для токена {token_address[:8]}...")
        
        return count
        
    except Exception as e:
        logger.error(f"Ошибка при получении количества транзакций из сети для {token_address}: {e}")
        return 0

def update_token_progress_in_db(
    token_address: str,
    on_chain_tx_count: Optional[int] = None,
    db_tx_count: Optional[int] = None,
    status: Optional[str] = None,
    error_message: Optional[str] = None
) -> bool:
    """
    Обновляет прогресс токена в БД.
    
    Args:
        token_address: Адрес токена
        on_chain_tx_count: Количество транзакций в сети (если известно)
        db_tx_count: Количество транзакций в БД (если известно)
        status: Статус ('unknown', 'checking', 'collecting', 'complete', 'error')
        error_message: Сообщение об ошибке
        
    Returns:
        bool: True если обновление успешно
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем текущие данные о токене (если есть)
        cursor.execute("""
            SELECT * FROM token_collection_progress 
            WHERE token_address = ?
        """, (token_address,))
        existing = cursor.fetchone()
        
        current_time = int(time.time())
        
        if existing:
            # Обновляем существующую запись
            updates = []
            params = []
            
            if on_chain_tx_count is not None:
                updates.append("on_chain_tx_count = ?")
                params.append(on_chain_tx_count)
                updates.append("last_checked_at = ?")
                params.append(current_time)
            
            if db_tx_count is not None:
                updates.append("db_tx_count = ?")
                params.append(db_tx_count)
            
            if status is not None:
                updates.append("status = ?")
                params.append(status)
            
            if error_message is not None:
                updates.append("error_message = ?")
                params.append(error_message)
            
            # Вычисляем completeness_ratio если есть оба числа
            final_on_chain = on_chain_tx_count if on_chain_tx_count is not None else existing.get('on_chain_tx_count', 0)
            final_db_count = db_tx_count if db_tx_count is not None else existing.get('db_tx_count', 0)
            
            if final_on_chain and final_on_chain > 0:
                completeness_ratio = min(1.0, final_db_count / final_on_chain)
                updates.append("completeness_ratio = ?")
                params.append(completeness_ratio)
            
            if updates:
                params.append(token_address)
                cursor.execute(f"""
                    UPDATE token_collection_progress 
                    SET {', '.join(updates)}
                    WHERE token_address = ?
                """, params)
        else:
            # Создаем новую запись
            completeness_ratio = None
            if on_chain_tx_count and on_chain_tx_count > 0 and db_tx_count is not None:
                completeness_ratio = min(1.0, db_tx_count / on_chain_tx_count)
            
            cursor.execute("""
                INSERT INTO token_collection_progress 
                (token_address, on_chain_tx_count, db_tx_count, completeness_ratio, 
                 last_checked_at, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                token_address,
                on_chain_tx_count,
                db_tx_count if db_tx_count is not None else 0,
                completeness_ratio,
                current_time if on_chain_tx_count is not None else None,
                status or 'unknown',
                error_message
            ))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении прогресса токена {token_address}: {e}")
        return False

def get_group_progress(group_name: str, pre_dump_only: bool = False) -> Dict:
    """
    Получает прогресс сбора данных для всех токенов в группе.
    Если pre_dump_only=True, учитывает только транзакции до первого дампа (по token_lifecycle).
    
    Args:
        group_name: Имя группы
        pre_dump_only: Флаг, указывающий, учитывать ли только транзакции до первого дампа
        
    Returns:
        Dict: Прогресс группы с общим статусом и списком токенов
    """
    try:
        # Получаем список токенов в группе
        tokens = get_group_tokens(group_name)
        if not tokens:
            logger.warning(f"Группа {group_name} не найдена или пуста")
            return {
                "group_name": group_name,
                "total_tokens": 0,
                "group_status": "idle",
                "tokens": []
            }
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        token_list = []
        
        # Получаем статус группы и прогресс
        group_progress_info = get_group_task_progress(group_name)
        group_status = group_progress_info['status']
        
        for token_address in tokens:
            # Получаем актуальное количество в БД
            if pre_dump_only:
                cursor.execute("""
                    SELECT COUNT(*) as db_count
                    FROM transactions
                    LEFT JOIN token_lifecycle ON transactions.source_query_address = token_lifecycle.token_address
                    WHERE transactions.source_query_address = ?
                    AND (
                        token_lifecycle.first_dump_time IS NULL
                        OR transactions.block_time <= token_lifecycle.first_dump_time
                    )
                """, (token_address,))
            else:
                cursor.execute("""
                    SELECT COUNT(*) as db_count
                    FROM transactions
                    WHERE source_query_address = ?
                """, (token_address,))
            db_count_result = cursor.fetchone()
            current_db_count = db_count_result['db_count'] if db_count_result else 0
            
            # Получаем сохраненный прогресс
            cursor.execute("""
                SELECT * FROM token_collection_progress 
                WHERE token_address = ?
            """, (token_address,))
            progress = cursor.fetchone()
            
            # НОВОЕ: Получаем аналитические данные из token_lifecycle
            cursor.execute("""
                SELECT 
                    first_dump_signature,
                    first_dump_time,
                    creation_time,
                    first_dump_price_drop_percent
                FROM token_lifecycle 
                WHERE token_address = ?
            """, (token_address,))
            lifecycle_data = cursor.fetchone()
            
            # НОВОЕ: Рассчитываем количество транзакций до дампа
            pre_dump_tx_count = None
            if lifecycle_data and lifecycle_data.get('first_dump_time'):
                cursor.execute("""
                    SELECT COUNT(*) as pre_dump_count
                    FROM transactions
                    WHERE source_query_address = ? AND block_time <= ?
                """, (token_address, lifecycle_data['first_dump_time']))
                pre_dump_result = cursor.fetchone()
                pre_dump_tx_count = pre_dump_result['pre_dump_count'] if pre_dump_result else 0
            
            if progress:
                # Обновляем актуальное количество в БД если оно изменилось
                if current_db_count != progress.get('db_tx_count', 0):
                    update_token_progress_in_db(token_address, db_tx_count=current_db_count)
                    progress['db_tx_count'] = current_db_count
                    
                    # Пересчитываем completeness_ratio с ограничением до 100%
                    if progress.get('on_chain_tx_count') and progress['on_chain_tx_count'] > 0:
                        progress['completeness_ratio'] = min(1.0, current_db_count / progress['on_chain_tx_count'])
                
                # Определяем фактический статус токена с учетом группового статуса
                token_status = progress.get('status', 'unknown')
                if group_status == 'refreshing' and token_status in ['unknown', 'complete']:
                    token_status = 'checking'
                elif group_status == 'collecting' and token_status in ['unknown', 'complete', 'checking']:
                    token_status = 'collecting'
                
                token_info = {
                    'token_address': token_address,
                    'db_tx_count': current_db_count,
                    'on_chain_tx_count': progress.get('on_chain_tx_count'),
                    'completeness_ratio': progress.get('completeness_ratio'),
                    'last_checked_at': progress.get('last_checked_at'),
                    'status': token_status,
                    'error_message': progress.get('error_message'),
                    'last_collection_at': progress.get('last_collection_at'),
                    # НОВЫЕ АНАЛИТИЧЕСКИЕ ПОЛЯ
                    'first_dump_signature': lifecycle_data.get('first_dump_signature') if lifecycle_data else None,
                    'first_dump_time': lifecycle_data.get('first_dump_time') if lifecycle_data else None,
                    'creation_time': lifecycle_data.get('creation_time') if lifecycle_data else None,
                    'first_dump_price_drop_percent': lifecycle_data.get('first_dump_price_drop_percent') if lifecycle_data else None,
                    'pre_dump_tx_count': pre_dump_tx_count
                }
            else:
                # Создаем запись для токена, который еще не отслеживается
                update_token_progress_in_db(token_address, db_tx_count=current_db_count, status='unknown')
                
                # Определяем статус токена с учетом группового статуса
                token_status = 'unknown'
                if group_status == 'refreshing':
                    token_status = 'checking'
                elif group_status == 'collecting':
                    token_status = 'collecting'
                
                token_info = {
                    'token_address': token_address,
                    'db_tx_count': current_db_count,
                    'on_chain_tx_count': None,
                    'completeness_ratio': None,
                    'last_checked_at': None,
                    'status': token_status,
                    'error_message': None,
                    'last_collection_at': None,
                    # НОВЫЕ АНАЛИТИЧЕСКИЕ ПОЛЯ
                    'first_dump_signature': lifecycle_data.get('first_dump_signature') if lifecycle_data else None,
                    'first_dump_time': lifecycle_data.get('first_dump_time') if lifecycle_data else None,
                    'creation_time': lifecycle_data.get('creation_time') if lifecycle_data else None,
                    'first_dump_price_drop_percent': lifecycle_data.get('first_dump_price_drop_percent') if lifecycle_data else None,
                    'pre_dump_tx_count': pre_dump_tx_count
                }
            
            token_list.append(token_info)
        
        conn.close()
        
        # НОВАЯ СОРТИРОВКА: Приоритизируем аналитическую ценность
        token_list.sort(key=lambda x: (
            not x['first_dump_signature'],  # Токены с дампами в начало
            x['status'] == 'unknown',  # unknown в конец
            -(x['completeness_ratio'] or 0),  # по убыванию полноты
            x['token_address']  # по адресу для стабильности
        ))
        
        return {
            "group_name": group_name,
            "total_tokens": len(tokens),
            "group_status": group_status,
            "progress_percent": group_progress_info.get('progress_percent'),
            "current_step_description": group_progress_info.get('current_step_description'),
            "tokens": token_list
        }
        
    except Exception as e:
        logger.error(f"Ошибка при получении прогресса группы {group_name}: {e}")
        return {
            "group_name": group_name,
            "total_tokens": 0,
            "group_status": "idle",
            "tokens": []
        }

def refresh_on_chain_counts_for_group(group_name: str) -> bool:
    """
    Фоновая задача для обновления статистики из сети для целой группы.
    Использует Helius DAS API для максимальной производительности.
    
    Args:
        group_name: Имя группы
        
    Returns:
        bool: True если процесс запущен успешно
    """
    logger.info(f"Запуск фоновой задачи: обновление статистики для группы '{group_name}'...")
    
    try:
        # Устанавливаем статус группы
        set_group_task_status(group_name, 'refreshing')
        
        tokens = get_group_tokens(group_name)
        if not tokens:
            logger.warning(f"В группе '{group_name}' нет токенов для обновления.")
            set_group_task_status(group_name, 'idle')
            return False

        logger.info(f"Начинаем обновление количества транзакций в сети для группы {group_name} ({len(tokens)} токенов)")

        for i, token_address in enumerate(tokens):
            try:
                # Обновляем прогресс группы
                progress_percent = (i / len(tokens)) * 100
                current_step = f"Проверка токена {i+1}/{len(tokens)}: {token_address[:12]}..."
                set_group_task_status(group_name, 'refreshing', progress_percent, current_step)
                
                # Отмечаем что начали проверку
                update_token_progress_in_db(token_address, status='checking')
                
                # 1. Получаем точное количество транзакций из сети (включая все ATA)
                on_chain_count = get_on_chain_transaction_count(token_address)
                
                if on_chain_count is not None:
                    # Получаем актуальное количество в БД
                    db_count = get_db_transaction_count(token_address)
                    
                    # 2. Обновляем статус в нашей кэш-таблице
                    update_token_progress_in_db(
                        token_address,
                        on_chain_tx_count=on_chain_count,
                        db_tx_count=db_count,
                        status="complete" if db_count >= on_chain_count else "checked"  # Ставим статус в зависимости от полноты
                    )
                    logger.info(f"Токен {token_address[:8]}... обновлен: БД={db_count}, Сеть={on_chain_count}")
                else:
                    # В случае ошибки ставим статус 'error'
                    update_token_progress_in_db(
                        token_address,
                        status="error",
                        error_message="Failed to fetch on-chain count from DAS API"
                    )
                    logger.warning(f"Не удалось получить количество транзакций для токена {token_address[:8]}...")
                
                time.sleep(0.2)  # Небольшая пауза между запросами
                
            except Exception as e:
                logger.error(f"Ошибка при обновлении токена {token_address}: {e}")
                update_token_progress_in_db(token_address, status='error', error_message=str(e))

        # Финальное обновление прогресса
        set_group_task_status(group_name, 'refreshing', 100.0, "Завершение обновления статистики...")
        
        logger.info(f"Фоновая задача для группы '{group_name}' завершена.")
        
        # Сбрасываем статус группы
        set_group_task_status(group_name, 'idle')
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении группы {group_name}: {e}")
        set_group_task_status(group_name, 'idle')
        return False

def start_collection_for_group(group_name: str) -> bool:
    """
    Запускает сбор транзакций для токенов группы через batch_process_transactions_sqlite.py.
    Это фоновая задача!
    
    Args:
        group_name: Имя группы
        
    Returns:
        bool: True если процесс запущен успешно
    """
    try:
        # Устанавливаем статус группы
        set_group_task_status(group_name, 'collecting')
        
        # Проверяем что группа существует
        tokens = get_group_tokens(group_name)
        if not tokens:
            logger.warning(f"Группа {group_name} не найдена или пуста")
            set_group_task_status(group_name, 'idle')
            return False
        
        # Путь к файлу группы
        group_file_path = Path(f"data/token_groups/{group_name}.txt")
        if not group_file_path.exists():
            logger.error(f"Файл группы не найден: {group_file_path}")
            set_group_task_status(group_name, 'idle')
            return False
        
        # Отмечаем токены как "в процессе сбора" с прогрессом
        set_group_task_status(group_name, 'collecting', 0.0, f"Подготовка сбора данных для {len(tokens)} токенов...")
        
        for i, token_address in enumerate(tokens):
            progress_percent = (i / len(tokens)) * 10  # Первые 10% на подготовку
            current_step = f"Подготовка токена {i+1}/{len(tokens)}: {token_address[:12]}..."
            set_group_task_status(group_name, 'collecting', progress_percent, current_step)
            update_token_progress_in_db(token_address, status='collecting')
        
        # Запускаем скрипт сбора как subprocess
        script_path = Path("scripts/batch_process_transactions_sqlite.py")
        
        cmd = [
            "python", str(script_path),
            "--tokens-file", str(group_file_path),
            "--signatures-limit", "10000",  # Увеличенный лимит для полного сбора
            "--rpc-delay", "0.1"  # Небольшая задержка между RPC запросами
        ]
        
        # Обновляем прогресс перед запуском сбора
        set_group_task_status(group_name, 'collecting', 10.0, f"Запуск процесса сбора транзакций...")
        
        logger.info(f"Запускаем сбор транзакций для группы {group_name}: {' '.join(cmd)}")
        
        # Запускаем в фоне
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=Path.cwd()
        )
        
        logger.info(f"Сбор транзакций для группы {group_name} запущен (PID: {process.pid})")
        
        # Обновляем время начала сбора
        current_time = int(time.time())
        for token_address in tokens:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE token_collection_progress 
                SET last_collection_at = ?
                WHERE token_address = ?
            """, (current_time, token_address))
            conn.commit()
            conn.close()
        
        # ВАЖНО: Мы НЕ сбрасываем статус группы здесь, 
        # так как процесс запущен в фоне и еще выполняется.
        # Статус должен сбрасываться при завершении процесса.
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при запуске сбора для группы {group_name}: {e}")
        
        # Возвращаем статус error для токенов
        tokens = get_group_tokens(group_name) or []
        for token_address in tokens:
            update_token_progress_in_db(token_address, status='error', error_message=str(e))
        
        # Сбрасываем статус группы при ошибке
        set_group_task_status(group_name, 'idle')
        return False

def start_collection_for_token(token_address: str) -> bool:
    """
    Запускает сбор транзакций для одного токена.
    
    Args:
        token_address: Адрес токена
        
    Returns:
        bool: True если процесс запущен успешно
    """
    try:
        # Создаем временный файл с одним токеном
        temp_file = Path(f"temp_single_token_{int(time.time())}.txt")
        
        with open(temp_file, 'w') as f:
            f.write(f"{token_address}\n")
        
        # Отмечаем токен как "в процессе сбора"
        update_token_progress_in_db(token_address, status='collecting')
        
        # Запускаем скрипт сбора
        script_path = Path("scripts/batch_process_transactions_sqlite.py")
        
        cmd = [
            "python", str(script_path),
            "--tokens-file", str(temp_file),
            "--signatures-limit", "10000",
            "--rpc-delay", "0.1"
        ]
        
        logger.info(f"Запускаем сбор транзакций для токена {token_address}: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=Path.cwd()
        )
        
        logger.info(f"Сбор транзакций для токена {token_address} запущен (PID: {process.pid})")
        
        # Обновляем время начала сбора
        current_time = int(time.time())
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE token_collection_progress 
            SET last_collection_at = ?
            WHERE token_address = ?
        """, (current_time, token_address))
        conn.commit()
        conn.close()
        
        # Удаляем временный файл через некоторое время
        def cleanup_temp_file():
            try:
                if temp_file.exists():
                    temp_file.unlink()
                    logger.info(f"Временный файл {temp_file} удален")
            except Exception as e:
                logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")
        
        # Запускаем очистку через 60 секунд
        import threading
        cleanup_timer = threading.Timer(60.0, cleanup_temp_file)
        cleanup_timer.start()
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при запуске сбора для токена {token_address}: {e}")
        update_token_progress_in_db(token_address, status='error', error_message=str(e))
        return False

def get_dump_details(token_address: str) -> Optional[Dict]:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM token_lifecycle WHERE token_address = ?", (token_address,))
        dump_info = cursor.fetchone()
        return dump_info  # dict или None
    finally:
        conn.close()

def get_token_dossier(token_address: str) -> Dict:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Дамп-инфо
        cursor.execute("SELECT * FROM token_lifecycle WHERE token_address = ?", (token_address,))
        dump_info = cursor.fetchone() or {}
        # Агрегаты по транзакциям
        cursor.execute("""
            SELECT COUNT(*) as tx_count,
                   MIN(block_time) as first_block_time,
                   MAX(block_time) as last_block_time
            FROM transactions
            WHERE source_query_address = ?
        """, (token_address,))
        tx_stats = cursor.fetchone() or {}
        dossier = {
            "token_address": token_address,
            "dump_info": dump_info,
            "tx_stats": tx_stats,
        }
        return dossier
    finally:
        conn.close()

def refresh_token_on_chain_count(token_address: str):
    """
    Запускает фоновую задачу для обновления количества транзакций в сети для одного токена.
    """
    logger.info(f"Запуск обновления статистики для токена {token_address}")
    
    try:
        # Отмечаем токен как "в процессе проверки"
        update_token_progress_in_db(token_address, status='checking')
        
        # Получаем актуальное количество транзакций в сети
        on_chain_count = get_on_chain_transaction_count(token_address)
        
        if on_chain_count is not None:
            # Обновляем в БД
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Также получаем актуальное количество в БД
            db_count = get_db_transaction_count(token_address)
            
            # Обновляем token_progress
            update_token_progress_in_db(
                token_address,
                on_chain_tx_count=on_chain_count,
                db_tx_count=db_count,
                status="complete" if db_count >= on_chain_count else "checked"
            )
            
            conn.close()
            logger.info(f"Обновление завершено для токена {token_address}: {on_chain_count} транзакций")
        else:
            update_token_progress_in_db(token_address, status='error', error_message='Не удалось получить данные из сети')
            logger.error(f"Не удалось получить количество транзакций для токена {token_address}")
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении статистики токена {token_address}: {str(e)}")
        update_token_progress_in_db(token_address, status='error', error_message=str(e))

def find_dump_for_token(token_address: str):
    """
    Запускает принудительный поиск дампа для токена по уже собранным данным.
    """
    logger.info(f"Запуск поиска дампа для токена {token_address}")
    
    try:
        # Отмечаем токен как "в процессе анализа"
        update_token_progress_in_db(token_address, status='analyzing')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Загружаем все транзакции для токена, отсортированные по времени
        cursor.execute("""
            SELECT signature, block_time
            FROM transactions
            WHERE source_query_address = ?
            ORDER BY block_time ASC
        """, (token_address,))
        
        transactions = cursor.fetchall()
        
        if not transactions:
            update_token_progress_in_db(token_address, status='error', error_message='Нет транзакций для анализа')
            logger.warning(f"Нет транзакций для анализа дампа токена {token_address}")
            return
        
        logger.info(f"Найдено {len(transactions)} транзакций для анализа токена {token_address}")
        
        # Инициализируем движок анализа цен
        price_engine = OnChainPriceEngine()
        
        # Ищем первый дамп
        for i, tx in enumerate(transactions):
            try:
                signature = tx['signature']
                block_time = tx['block_time']
                
                # Анализируем транзакцию на предмет дампа
                is_dump, dump_data = price_engine.analyze_transaction_for_dump(signature, token_address)
                
                if is_dump and dump_data:
                    # Найден дамп! Записываем в token_lifecycle
                    cursor.execute("""
                        INSERT OR REPLACE INTO token_lifecycle 
                        (token_address, first_dump_signature, first_dump_time, 
                         first_dump_price_drop_percent, pre_dump_tx_count, last_processed_signature)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        token_address,
                        signature,
                        block_time,
                        dump_data.get('price_drop_percent'),
                        i,  # Количество транзакций до дампа
                        signature
                    ))
                    
                    conn.commit()
                    
                    update_token_progress_in_db(token_address, status='completed')
                    logger.info(f"Дамп найден для токена {token_address}: {signature}")
                    return
                    
            except Exception as e:
                logger.warning(f"Ошибка анализа транзакции {signature} для токена {token_address}: {str(e)}")
                continue
        
        # Дамп не найден
        update_token_progress_in_db(token_address, status='completed')
        logger.info(f"Дамп не найден для токена {token_address} среди {len(transactions)} транзакций")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка при поиске дампа для токена {token_address}: {str(e)}")
        update_token_progress_in_db(token_address, status='error', error_message=str(e))

 