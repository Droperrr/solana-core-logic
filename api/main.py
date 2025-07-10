# api/main.py

import sys
import subprocess
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from fastapi.middleware.cors import CORSMiddleware
from uuid import uuid4
import time
import db.db_manager as dbm

# Импортируем наши новые модули
from .models import Hypothesis, Feature, TokenGroup, TokenGroupDetail, CreateTokenGroupRequest, DumpOperation
from .services.hypotheses_service import parse_hypotheses_md
from .services.features_service import get_all_features
from .services.analysis_service import list_analysis_results, get_analysis_result
from .services.groups_service import list_groups, get_group_info, get_group_tokens, create_group, delete_group, update_group
from .services.progress_service import (
    get_group_progress, refresh_on_chain_counts_for_group, 
    start_collection_for_group, start_collection_for_token, get_dump_details, get_token_dossier,
    refresh_token_on_chain_count, find_dump_for_token
)
from api.services.operation_logger import log_operation_step

# Создаем экземпляр приложения FastAPI
app = FastAPI(
    title="BANT API",
    description="API для системы анализа транзакций на Solana",
    version="1.0.0"
)

# --- НАСТРОЙКА CORS ---
# Список источников, которым разрешен доступ
origins = [
    "http://localhost",
    "http://localhost:3000",  # Наш frontend Next.js
    "http://127.0.0.1:3000",  # Альтернативный адрес
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# --- Модель для тела запроса ---
class AnalysisRequest(BaseModel):
    script_name: str
    parameters: Optional[List[str]] = []

# --- Функция для запуска скрипта в фоне ---
def run_script(script_name: str, params: List[str]):
    """
    Функция, которая будет выполняться в фоновом потоке.
    Она запускает указанный скрипт с параметрами.
    """
    # Формируем команду для запуска. Убедимся, что путь к скрипту правильный.
    # sys.executable - это путь к текущему интерпретатору Python
    command = [sys.executable, f"analysis/{script_name}"] + params
    print(f"Background Task: Запуск команды - {' '.join(command)}")
    try:
        # Запускаем процесс и ждем его завершения
        result = subprocess.run(
            command, 
            capture_output=True, 
            text=True, 
            check=True,  # Вызовет исключение, если скрипт вернет ненулевой код
            encoding='utf-8'
        )
        print(f"Background Task: Скрипт {script_name} успешно выполнен.")
        print(f"Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Background Task ERROR: Скрипт {script_name} завершился с ошибкой.")
        print(f"Return Code: {e.returncode}")
        print(f"Output:\n{e.stdout}")
        print(f"Error Output:\n{e.stderr}")
    except Exception as e:
        print(f"Background Task CRITICAL ERROR: Не удалось запустить скрипт {script_name}. Ошибка: {e}")

@app.get("/api/health", tags=["System"])
async def health_check():
    """
    Проверяет работоспособность API.
    Возвращает статус "ok", если сервер работает.
    """
    return {"status": "ok"}

# ЭНДПОИНТ ДЛЯ ГИПОТЕЗ
@app.get("/api/hypotheses", response_model=List[Hypothesis], tags=["Analysis"])
async def get_hypotheses():
    """
    Возвращает список всех исследовательских гипотез из файла analysis/hypotheses.md.
    """
    try:
        hypotheses_data = parse_hypotheses_md()
        if not hypotheses_data:
            raise HTTPException(status_code=404, detail="Hypotheses file not found or empty.")
        return hypotheses_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ЭНДПОИНТ ДЛЯ ПРИЗНАКОВ
@app.get("/api/features", response_model=List[Feature], tags=["Analysis"])
async def get_features():
    """
    Возвращает список всех доступных ML-признаков.
    """
    try:
        features_data = get_all_features()
        return features_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# НОВЫЙ ЭНДПОИНТ - СПИСОК РЕЗУЛЬТАТОВ
@app.get("/api/analysis_results", response_model=List[str], tags=["Analysis"])
async def get_results_list():
    """
    Возвращает список доступных файлов с результатами анализа.
    """
    try:
        results = list_analysis_results()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# НОВЫЙ ЭНДПОИНТ - КОНКРЕТНЫЙ РЕЗУЛЬТАТ
@app.get("/api/analysis_results/{filename}", response_model=Dict, tags=["Analysis"])
async def get_result_by_filename(filename: str):
    """
    Возвращает содержимое конкретного файла с результатами анализа по его имени.
    """
    try:
        result_data = get_analysis_result(filename)
        if result_data is None:
            raise HTTPException(status_code=404, detail=f"Result file '{filename}' not found.")
        if "error" in result_data:
            raise HTTPException(status_code=500, detail=result_data["error"])
        return result_data
    except HTTPException:
        # Повторно поднимаем HTTPException без изменений
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# НОВЫЙ ЭНДПОИНТ - ЗАПУСК АНАЛИЗА
@app.post("/api/analysis/run", tags=["Analysis"])
async def run_analysis(request: AnalysisRequest, background_tasks: BackgroundTasks):
    """
    Запускает аналитический скрипт в фоновом режиме.
    """
    # Проверяем, существует ли такой скрипт
    script_path = Path("analysis") / request.script_name
    if not script_path.exists() or not script_path.is_file():
        raise HTTPException(status_code=404, detail=f"Скрипт '{request.script_name}' не найден.")

    # Добавляем задачу в фон
    background_tasks.add_task(run_script, request.script_name, request.parameters)

    return {
        "status": "accepted",
        "message": f"Запуск скрипта '{request.script_name}' начат в фоновом режиме."
    }

# ЭНДПОИНТЫ ДЛЯ УПРАВЛЕНИЯ ГРУППАМИ ТОКЕНОВ
@app.get("/api/groups", response_model=List[TokenGroup], tags=["Token Groups"])
async def get_token_groups():
    """
    Возвращает список всех групп токенов с базовой информацией.
    """
    try:
        group_names = list_groups()
        groups = []
        
        for group_name in group_names:
            info = get_group_info(group_name)
            if info:
                groups.append(TokenGroup(
                    name=info["name"],
                    token_count=info["token_count"],
                    created_at=info["created_at"],
                    modified_at=info["modified_at"],
                    file_size=info["file_size"]
                ))
        
        return groups
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/groups/{group_name}", response_model=TokenGroupDetail, tags=["Token Groups"])
async def get_token_group_detail(group_name: str):
    """
    Возвращает детальную информацию о конкретной группе токенов, включая список токенов.
    """
    try:
        info = get_group_info(group_name)
        if not info:
            raise HTTPException(status_code=404, detail=f"Группа '{group_name}' не найдена.")
        
        return TokenGroupDetail(**info)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/groups", tags=["Token Groups"])
async def create_token_group(request: CreateTokenGroupRequest):
    """
    Создает новую группу токенов.
    """
    try:
        # Валидация входных данных
        if not request.group_name.strip():
            raise HTTPException(status_code=400, detail="Имя группы не может быть пустым.")
        
        if not request.tokens:
            raise HTTPException(status_code=400, detail="Список токенов не может быть пустым.")
        
        # Проверяем, что группа с таким именем не существует
        existing_groups = list_groups()
        safe_name = "".join(c for c in request.group_name if c.isalnum() or c in "._-")
        
        if safe_name in existing_groups:
            raise HTTPException(status_code=409, detail=f"Группа с именем '{safe_name}' уже существует.")
        
        # Создаем группу
        success = create_group(request.group_name, request.tokens)
        
        if not success:
            raise HTTPException(status_code=500, detail="Не удалось создать группу.")
        
        return {
            "status": "success",
            "message": f"Группа '{safe_name}' успешно создана с {len(request.tokens)} токенами.",
            "group_name": safe_name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/groups/{group_name}", tags=["Token Groups"])
async def update_token_group(group_name: str, request: dict):
    """
    Обновляет состав группы токенов.
    """
    try:
        # Проверяем что группа существует
        existing_info = get_group_info(group_name)
        if not existing_info:
            raise HTTPException(status_code=404, detail=f"Группа '{group_name}' не найдена.")
        
        # Извлекаем новый список токенов из запроса
        if "tokens" not in request:
            raise HTTPException(status_code=400, detail="Поле 'tokens' обязательно.")
        
        new_tokens = request["tokens"]
        if not isinstance(new_tokens, list):
            raise HTTPException(status_code=400, detail="Поле 'tokens' должно быть массивом.")
        
        if not new_tokens:
            raise HTTPException(status_code=400, detail="Список токенов не может быть пустым.")
        
        # Обновляем группу
        success = update_group(group_name, new_tokens)
        
        if not success:
            raise HTTPException(status_code=500, detail="Не удалось обновить группу.")
        
        return {
            "status": "success",
            "message": f"Группа '{group_name}' успешно обновлена.",
            "token_count": len([t for t in new_tokens if t.strip()])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/groups/{group_name}", tags=["Token Groups"])
async def delete_token_group(group_name: str):
    """
    Удаляет группу токенов.
    """
    try:
        success = delete_group(group_name)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Группа '{group_name}' не найдена.")
        
        return {
            "status": "success",
            "message": f"Группа '{group_name}' успешно удалена."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ЭНДПОИНТЫ ДЛЯ ПРОГРЕССА СБОРА ДАННЫХ
@app.get("/api/groups/{group_name}/progress", tags=["Token Groups", "Data Collection"])
async def get_group_progress_endpoint(group_name: str, pre_dump_only: bool = Query(False)):
    """
    Возвращает прогресс сбора данных для всех токенов в группе с информацией о статусе группы.
    Теперь поддерживает фильтрацию только по транзакциям до дампа.
    """
    try:
        progress_data = get_group_progress(group_name, pre_dump_only=pre_dump_only)
        return progress_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/groups/{group_name}/refresh", tags=["Token Groups", "Data Collection"])
async def refresh_group_on_chain_counts(group_name: str, background_tasks: BackgroundTasks):
    """
    Запускает обновление количества транзакций в сети для всех токенов группы.
    ВНИМАНИЕ: Это фоновая задача, которая может занять много времени!
    """
    try:
        # Запускаем обновление в фоне
        background_tasks.add_task(refresh_on_chain_counts_for_group, group_name)
        
        return {
            "status": "accepted",
            "message": f"Обновление статистики для группы '{group_name}' запущено в фоновом режиме."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/groups/{group_name}/collect", tags=["Token Groups", "Data Collection"])
async def start_group_collection(group_name: str, background_tasks: BackgroundTasks):
    """
    Запускает сбор транзакций для всех токенов группы.
    Это фоновая задача!
    """
    try:
        # Запускаем сбор в фоне
        background_tasks.add_task(start_collection_for_group, group_name)
        
        return {
            "status": "accepted", 
            "message": f"Сбор данных для группы '{group_name}' запущен в фоновом режиме."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/tokens/{token_address}/collect", tags=["Data Collection"])
async def start_token_collection(token_address: str, background_tasks: BackgroundTasks):
    """
    Запускает сбор транзакций для одного токена.
    """
    try:
        # Запускаем сбор в фоне
        background_tasks.add_task(start_collection_for_token, token_address)
        
        return {
            "status": "accepted",
            "message": f"Сбор данных для токена '{token_address[:8]}...' запущен в фоновом режиме."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tokens/{token_address}/dump_details", tags=["Tokens"])
async def get_token_dump_details(token_address: str) -> Optional[dict]:
    details = get_dump_details(token_address)
    return details  # None если дамп не найден

@app.get("/api/tokens/{token_address}/dossier", tags=["Tokens"])
async def get_token_dossier_endpoint(token_address: str) -> dict:
    return get_token_dossier(token_address)

@app.post("/api/tokens/{token_address}/refresh", tags=["Data Collection"])
async def refresh_token_on_chain_counts(token_address: str, background_tasks: BackgroundTasks):
    """
    Запускает обновление количества транзакций в сети для одного токена.
    ВНИМАНИЕ: Это фоновая задача, которая может занять время!
    """
    try:
        # Запускаем обновление в фоне
        background_tasks.add_task(refresh_token_on_chain_count, token_address)
        
        return {
            "status": "accepted",
            "message": f"Обновление статистики для токена '{token_address[:8]}...' запущено в фоновом режиме."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/tokens/{token_address}/find_dump", tags=["Data Collection"])
async def find_dump_for_token_endpoint(token_address: str, background_tasks: BackgroundTasks):
    """
    Запускает принудительный поиск дампа для токена по уже собранным данным.
    Анализирует все транзакции токена в хронологическом порядке и ищет первый дамп.
    """
    try:
        # Запускаем поиск дампа в фоне
        background_tasks.add_task(find_dump_for_token, token_address)
        
        return {
            "status": "accepted",
            "message": f"Поиск дампа для токена '{token_address[:8]}...' запущен в фоновом режиме."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def run_dump_analysis_task(operation_id: str, token_id: str):
    import time
    conn = dbm.get_connection()
    try:
        cursor = conn.cursor()
        log_operation_step(operation_id, "INFO", f"Запущен анализ для токена {token_id}")
        # Старт анализа
        cursor.execute(
            "UPDATE dump_operations SET status=?, stage=?, progress=?, progress_percent=?, stage_description=? WHERE id=?",
            ("in_progress", "fetching_transactions", 0.1, 10.0, "Загрузка транзакций из БД...", operation_id)
        )
        conn.commit()
        log_operation_step(operation_id, "DEBUG", "Получено 1000 транзакций из БД (фиктивно)")
        time.sleep(1)
        # Этап 2: анализ транзакций
        cursor.execute(
            "UPDATE dump_operations SET stage=?, progress=?, progress_percent=?, stage_description=? WHERE id=?",
            ("analyzing_transactions", 0.5, 50.0, "Анализ транзакций (1/3)...", operation_id)
        )
        conn.commit()
        for i in range(3):
            log_operation_step(operation_id, "DEBUG", f"Анализ транзакции {i+1}/3...")
            cursor.execute(
                "UPDATE dump_operations SET stage_description=?, progress_percent=? WHERE id=?",
                (f"Анализ транзакции {i+1}/3...", 50.0 + (i+1)*15.0, operation_id)
            )
            conn.commit()
            time.sleep(0.3)
        # Этап 3: завершение
        result = {"dump_found": False, "details": {"analyzed": 1000}}
        finished_at = time.time()
        cursor.execute(
            "UPDATE dump_operations SET status=?, finished_at=?, progress=?, stage=?, result=?, progress_percent=?, stage_description=? WHERE id=?",
            ("finished", finished_at, 1.0, "done", str(result), 100.0, "Завершено", operation_id)
        )
        conn.commit()
        log_operation_step(operation_id, "INFO", "Анализ завершен.")
    except Exception as e:
        conn.rollback()
        finished_at = time.time()
        cursor.execute(
            "UPDATE dump_operations SET status=?, finished_at=?, error_message=?, progress_percent=?, stage_description=? WHERE id=?",
            ("failed", finished_at, str(e), 100.0, f"Ошибка: {str(e)}", operation_id)
        )
        conn.commit()
        log_operation_step(operation_id, "ERROR", f"Произошла ошибка: {str(e)}")
    finally:
        dbm.release_connection(conn)

@app.post("/api/dump/find", tags=["Dump"])
async def find_dump(request: dict, background_tasks: BackgroundTasks):
    """
    Асинхронно создает запись операции поиска дампа и запускает анализ в фоне.
    """
    token_id = request.get("token_id")
    if not token_id:
        raise HTTPException(status_code=400, detail="token_id is required")
    operation_id = str(uuid4())
    started_at = time.time()
    conn = dbm.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO dump_operations (id, token_id, status, started_at, progress, stage)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (operation_id, token_id, "pending", started_at, 0.0, "init")
        )
        conn.commit()
    finally:
        dbm.release_connection(conn)
    background_tasks.add_task(run_dump_analysis_task, operation_id, token_id)
    return {"operation_id": operation_id, "status": "started"}

@app.get("/api/operation/{operation_id}/status", tags=["Dump"])
async def get_operation_status(operation_id: str):
    """
    Возвращает статус и прогресс операции по operation_id, включая progress_percent и stage_description.
    """
    conn = dbm.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dump_operations WHERE id=?", (operation_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Operation not found")
        row["result"] = row["result"] if row["result"] is None else eval(row["result"])
        # Возвращаем только нужные поля для фронта
        return {
            "operation_id": row["id"],
            "status": row["status"],
            "progress": row["progress"],
            "progress_percent": row.get("progress_percent", 0.0),
            "stage": row["stage"],
            "stage_description": row.get("stage_description", ""),
            "result": row["result"],
            "error_message": row["error_message"]
        }
    finally:
        dbm.release_connection(conn)

@app.get("/api/operation/{operation_id}/logs", tags=["Dump"])
async def get_operation_logs(operation_id: str):
    """
    Возвращает список логов для указанной операции, отсортированных по времени.
    """
    conn = dbm.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT timestamp, level, message FROM operation_logs WHERE operation_id=? ORDER BY timestamp ASC",
            (operation_id,)
        )
        logs = cursor.fetchall()
        return logs
    finally:
        dbm.release_connection(conn)

# Добавляем базовый маршрут для корневого URL
@app.get("/", tags=["System"])
async def read_root():
    """
    Корневой эндпоинт. Возвращает приветственное сообщение.
    """
    return {"message": "Welcome to BANT API. Go to /docs for documentation."} 