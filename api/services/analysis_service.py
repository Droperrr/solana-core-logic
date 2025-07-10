# api/services/analysis_service.py
import os
from pathlib import Path
import json
from typing import List, Dict, Optional

# Определяем базовую директорию для поиска отчетов.
# Исходим из того, что скрипт запускается из корня проекта `bant/`
REPORTS_BASE_DIR = Path("analysis")

def list_analysis_results() -> List[str]:
    """
    Сканирует известные директории с результатами и возвращает список имен файлов.
    """
    # Список директорий, где могут храниться JSON-отчеты
    known_dirs = [
        Path("reports"),  # Новая директория для отчетов
        REPORTS_BASE_DIR / "phase_group_b" / "output",
        Path("output"),  # Общая папка для результатов
        Path("output") / "processed" / "group_001",  # Дополнительная директория
    ]
    
    report_files = set()
    for directory in known_dirs:
        if directory.exists() and directory.is_dir():
            for file_path in directory.glob("*.json"):
                report_files.add(file_path.name)
    
    return sorted(list(report_files), reverse=True)  # Сортируем, чтобы новые были сверху

def get_analysis_result(filename: str) -> Optional[Dict]:
    """
    Читает и возвращает содержимое JSON-файла с результатами анализа.
    Ищет файл в известных директориях.
    """
    known_dirs = [
        Path("reports"),  # Новая директория для отчетов
        REPORTS_BASE_DIR / "phase_group_b" / "output",
        Path("output"),
        Path("output") / "processed" / "group_001",
    ]

    for directory in known_dirs:
        file_path = directory / filename
        if file_path.exists() and file_path.is_file():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                # В случае ошибки возвращаем информацию об ошибке
                return {"error": f"Failed to read or parse file: {e}"}

    return None  # Если файл не найден ни в одной из директорий 