# api/services/groups_service.py

import os
from pathlib import Path
from typing import List, Optional

# Путь к директории с группами токенов
GROUPS_DIR = Path("data/token_groups")

def ensure_groups_directory():
    """Убеждаемся, что директория для групп существует"""
    GROUPS_DIR.mkdir(parents=True, exist_ok=True)

def list_groups() -> List[str]:
    """
    Возвращает список имен всех групп токенов.
    Сканирует директорию data/token_groups/ и возвращает имена файлов без расширения.
    
    Returns:
        List[str]: Список имен групп (например, ["group_1659.0_sol", "test_group"])
    """
    ensure_groups_directory()
    
    groups = []
    for file_path in GROUPS_DIR.glob("*.txt"):
        # Убираем расширение .txt из имени файла
        group_name = file_path.stem
        
        # Проверяем, что можем прочитать файл
        try:
            with open(file_path, 'r', encoding='utf-8') as test_file:
                test_file.read(1)  # Пробуем прочитать первый символ
            groups.append(group_name)
        except UnicodeDecodeError:
            print(f"Пропускаем файл {file_path.name}: некорректная кодировка")
        except Exception as e:
            print(f"Пропускаем файл {file_path.name}: {e}")
    
    return sorted(groups)

def get_group_tokens(group_name: str) -> Optional[List[str]]:
    """
    Возвращает список токенов для указанной группы.
    
    Args:
        group_name (str): Имя группы (без расширения .txt)
    
    Returns:
        Optional[List[str]]: Список токенов или None, если группа не найдена
    """
    ensure_groups_directory()
    
    file_path = GROUPS_DIR / f"{group_name}.txt"
    
    if not file_path.exists():
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Читаем все строки, убираем пробелы и пустые строки
            tokens = [line.strip() for line in file.readlines() if line.strip()]
            return tokens
    except UnicodeDecodeError:
        print(f"Ошибка кодировки файла {group_name}: файл содержит некорректные символы")
        return None
    except Exception as e:
        print(f"Ошибка при чтении группы {group_name}: {e}")
        return None

def create_group(group_name: str, tokens: List[str]) -> bool:
    """
    Создает новую группу токенов.
    
    Args:
        group_name (str): Имя группы (без расширения .txt)
        tokens (List[str]): Список адресов токенов
    
    Returns:
        bool: True если группа успешно создана, False в случае ошибки
    """
    ensure_groups_directory()
    
    # Проверяем, что имя группы не пустое
    if not group_name or not group_name.strip():
        return False
    
    # Очищаем имя группы от небезопасных символов
    safe_group_name = "".join(c for c in group_name if c.isalnum() or c in "._-")
    
    file_path = GROUPS_DIR / f"{safe_group_name}.txt"
    
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            # Записываем каждый токен в отдельной строке
            for token in tokens:
                if token.strip():  # Пропускаем пустые строки
                    file.write(f"{token.strip()}\n")
        
        print(f"Группа '{safe_group_name}' создана с {len(tokens)} токенами")
        return True
        
    except Exception as e:
        print(f"Ошибка при создании группы {safe_group_name}: {e}")
        return False

def delete_group(group_name: str) -> bool:
    """
    Удаляет группу токенов.
    
    Args:
        group_name (str): Имя группы (без расширения .txt)
    
    Returns:
        bool: True если группа успешно удалена, False в случае ошибки
    """
    ensure_groups_directory()
    
    file_path = GROUPS_DIR / f"{group_name}.txt"
    
    if not file_path.exists():
        return False
    
    try:
        file_path.unlink()
        print(f"Группа '{group_name}' удалена")
        return True
    except Exception as e:
        print(f"Ошибка при удалении группы {group_name}: {e}")
        return False

def update_group(group_name: str, tokens: List[str]) -> bool:
    """
    Обновляет состав группы токенов, перезаписывая файл группы.
    
    Args:
        group_name (str): Имя группы (без расширения .txt)
        tokens (List[str]): Новый список адресов токенов
    
    Returns:
        bool: True если группа успешно обновлена, False в случае ошибки
    """
    ensure_groups_directory()
    
    file_path = GROUPS_DIR / f"{group_name}.txt"
    
    # Проверяем, что группа существует
    if not file_path.exists():
        print(f"Группа '{group_name}' не найдена")
        return False
    
    # Валидируем токены
    if not tokens:
        print(f"Список токенов для группы '{group_name}' не может быть пустым")
        return False
    
    # Очищаем токены от пустых строк и пробелов
    clean_tokens = [token.strip() for token in tokens if token.strip()]
    
    if not clean_tokens:
        print(f"После очистки список токенов для группы '{group_name}' оказался пустым")
        return False
    
    try:
        # Создаем резервную копию на случай ошибки
        backup_path = file_path.with_suffix('.txt.backup')
        if file_path.exists():
            import shutil
            shutil.copy2(file_path, backup_path)
        
        # Перезаписываем файл новым списком токенов
        with open(file_path, 'w', encoding='utf-8') as file:
            for token in clean_tokens:
                file.write(f"{token}\n")
        
        # Удаляем резервную копию после успешной записи
        if backup_path.exists():
            backup_path.unlink()
        
        print(f"Группа '{group_name}' обновлена. Токенов: {len(clean_tokens)}")
        return True
        
    except Exception as e:
        print(f"Ошибка при обновлении группы {group_name}: {e}")
        
        # Восстанавливаем из резервной копии в случае ошибки
        backup_path = file_path.with_suffix('.txt.backup')
        if backup_path.exists():
            try:
                import shutil
                shutil.copy2(backup_path, file_path)
                backup_path.unlink()
                print(f"Файл группы '{group_name}' восстановлен из резервной копии")
            except Exception as restore_error:
                print(f"Ошибка при восстановлении группы {group_name}: {restore_error}")
        
        return False

def get_group_info(group_name: str) -> Optional[dict]:
    """
    Возвращает подробную информацию о группе.
    
    Args:
        group_name (str): Имя группы
    
    Returns:
        Optional[dict]: Информация о группе или None
    """
    tokens = get_group_tokens(group_name)
    if tokens is None:
        return None
    
    file_path = GROUPS_DIR / f"{group_name}.txt"
    file_stats = file_path.stat()
    
    return {
        "name": group_name,
        "token_count": len(tokens),
        "tokens": tokens,
        "created_at": file_stats.st_ctime,
        "modified_at": file_stats.st_mtime,
        "file_size": file_stats.st_size
    } 