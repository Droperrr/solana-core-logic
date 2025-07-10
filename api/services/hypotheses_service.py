import re
from typing import List, Dict, Optional

def parse_hypotheses_md(file_path: str = "analysis/hypotheses.md") -> List[Dict]:
    """
    Парсит Markdown-файл с гипотезами и возвращает список словарей.
    Адаптирован под реальную структуру файла hypotheses.md
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        return []

    hypotheses = []
    
    # Ищем разные варианты заголовков с ID
    # Варианты: ### **ID:** H-005, ### **ID: H-001**, ### **ID: H-007 (...)**
    id_patterns = [
        r'### \*\*ID:\*\* (H-\d+)',          # ### **ID:** H-005
        r'### \*\*ID: (H-\d+)',              # ### **ID: H-007 (Приоритет...)
    ]
    
    # Разделяем по заголовкам с ID
    sections = []
    for pattern in id_patterns:
        matches = list(re.finditer(pattern, content))
        for match in matches:
            start = match.start()
            # Найдем конец секции (следующий ### или конец файла)
            next_section = re.search(r'\n### ', content[start + 1:])
            if next_section:
                end = start + 1 + next_section.start()
                section_content = content[start:end]
            else:
                section_content = content[start:]
            
            sections.append((match.group(1), section_content))
    
    # Парсим каждую секцию
    for hypothesis_id, section in sections:
        hypothesis = {
            'id': hypothesis_id,
            'title': '',
            'description': '',
            'status': ''
        }
        
        # Извлекаем название (ищем после **Название:** или строку после ID)
        title_patterns = [
            r'\*\*Название:\*\*\s*([^\n]+)',     # **Название:** ...
        ]
        
        for pattern in title_patterns:
            title_match = re.search(pattern, section)
            if title_match:
                hypothesis['title'] = title_match.group(1).strip()
                break
        
        # Если **Название:** не найдено, ищем в строке с ID
        if not hypothesis['title']:
            # Пытаемся извлечь название из строки с ID
            lines = section.split('\n')
            for line in lines[:5]:  # Первые 5 строк секции
                if hypothesis_id in line:
                    # Убираем ID и извлекаем название
                    line_clean = re.sub(r'### \*\*ID[:\*]*\*\*?\s*H-\d+\s*', '', line)
                    line_clean = re.sub(r'\(Приоритет:[^)]*\)', '', line_clean)  # Убираем (Приоритет: ...)
                    line_clean = re.sub(r'[⭐🔥🚨✅❌🔬🔄⚡]+\s*', '', line_clean)  # Убираем эмодзи
                    line_clean = re.sub(r'\*\*([^*]+)\*\*', r'\1', line_clean)  # Убираем ** вокруг текста
                    title = line_clean.strip()
                    
                    # Исключаем статусы как заголовки
                    exclude_terms = ['ПОДТВЕРЖДЕНА', 'ПРИОРИТЕТНАЯ', 'ОПРОВЕРГНУТА', 'КРИТИЧЕСКИЙ', 'К проверке', 'В процессе']
                    if title and len(title) > 3 and not any(term in title for term in exclude_terms):
                        hypothesis['title'] = title
                        break
        
        # Извлекаем описание  
        desc_patterns = [
            r'\*\*Описание:\*\*\s*(.*?)(?=\n\*\*|$)',               # **Описание:** ...
            r'\*\*Описание\*\*[:\s]*(.*?)(?=\n\*\*|$)',             # **Описание** ...
            r'(?<=\n\n)((?!.*\*\*)[^\n]+(?:\n(?!.*\*\*)[^\n]+)*)', # Абзац без ** маркеров
        ]
        
        for pattern in desc_patterns:
            desc_match = re.search(pattern, section, re.DOTALL)
            if desc_match:
                description = desc_match.group(1).strip()
                # Очищаем от множественных пробелов и переносов
                description = re.sub(r'\s+', ' ', description)
                if len(description) > 20:  # Минимальная длина описания
                    hypothesis['description'] = description
                    break
        
        # Извлекаем статус (различные форматы)
        status_patterns = [
            r'\*\*Статус:\*\*\s*([^\n]+)',           # **Статус:** ✅ **ПОДТВЕРЖДЕНА**
            r'✅\s*\*\*([^*]+)\*\*',                  # ✅ **ПОДТВЕРЖДЕНА**
            r'❌\s*\*\*([^*]+)\*\*',                  # ❌ **ОПРОВЕРГНУТА** 
            r'🔬\s*([^\n]+)',                         # 🔬 К проверке
            r'🔄\s*([^\n]+)',                         # 🔄 В процессе проверки
            r'⚡\s*([^\n]+)',                         # ⚡ **ПРОВЕРЯЕТСЯ**
        ]
        
        for pattern in status_patterns:
            status_match = re.search(pattern, section)
            if status_match:
                status = status_match.group(1).strip()
                # Убираем лишние звездочки
                status = re.sub(r'\*\*', '', status)
                hypothesis['status'] = status
                break
        
        # Если статус не найден, пытаемся определить по эмодзи в заголовке
        if not hypothesis['status']:
            if '✅' in section[:200]:
                hypothesis['status'] = 'Подтверждена'
            elif '❌' in section[:200]:
                hypothesis['status'] = 'Опровергнута'
            elif '🔬' in section[:200]:
                hypothesis['status'] = 'К проверке'
            elif '🔄' in section[:200]:
                hypothesis['status'] = 'В процессе проверки'
            elif '⚡' in section[:200]:
                hypothesis['status'] = 'Проверяется'
            else:
                hypothesis['status'] = 'Неизвестен'
        
        # Добавляем только если есть минимальные данные
        if hypothesis['id'] and (hypothesis['title'] or hypothesis['description']):
            hypotheses.append(hypothesis)

    return hypotheses 