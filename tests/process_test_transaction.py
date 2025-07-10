"""
Тестовый скрипт для проверки работы пайплайна обработки транзакций
с поддержкой отказоустойчивости.
"""
import json
import logging
import sys
import os
import sys

# Добавляем корневую директорию проекта в PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from parser.universal_parser import UniversalParser

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_transaction_from_file(file_path):
    """
    Обрабатывает транзакцию из файла и проверяет работу пайплайна.
    
    Args:
        file_path: Путь к JSON файлу с транзакцией
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_tx = json.load(f)
            
        logger.info(f"Загрузил транзакцию из {file_path}")
        
        parser = UniversalParser()
        
        # Обрабатываем транзакцию через универсальный парсер
        result = parser.parse_raw_transaction(raw_tx)
        
        logger.info(f"Результат обработки: версия парсера = {result['parser_version']}")
        logger.info(f"Обогащенных событий: {len(result['enriched_events'])}")
        
        # Выводим подробную информацию о каждом событии
        for i, event in enumerate(result['enriched_events']):
            logger.info(f"Событие #{i+1}:")
            logger.info(f"  Тип события: {event['event_type']}")
            logger.info(f"  ID события: {event['event_id']}")
            logger.info(f"  Подпись транзакции: {event['tx_signature']}")
            if 'protocol' in event and event['protocol']:
                logger.info(f"  Протокол: {event['protocol']}")
            
            # Выводим QC-теги, если они есть
            if 'protocol_details' in event and 'qc_tags' in event['protocol_details']:
                logger.info(f"  QC-теги: {event['protocol_details']['qc_tags']}")
            
            # Выводим ошибки, если они есть
            if 'protocol_details' in event and 'errors' in event['protocol_details']:
                logger.info(f"  Ошибки: {event['protocol_details']['errors']}")
                
        return True
    except Exception as e:
        logger.error(f"Ошибка обработки транзакции: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python tests/process_test_transaction.py <путь_к_файлу_транзакции>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    success = process_transaction_from_file(file_path)
    
    if success:
        logger.info("Обработка транзакции успешно завершена")
        sys.exit(0)
    else:
        logger.error("Обработка транзакции завершилась с ошибкой")
        sys.exit(1) 