import json
import logging
import traceback
from decoder.normalizer import normalize
from decoder.parsers import parse
from decoder.resolver import resolve

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("debug_decoder")

if __name__ == "__main__":
    try:
        # 1. Загружаем raw_json транзакции
        with open("debug_raw_tx.json", "r", encoding="utf-8") as f:
            raw_tx = json.load(f)
        logger.info("Загружен raw_json транзакции. Ключи: %s", list(raw_tx.keys()))

        # 2. Нормализация
        normalized_tx = normalize(raw_tx)
        logger.info("После нормализации: %d инструкций", len(normalized_tx.get('instructions', [])))
        for i, instr in enumerate(normalized_tx.get('instructions', [])):
            logger.info(f"  Инструкция {i+1}: program={instr.get('program')}, type={instr.get('type')}, parsed={instr.get('parsed', False)}")

        # 3. Парсинг инструкций
        parsed_instructions = parse(normalized_tx)
        logger.info("После парсинга: %d инструкций", len(parsed_instructions))
        for i, instr in enumerate(parsed_instructions):
            try:
                if isinstance(instr, dict):
                    logger.info(f"  Parsed {i+1}: program={instr.get('program')}, type={instr.get('type')}, inner={instr.get('inner', False)}")
                else:
                    logger.info(f"  Parsed {i+1}: {vars(instr) if hasattr(instr, '__dict__') else str(instr)}")
            except Exception as e:
                logger.warning(f"  Parsed {i+1}: ошибка логирования: {e}")

        # 4. Резолвинг событий
        resolved_events = resolve(parsed_instructions, normalized_tx)
        logger.info("После резолвинга: %d событий", len(resolved_events))
        for i, event in enumerate(resolved_events):
            try:
                if isinstance(event, dict):
                    logger.info(f"  Event {i+1}: type={event.get('event_type')}, details={event.get('details', {})}")
                else:
                    logger.info(f"  Event {i+1}: {vars(event) if hasattr(event, '__dict__') else str(event)}")
            except Exception as e:
                logger.warning(f"  Event {i+1}: ошибка логирования: {e}")

        # 5. Итог
        if any(e.get('event_type') == 'SWAP' for e in resolved_events):
            logger.info("УСПЕХ: Найдено SWAP-событие!")
        else:
            logger.warning("SWAP-событие не найдено. Требуется анализ и исправление.")
    except Exception as e:
        logger.error("Ошибка при трассировке пайплайна: %s", e)
        traceback.print_exc() 