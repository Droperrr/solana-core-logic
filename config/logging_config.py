import logging
import os
import json
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
from multiprocessing import Queue
import sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_object = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger_name": record.name,
        }
        if record.exc_info:
            log_object['exc_info'] = self.formatException(record.exc_info)
        return json.dumps(log_object, ensure_ascii=False)

class DBWriteFilter(logging.Filter):
    def filter(self, record):
        return 'db.write:' in record.getMessage()

def setup_console_run_logging():
    LOG_DIR = 'logs'
    os.makedirs(LOG_DIR, exist_ok=True)
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'console_run.log'), mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = []
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

def setup_logging():
    LOG_DIR = 'logs'
    os.makedirs(LOG_DIR, exist_ok=True)
    log_queue = Queue(-1)
    db_error_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, 'failed_transactions.log'),
        mode='w',
        maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    db_error_handler.setFormatter(JsonFormatter())
    listener = QueueListener(log_queue, db_error_handler, respect_handler_level=True)
    queue_handler = QueueHandler(log_queue)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    root_logger.addHandler(queue_handler)

    # --- Добавляем отдельный консольный handler только для db.write ---
    db_console_handler = logging.StreamHandler(sys.stdout)
    db_console_handler.setLevel(logging.INFO)
    db_console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    db_logger = logging.getLogger("db.write")
    db_logger.setLevel(logging.INFO)
    db_logger.addHandler(db_console_handler)
    db_logger.propagate = False

    # --- FileHandler для db.write ---
    db_file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'db_write_status.log'), mode='w', encoding='utf-8')
    db_file_handler.setLevel(logging.INFO)
    db_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    db_logger = logging.getLogger("db.write")
    db_logger.addHandler(db_file_handler)

    return listener 

def setup_db_write_logging():
    LOG_DIR = 'logs'
    os.makedirs(LOG_DIR, exist_ok=True)
    db_write_handler = logging.FileHandler(os.path.join(LOG_DIR, 'db_write.log'), mode='w', encoding='utf-8')
    db_write_handler.setLevel(logging.INFO)
    db_write_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    db_write_handler.addFilter(DBWriteFilter())
    root_logger = logging.getLogger()
    root_logger.addHandler(db_write_handler) 

def setup_enrich_qc_logging():
    LOG_DIR = 'logs'
    os.makedirs(LOG_DIR, exist_ok=True)
    enrich_qc_handler = logging.FileHandler(os.path.join(LOG_DIR, 'enrich_qc.log'), mode='w', encoding='utf-8')
    enrich_qc_handler.setLevel(logging.INFO)
    enrich_qc_handler.setFormatter(JsonFormatter())
    enrich_qc_logger = logging.getLogger("enrich_qc")
    enrich_qc_logger.setLevel(logging.INFO)
    enrich_qc_logger.addHandler(enrich_qc_handler)
    enrich_qc_logger.propagate = False 