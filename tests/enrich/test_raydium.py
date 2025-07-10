import os
import sys
import json
import traceback
import importlib.util
import logging

logfile = "tests/enrich/test_raydium.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler(logfile, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("enrich_test")

log.info('DEBUG: script started')
log.info("DEBUG: sys.path before append: %s", sys.path)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
log.info("DEBUG: sys.path after append: %s", sys.path)

try:
    module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../enrich_parser/enrich/raydium.py'))
    log.info("DEBUG: module_path: %s", module_path)
    spec = importlib.util.spec_from_file_location("raydium", module_path)
    raydium = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(raydium)
    enrich_raydium_swap = raydium.enrich_raydium_swap
    log.info("DEBUG: after manual import")
except Exception as e:
    log.error("[MANUAL IMPORT ERROR] %s", e)
    traceback.print_exc()

DUMP_DIR = "D:/bant/helius_raw_dump"

# Пример: тестируем одну транзакцию (замени на реальную сигнатуру)
SIG = "c4cQvf1x4visNj2hfshfJ5gCy9eZRVhynrVctDM6qr1v2nwuYSkSQR3HfRuNpvq1DFrPGpDmqfR9z71NW865MKR"
FILE = os.path.join(DUMP_DIR, f"{SIG}.json")

def main():
    log.info("DEBUG: inside main")
    try:
        log.info("START TEST")
        log.info("FILE: %s", FILE)
        with open(FILE, "r", encoding="utf-8") as f:
            tx = json.load(f)
        result = enrich_raydium_swap(tx)
        log.info("token_flows: %s", result.get("token_flows"))
        log.info("swap_summary: %s", result.get("swap_summary"))
        log.info("errors: %s", result.get("errors"))
        log.info("END TEST")
    except Exception as e:
        log.error("[ERROR] %s", e)
        traceback.print_exc()

main() 