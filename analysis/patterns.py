# analysis/patterns.py

import logging
from typing import List, Dict, Any, Optional, Set

# Константы программ (лучше вынести в общий config или utils)
ATA_PROGRAM_ID = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
SPL_TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
SUPPORTED_TOKEN_PROGRAM_IDS: Set[str] = {SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID}
# SYSTEM_PROGRAM_ID = '11111111111111111111111111111111' # Не используется в этом файле напрямую

logger_pattern = logging.getLogger("analyzer.pattern_detector") # Используем иерархию логгеров

# --- Детектор строгого паттерна "managed transfer" (create A -> ... -> close A) ---
def detect_managed_transfer_pattern(
    parsed_instructions: List[Dict[str, Any]],
    signature: str = "unknown_sig"
) -> bool:
    """
    Ищет СТРОГИЙ паттерн: create(ATA) -> transfer/transferChecked(SPL/T22) -> closeAccount(SPL/T22)
    в инструкциях транзакции, где все операции затрагивают ОДИН И ТОТ ЖЕ ATA (т.е. закрывается тот же аккаунт, что и создается),
    но инструкции НЕ обязательно идут строго подряд.

    Возвращает True, если найден хотя бы один такой полный паттерн.
    """
    n_instructions = len(parsed_instructions)
    if n_instructions < 3:
        return False

    logger_pattern.debug(f"[{signature[:10]}] Checking for *strict* managed_transfer pattern (create A -> ... -> close A)...")

    # Итерируемся по всем инструкциям, ища 'create'/'createIdempotent'
    for i in range(n_instructions): # Внешний цикл по 'create'
        ix1 = parsed_instructions[i]
        is_potential_create = (
            ix1.get('program_id') == ATA_PROGRAM_ID and
            ix1.get('instruction_type') in {'create', 'createIdempotent'}
        )
        if not is_potential_create: continue

        created_ata_pubkey = ix1.get('details', {}).get('identified_created_ata')
        if not created_ata_pubkey: continue

        logger_pattern.debug(f"[{signature[:10]}] Ix {i}: Found potential create ATA (Strict Check): {created_ata_pubkey[:10]}... Looking for subsequent transfer.")

        # Ищем 'transfer' ПОСЛЕ текущей инструкции 'create' (начиная с i + 1)
        # Средний цикл по 'transfer'
        for j in range(i + 1, n_instructions):
            ix2 = parsed_instructions[j]
            is_potential_transfer = (
                ix2.get('program_id') in SUPPORTED_TOKEN_PROGRAM_IDS and
                ix2.get('instruction_type') in {'transfer', 'transferChecked'}
            )
            if not is_potential_transfer: continue

            dest_account_pubkey = ix2.get('details', {}).get('identified_transfer_destination')
            if not dest_account_pubkey: continue

            # Сравниваем назначение с созданным ATA
            if dest_account_pubkey == created_ata_pubkey:
                logger_pattern.debug(f"[{signature[:10]}] Ix {j}: Found matching transfer (Strict Check) for ATA {created_ata_pubkey[:10]}... Looking for subsequent close.")

                # Ищем 'closeAccount' ПОСЛЕ текущей инструкции 'transfer' (начиная с j + 1)
                # Внутренний цикл по 'closeAccount'
                for k in range(j + 1, n_instructions):
                    ix3 = parsed_instructions[k]
                    is_potential_close = (
                        ix3.get('program_id') in SUPPORTED_TOKEN_PROGRAM_IDS and
                        ix3.get('instruction_type') == 'closeAccount'
                    )
                    if not is_potential_close: continue

                    closed_account_pubkey = ix3.get('details', {}).get('identified_closed_account')
                    if not closed_account_pubkey: continue

                    # Сравниваем закрываемый аккаунт с созданным ATA
                    if closed_account_pubkey == created_ata_pubkey:
                        # Паттерн найден!
                        logger_pattern.info(f"[{signature[:10]}] Found STRICT MANAGED_TRANSFER pattern (indices {i} -> {j} -> {k}) for ATA {created_ata_pubkey}.")
                        return True # Завершаем поиск успешно

                    # Если closeAccount найден, но ключ не совпал, продолжаем искать дальше в цикле k

                # Если внутренний цикл k завершился без return True
                logger_pattern.debug(f"[{signature[:10]}] Ix {j}: Finished inner loop 'k' (Strict Check) without finding matching closeAccount for ATA {created_ata_pubkey[:10]}...")
                # Продолжаем цикл j (средний) - возможно, есть другой transfer для этого же create

            # Если dest_account_pubkey != created_ata_pubkey, продолжаем искать дальше в цикле j

        # Если средний цикл j завершился, продолжаем искать следующий 'create' в цикле i

    logger_pattern.debug(f"[{signature[:10]}] No strict managed_transfer pattern found.")
    return False


# --- НОВАЯ ФУНКЦИЯ - Детектор "пакетного перевода" ---
def detect_bundled_transfer(
    parsed_instructions: List[Dict[str, Any]],
    signature: str = "unknown_sig"
) -> bool:
    """
    Ищет паттерн пакетного перевода: наличие БОЛЕЕ ОДНОГО transfer/transferChecked
    ОДНОВРЕМЕННО с наличием хотя бы одного create (ATA) ИЛИ closeAccount (SPL/T22).

    Это простой детектор для случаев типа транзакции 43FjhJUMep...
    """
    if not parsed_instructions: return False

    transfer_count = 0
    has_create_ata = False
    has_close_account = False

    logger_pattern.debug(f"[{signature[:10]}] Checking for bundled_transfer pattern...")

    for ix in parsed_instructions:
        prog_id = ix.get('program_id')
        instr_type = ix.get('instruction_type')

        # Считаем переводы
        if prog_id in SUPPORTED_TOKEN_PROGRAM_IDS and instr_type in {'transfer', 'transferChecked'}:
            transfer_count += 1
        # Проверяем наличие создания ATA
        elif prog_id == ATA_PROGRAM_ID and instr_type in {'create', 'createIdempotent'}:
            has_create_ata = True
        # Проверяем наличие закрытия счета
        elif prog_id in SUPPORTED_TOKEN_PROGRAM_IDS and instr_type == 'closeAccount':
            has_close_account = True

        # Оптимизация: если все условия уже выполнены, можно выйти из цикла раньше
        # Нам нужно >1 перевода И (создание ИЛИ закрытие)
        if transfer_count > 1 and (has_create_ata or has_close_account):
            logger_pattern.info(f"[{signature[:10]}] Found BUNDLED_TRANSFER pattern (Tfrs:{transfer_count}, Create:{has_create_ata}, Close:{has_close_account}).")
            return True

    # Проверяем результат после цикла
    pattern_found = transfer_count > 1 and (has_create_ata or has_close_account)
    logger_pattern.debug(f"[{signature[:10]}] Bundled transfer check result: Tfrs={transfer_count}, Create={has_create_ata}, Close={has_close_account}. Pattern detected: {pattern_found}")
    return pattern_found

def detect_temp_ata_swap_pattern(parsed_instructions: List[Dict[str, Any]], signature: str = "unknown_sig") -> Optional[str]:
    """
    Детектирует паттерн: создание временного ATA (WSOL), полный вход-выход (transfer+swap), закрытие ATA.
    Возвращает pubkey ATA если найден паттерн, иначе None.
    """
    n = len(parsed_instructions)
    if n < 3:
        return None
    for i in range(n):
        ix1 = parsed_instructions[i]
        if ix1.get('program_id') != ATA_PROGRAM_ID or ix1.get('instruction_type') not in {'create', 'createIdempotent'}:
            continue
        ata = ix1.get('details', {}).get('identified_created_ata')
        if not ata:
            continue
        # ищем transfer WSOL на этот ATA
        for j in range(i+1, n):
            ix2 = parsed_instructions[j]
            if ix2.get('program_id') not in SUPPORTED_TOKEN_PROGRAM_IDS or ix2.get('instruction_type') not in {'transfer', 'transferChecked'}:
                continue
            dst = ix2.get('details', {}).get('identified_transfer_destination')
            mint = ix2.get('details', {}).get('mint')
            if dst != ata or mint != 'So11111111111111111111111111111111111111112':
                continue
            # ищем swap с этим ATA как source
            for k in range(j+1, n):
                ix3 = parsed_instructions[k]
                if ix3.get('instruction_type') not in {'swap', 'route'}:
                    continue
                src = ix3.get('details', {}).get('user_source_token_account')
                if src != ata:
                    continue
                # ищем closeAccount этого ATA
                for m in range(k+1, n):
                    ix4 = parsed_instructions[m]
                    if ix4.get('program_id') not in SUPPORTED_TOKEN_PROGRAM_IDS or ix4.get('instruction_type') != 'closeAccount':
                        continue
                    closed = ix4.get('details', {}).get('identified_closed_account')
                    if closed == ata:
                        logger_pattern.info(f"[{signature[:10]}] TEMP_ATA_SWAP pattern: {ata}")
                        return ata
    return None