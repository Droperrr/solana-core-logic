from enum import Enum

class QCTag(str, Enum):
    """
    Централизованный справочник QC-тегов для enriched events.
    Используйте только значения из этого Enum для поля qc_tags в EnrichedEvent.
    """
    # Success states
    SWAP_SUCCESS = "SWAP_SUCCESS"
    LIQUIDITY_ADD_SUCCESS = "LIQUIDITY_ADD_SUCCESS"
    # Partial/Warning states
    LIQUIDITY_ADD_PARTIAL = "LIQUIDITY_ADD_PARTIAL"  # e.g., only one token flow found
    AMBIGUOUS_OWNER = "AMBIGUOUS_OWNER"              # e.g., could not determine the ultimate owner
    # Error states
    SWAP_IN_OUT_MISSING = "SWAP_IN_OUT_MISSING"      # Could not determine both sides of the swap
    UNSUPPORTED_INSTRUCTION = "UNSUPPORTED_INSTRUCTION"

    # --- General Errors ---
    UNHANDLED_EXCEPTION = "UNHANDLED_EXCEPTION" # Необработанное исключение в модуле обогащения.
    PARTIAL_ENRICHMENT = "PARTIAL_ENRICHMENT" # Обогащение было выполнено, но не все поля удалось извлечь.

    # --- Jupiter Errors ---
    JUPITER_IX_NOT_FOUND = "JUPITER_IX_NOT_FOUND" # В транзакции не найдена инструкция, относящаяся к Jupiter.
    JUPITER_INPUT_TOKEN_MISSING = "JUPITER_INPUT_TOKEN_MISSING" # Не удалось определить входящий токен или его количество.
    JUPITER_OUTPUT_TOKEN_MISSING = "JUPITER_OUTPUT_TOKEN_MISSING" # Не удалось определить исходящий токен или его количество.
    JUPITER_AMOUNT_OUT_MISMATCH_RETURN_DATA = "JUPITER_AMOUNT_OUT_MISMATCH_RETURN_DATA" # Сумма на выходе из балансов не совпадает с returnData.

    # --- Raydium Errors (Примеры) ---
    RAYDIUM_POOL_NOT_FOUND = "RAYDIUM_POOL_NOT_FOUND"
    RAYDIUM_BALANCE_CHANGE_MISMATCH = "RAYDIUM_BALANCE_CHANGE_MISMATCH"

    # --- Orca Errors (Примеры) ---
    ORCA_WHIRLPOOL_NOT_FOUND = "ORCA_WHIRLPOOL_NOT_FOUND"

    # --- Raydium Errors ---
    RAYDIUM_IX_NOT_FOUND = "RAYDIUM_IX_NOT_FOUND"
    RAYDIUM_POOL_VAULT_MISMATCH = "RAYDIUM_POOL_VAULT_MISMATCH"

    # --- Orca Errors ---
    ORCA_IX_NOT_FOUND = "ORCA_IX_NOT_FOUND"

    def __str__(self) -> str:
        return self.value 

# Пример использования:
# from qc.qc_catalog import QCTag
# event.qc_tags.append(QCTag.SWAP_SUCCESS) 