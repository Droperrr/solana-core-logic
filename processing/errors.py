class TransactionError(Exception):
    """Базовое исключение для ошибок обработки транзакции."""
    pass

class SerializationError(TransactionError):
    """Ошибка при сериализации данных в JSON."""
    pass

class DatabaseError(TransactionError):
    """Ошибка при взаимодействии с базой данных."""
    pass

class EnrichmentError(TransactionError):
    """Ошибка во время этапа обогащения данных."""
    pass 