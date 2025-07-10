-- Получить по одной сигнатуре для каждого типа транзакции
SELECT signature, transaction_type
FROM transactions
WHERE transaction_type = 'buy'
LIMIT 1;

SELECT signature, transaction_type
FROM transactions
WHERE transaction_type = 'sell'
LIMIT 1;

SELECT signature, transaction_type
FROM transactions
WHERE transaction_type = 'transfer'
LIMIT 1;

SELECT signature, transaction_type
FROM transactions
WHERE transaction_type = 'failed_transaction'
LIMIT 1;

SELECT signature, transaction_type
FROM transactions
WHERE transaction_type = 'account_management'
LIMIT 1; 