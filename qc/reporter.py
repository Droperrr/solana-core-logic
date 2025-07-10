from qc.checks import (
    check_tx_structure,
    check_missing_fields,
    check_duplicate_signatures,
    check_orphan_token_transfers,
    check_enrich_errors
)

def run_all_checks(tx_details: dict, db_conn=None) -> dict:
    results = []
    # Проверки структуры на уровне dict
    results.append(check_tx_structure(tx_details))
    # Проверки на уровне БД, если соединение передано
    if db_conn is not None:
        results.append(check_missing_fields(db_conn))
        results.append(check_duplicate_signatures(db_conn))
        results.append(check_orphan_token_transfers(db_conn))
        results.append(check_enrich_errors(db_conn))
    return {'qc_results': results} 