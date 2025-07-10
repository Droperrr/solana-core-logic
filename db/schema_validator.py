import logging
import json
from datetime import datetime
from typing import Dict, Any, Tuple

EXPECTED_SCHEMA = {
    "transactions": {
        "signature": "text",
        "block_time": "timestamp with time zone",
        "slot": "bigint",
        "fee_payer": "text",
        "fee": "numeric",
        "error": "jsonb",
        "additional_context": "jsonb",
        "units_consumed": "bigint",
        "is_sniper": "boolean",
        "bad_sniper": "boolean",
        "priority_fee": "numeric",
        "transaction_version": "integer",
        "raw_json": "jsonb",
        "source_query_type": "text",
        "detected_patterns": "text[]",
        "compute_unit_limit": "bigint",
        "priority_micro_lamports": "numeric",
        "source_query_address": "text",
        "is_direction_ambiguous": "boolean",
        "exchange_details": "jsonb",
        "is_complex_swap": "boolean",
        "priority_fee_microlamports": "numeric",
        "priority_fee_per_cu": "numeric",
        "raw_tx_details": "jsonb",
        "involved_platforms": "text[]",
        "units_consumed_diff": "numeric",
        "instruction_types_str": "text",
        "message_raw": "jsonb",
        "meta_raw": "jsonb",
        "transaction_type": "text",
        "swap_instruction_location": "text",
        "involved_tokens_net_flow": "jsonb",
        "analysis_direction": "text",
        "analysis_tags": "text",
        "loaded_timestamp": "timestamp with time zone"
    },
    "discovered_pools": {
        "token_mint_address": "character varying",
        "pool_address": "character varying",
        "dex_id": "character varying",
        "pool_type": "character varying",
        "first_seen_signature": "text",
        "last_seen_signature": "text",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone"
    },
    # ... другие таблицы
}

CRITICAL_TABLES = {
    "transactions",
    "instructions",
    "transaction_accounts",
    # ... другие критические таблицы
}

TYPE_SYNONYMS = {
    "text[]": {"_text", "text[]"},
    "character varying": {"character varying", "varchar"},
    "integer": {"integer", "int4", "int"},
    "bigint": {"bigint", "int8"},
    "numeric": {"numeric", "decimal"},
    "text": {"text"},
    "jsonb": {"jsonb"},
    "boolean": {"boolean", "bool"},
    "timestamp with time zone": {"timestamp with time zone", "timestamptz"},
}

def normalize_type(type_name: str) -> str:
    for canon, synonyms in TYPE_SYNONYMS.items():
        if type_name in synonyms:
            return canon
    return type_name

def get_deprecated_fields(expected_schema, db_schema):
    warnings = []
    for table, columns in db_schema.items():
        if table not in expected_schema:
            continue
        for col in columns:
            if col not in expected_schema[table]:
                warnings.append({
                    "type": "FIELD_DEPRECATED",
                    "table": table,
                    "column": col,
                    "severity": "warning"
                })
    return warnings

class SchemaValidator:
    def __init__(self, expected_schema: Dict[str, Dict[str, str]]):
        self.expected_schema = expected_schema
        self.report_path = "logs/db_schema_report.json"

    def validate(self, conn) -> Tuple[Dict[str, Any], bool]:
        schema_context = {}
        errors = []
        warnings = []
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
        """)
        db_schema = {}
        for table, col, typ in cur.fetchall():
            db_schema.setdefault(table, {})[col] = normalize_type(typ)
        # Проверка по эталону
        mismatch_lines = []  # Для текстового лога
        for table, columns in self.expected_schema.items():
            if table not in db_schema:
                entry = {
                    "type": "TABLE_NOT_FOUND",
                    "table": table,
                    "severity": "critical" if table in CRITICAL_TABLES else "warning"
                }
                (errors if table in CRITICAL_TABLES else warnings).append(entry)
                schema_context[f"can_write_to_{table}"] = False
                mismatch_lines.append(f"Table '{table}' not found in DB")
                continue
            for col, expected_type in columns.items():
                if col not in db_schema[table]:
                    entry = {
                        "type": "TABLE_FIELD_MISSING",
                        "table": table,
                        "column": col,
                        "severity": "critical" if table in CRITICAL_TABLES else "warning"
                    }
                    (errors if table in CRITICAL_TABLES else warnings).append(entry)
                    schema_context[f"can_write_to_{table}"] = False
                    mismatch_lines.append(f"Column '{col}' not found in table '{table}'")
                    continue
                db_type = db_schema[table][col]
                canon_expected = normalize_type(expected_type)
                if db_type != canon_expected:
                    entry = {
                        "type": "TABLE_FIELD_TYPE_MISMATCH",
                        "table": table,
                        "column": col,
                        "expected": canon_expected,
                        "found": db_type,
                        "severity": "critical" if table in CRITICAL_TABLES else "warning"
                    }
                    (errors if table in CRITICAL_TABLES else warnings).append(entry)
                    schema_context[f"can_write_to_{table}"] = False
                    mismatch_lines.append(f"Mismatch in table '{table}', column '{col}': expected '{canon_expected}', found '{db_type}'")
            if f"can_write_to_{table}" not in schema_context:
                schema_context[f"can_write_to_{table}"] = True
        # Deprecated fields (есть в БД, нет в эталоне)
        warnings.extend(get_deprecated_fields(self.expected_schema, db_schema))
        # Формируем отчет
        has_critical_errors = len(errors) > 0
        report = {
            "validation_timestamp_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "status": "OK" if not has_critical_errors else "MISMATCH",
            "errors": errors,
            "warnings": warnings
        }
        with open(self.report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        # --- обновляемый текстовый лог ---
        with open("logs/db_schema_mismatch.log", "w", encoding="utf-8") as f:
            for line in mismatch_lines:
                f.write(line + "\n")
        return schema_context, has_critical_errors 