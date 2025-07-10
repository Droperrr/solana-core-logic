from typing import Any, Dict, List
from .models import EtalonModel
from parser.utils import _safe_get

def diff_etalon_vs_enrich(etalon: EtalonModel, enrich_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Сравнивает только enrich-поля эталона и результата обогащения (best practice)."""
    report = {"diffs": [], "summary": {"critical": 0, "warning": 0, "info": 0}}

    # --- Сравнение потоков токенов ---
    etalon_flows_set = { (f.token_mint, f.amount, f.flow_type) for f in etalon.token_flows }
    enrich_flows_set = set()
    enrich_flows_raw = enrich_dict.get('token_flows', [])
    if isinstance(enrich_flows_raw, list):
         for f in enrich_flows_raw:
            enrich_flows_set.add((f.get('token_mint'), f.get('amount'), f.get('flow_type')))
    if etalon_flows_set != enrich_flows_set:
        report["diffs"].append({
            "field": "token_flows",
            "etalon": sorted(list(etalon_flows_set)), 
            "enrich": sorted(list(enrich_flows_set)), 
            "severity": "CRITICAL"
        })
        report["summary"]["critical"] += 1

    # --- Сравнение swap_summary (если есть) ---
    if etalon.swap_summary or enrich_dict.get('swap_summary'):
        if etalon.swap_summary != enrich_dict.get('swap_summary'):
            report["diffs"].append({
                "field": "swap_summary",
                "etalon": etalon.swap_summary,
                "enrich": enrich_dict.get('swap_summary'),
                "severity": "CRITICAL"
            })
            report["summary"]["critical"] += 1

    return report 