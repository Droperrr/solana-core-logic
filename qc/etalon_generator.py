from .models import EtalonModel
from typing import Dict, Any
from parser.utils import _safe_get
from enrich_context_schema import TokenInfo, SwapSummary, TransactionMetadata, TokenFlow

def generate_from_reparse(raw_json: Dict[str, Any]) -> EtalonModel:
    """Генерирует эталонную модель только с enrich-полями (best practice)."""
    token_flows_objects = []
    pre_balances = _safe_get(raw_json, 'meta.preTokenBalances', [])
    post_balances = _safe_get(raw_json, 'meta.postTokenBalances', [])
    balances_by_owner = {}
    for bal in pre_balances + post_balances:
        owner = bal.get('owner')
        mint = bal.get('mint')
        if owner and mint:
            if owner not in balances_by_owner:
                balances_by_owner[owner] = {}
            if mint not in balances_by_owner[owner]:
                balances_by_owner[owner][mint] = {'pre': 0, 'post': 0}
    for bal in pre_balances:
        if bal.get('owner') and bal.get('mint'):
            amounts = bal.get('uiTokenAmount', {})
            if amounts:
                balances_by_owner[bal['owner']][bal['mint']]['pre'] = int(amounts.get('amount', 0))
    for bal in post_balances:
        if bal.get('owner') and bal.get('mint'):
            amounts = bal.get('uiTokenAmount', {})
            if amounts:
                balances_by_owner[bal['owner']][bal['mint']]['post'] = int(amounts.get('amount', 0))
    for owner, mints in balances_by_owner.items():
        for mint, amounts in mints.items():
            delta = amounts['post'] - amounts['pre']
            if delta != 0:
                direction = "IN" if delta > 0 else "OUT"
                token_flows_objects.append(TokenFlow(
                    token_mint=mint,
                    amount=str(abs(delta)),
                    flow_type=direction,
                    owner=owner
                ))
    token_flows_dicts = [flow.model_dump() for flow in token_flows_objects]
    return EtalonModel(
        token_flows=token_flows_dicts,
        swap_summary=None,
        meta=_safe_get(raw_json, 'meta')
    ) 