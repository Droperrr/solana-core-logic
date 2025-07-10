from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

# Используем те же дочерние модели, что и в основной схеме
from enrich_context_schema import TokenInfo, SwapSummary, TransactionMetadata, TokenFlow

class FieldConfidence:
    GUARANTEED = "guaranteed"
    HEURISTIC = "heuristic"
    EXTERNAL = "external"
    AMBIGUOUS = "ambiguous"

class TokenFlow(BaseModel):
    token_mint: str
    flow_type: str  # IN/OUT
    amount: str
    owner: Optional[str] = None
    confidence: str = FieldConfidence.GUARANTEED

class SwapSummary(BaseModel):
    input_token_mint: Optional[str]
    input_token_amount: Optional[str]
    output_token_mint: Optional[str]
    output_token_amount: Optional[str]
    confidence: str = FieldConfidence.HEURISTIC

class EtalonModel(BaseModel):
    """
    Эталонная модель, отражающая только enrich-поля (best practice).
    """
    token_flows: List[TokenFlow] = Field(default_factory=list)
    swap_summary: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = None 