from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

# Это будет наша единая модель данных.
# Используем Pydantic для валидации и строгой типизации.

class TokenInfo(BaseModel):
    mint: str
    symbol: Optional[str] = None
    amount: str
    amount_usd: Optional[float] = None
    decimals: Optional[int] = None
    ui_amount: Optional[float] = None

class FeeInfo(BaseModel):
    fee_bps: Optional[int] = None
    fee_amount: Optional[str] = None
    fee_amount_usd: Optional[float] = None

class AccountInvolved(BaseModel):
    address: str
    role: Optional[str] = None

class Hop(BaseModel):
    hop_index: int
    dex_name: str
    pool_address: str
    pool_type: Optional[str] = None
    input_token: TokenInfo
    output_token: TokenInfo
    fee: Optional[FeeInfo] = None
    price_impact: Optional[float] = None
    accounts_involved: List[AccountInvolved] = Field(default_factory=list)
    dex_specific: Optional[Dict[str, Any]] = Field(default_factory=dict)

class AggregatorInfo(BaseModel):
    name: Optional[str] = None
    version: Optional[str] = None
    program_id: Optional[str] = None

class Route(BaseModel):
    aggregator: Optional[AggregatorInfo] = None
    hops: List[Hop] = Field(default_factory=list)

class TokenFlow(BaseModel):
    token_mint: str
    flow_type: str  # IN/OUT
    amount: str
    owner: Optional[str] = None
    confidence: str = "guaranteed"

class TransactionMetadata(BaseModel):
    signature: str
    block_time: int
    slot: int
    fee: int = 0
    success: bool

class SwapSummary(BaseModel):
    protocol_type: str  # 'dex' or 'aggregator'
    protocol_name: str
    swap_type: Optional[str] = None
    input_token: TokenInfo
    output_token: TokenInfo
    price_impact: Optional[float] = None
    total_fee_usd: Optional[float] = None
    initiator: str
    beneficiary: Optional[str] = None

class ProcessingInfo(BaseModel):
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    missing_data: List[str] = Field(default_factory=list)
    processing_time_ms: Optional[int] = None
    is_valid: Optional[bool] = None
    fallback_applied: Optional[bool] = None
    fallback_fields: List[str] = Field(default_factory=list)
    
class ProtocolSpecificData(BaseModel):
    jupiter: Optional[Dict[str, Any]] = Field(default_factory=dict)
    raydium: Optional[Dict[str, Any]] = Field(default_factory=dict)
    orca: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
class EnrichContext(BaseModel):
    schema_version: str = '2.0'
    transaction_metadata: TransactionMetadata
    swap_summary: SwapSummary
    route: Route
    token_flows: List[TokenFlow] = Field(default_factory=list)
    protocol_specific_data: ProtocolSpecificData = Field(default_factory=ProtocolSpecificData)
    processing_info: ProcessingInfo = Field(default_factory=ProcessingInfo)
    
    # Агрегированные поля верхнего уровня
    sol_spent_lamports: Optional[int] = 0
    sol_spent_ui_amount: Optional[float] = 0.0
    sol_received_lamports: Optional[int] = 0
    sol_received_ui_amount: Optional[float] = 0.0
    sol_net_change_lamports: Optional[int] = 0
    sol_net_change_ui_amount: Optional[float] = 0.0
    total_fees_lamports: Optional[int] = 0
    
    # Детализация по инструкциям
    instruction_details: List[Dict[str, Any]] = Field(default_factory=list)

# Для тестов и отладки можно добавить этот блок
if __name__ == "__main__":
    print("Pydantic models in enrich_context_schema.py are defined and syntactically correct.") 