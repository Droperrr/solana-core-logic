# api/models.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict

class Hypothesis(BaseModel):
    id: str
    title: str
    description: str
    status: str
    category: Optional[str] = None  # Добавим позже, если понадобится

# НОВЫЕ МОДЕЛИ ДЛЯ ПРИЗНАКОВ
class FeatureDistribution(BaseModel):
    min: float
    max: float
    mean: float
    median: float
    std: float

class Feature(BaseModel):
    name: str
    type: str  # "numerical", "categorical", "boolean"
    category: str  # "liquidity", "timing", "network", "market"
    description: str
    importance: float
    correlation: float
    null_rate: float = Field(alias="nullRate")
    unique_values: int = Field(alias="uniqueValues")
    distribution: FeatureDistribution

# НОВЫЕ МОДЕЛИ ДЛЯ ГРУПП ТОКЕНОВ
class TokenGroup(BaseModel):
    name: str
    token_count: int
    created_at: float
    modified_at: float
    file_size: int

class TokenGroupDetail(BaseModel):
    name: str
    token_count: int
    tokens: List[str]
    created_at: float
    modified_at: float
    file_size: int

class CreateTokenGroupRequest(BaseModel):
    group_name: str
    tokens: List[str]

class DumpOperation(BaseModel):
    id: str
    token_id: str
    status: str  # pending, in_progress, finished, failed
    started_at: float
    finished_at: Optional[float] = None
    progress: float = 0.0
    stage: Optional[str] = None
    result: Optional[dict] = None
    error_message: Optional[str] = None

class OperationLog(BaseModel):
    id: Optional[int] = None
    operation_id: str
    timestamp: float
    level: str  # INFO, DEBUG, ERROR
    message: str 