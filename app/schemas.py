from pydantic import BaseModel
from typing import Any, Optional
from datetime import datetime


class WebhookDados(BaseModel):
    id: Optional[int] = None
    codigoSituacao: Optional[int] = None
    idNotaFiscal: Optional[int] = None


class WebhookPayload(BaseModel):
    dados: WebhookDados


class WebhookResponse(BaseModel):
    ok: bool = True


class HealthResponse(BaseModel):
    events_total: int
    jobs_queued: int


class JobItem(BaseModel):
    id: int
    job_type: str
    dedupe_key: str
    status: str
    created_at: datetime


class JobsListResponse(BaseModel):
    jobs: list[JobItem]
