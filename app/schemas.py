from pydantic import BaseModel
from typing import Any, Optional, Union
from datetime import datetime
from uuid import UUID


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
    jobs_failed: int
    jobs_dead: int
    last_event_at: Optional[datetime] = None
    last_job_done_at: Optional[datetime] = None


class JobItem(BaseModel):
    id: Union[int, str, UUID]
    job_type: str
    dedupe_key: str
    status: str
    created_at: datetime


class JobsListResponse(BaseModel):
    jobs: list[JobItem]


class RunJobsResponse(BaseModel):
    processed: int
