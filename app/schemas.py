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
    payload: Optional[Any] = None
    action_preview: Optional[Any] = None


class JobsListResponse(BaseModel):
    jobs: list[JobItem]


class RunJobsResponse(BaseModel):
    processed: int


class OrderAItem(BaseModel):
    venda_a_id: str
    needs_fetch: bool
    updated_at: datetime
    created_at: datetime


class OrderAListResponse(BaseModel):
    orders: list[OrderAItem]


class OrderASnapshotResponse(BaseModel):
    venda_a_id: str
    created_at: datetime
    updated_at: datetime
    webhook_payload: Optional[Any] = None
    fetched_payload: Optional[Any] = None
    needs_fetch: bool
    notes: Optional[str] = None
