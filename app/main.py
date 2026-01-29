import json
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.db import (
    init_db, close_db, insert_event, insert_job,
    get_events_count, get_jobs_count_by_status,
    get_jobs_list, get_last_event_at, get_last_job_done_at
)
from app.schemas import WebhookResponse, HealthResponse, JobsListResponse, JobItem, RunJobsResponse
from app.utils import generate_event_key, generate_dedupe_key, determine_job_type, to_int_or_none
from app.worker import worker_loop, stop_worker, run_worker_once


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    worker_task = asyncio.create_task(worker_loop())
    yield
    stop_worker()
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    await close_db()


app = FastAPI(title="Tiny Webhooks Receiver", lifespan=lifespan)


@app.get("/")
async def root():
    return {"status": "ok"}


async def process_webhook(request: Request, source: str, topic: str) -> JSONResponse:
    payload = await request.json()
    
    dados = payload.get("dados", {})
    venda_id_raw = dados.get("id")
    codigo_situacao_raw = dados.get("codigoSituacao")
    id_nota_fiscal_raw = dados.get("idNotaFiscal")
    
    venda_id_int = to_int_or_none(venda_id_raw)
    codigo_situacao_int = to_int_or_none(codigo_situacao_raw)
    id_nota_fiscal_int = to_int_or_none(id_nota_fiscal_raw)
    
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    
    event_key = generate_event_key(
        source=source,
        topic=topic,
        venda_id=venda_id_int,
        codigo_situacao=codigo_situacao_int,
        id_nota_fiscal=id_nota_fiscal_int,
        payload=payload
    )
    
    await insert_event(
        event_key=event_key,
        source=source,
        topic=topic,
        venda_id=venda_id_int,
        codigo_situacao=codigo_situacao_int,
        id_nota_fiscal=id_nota_fiscal_int,
        payload=payload_str
    )
    
    job_type = determine_job_type(source, topic, codigo_situacao_int)
    dedupe_key = generate_dedupe_key(source, topic, venda_id_int, job_type)
    
    job_payload = {
        "source": source,
        "topic": topic,
        "venda_id": str(venda_id_int) if venda_id_int is not None else None,
        "codigo_situacao": str(codigo_situacao_int) if codigo_situacao_int is not None else None,
        "id_nota_fiscal": str(id_nota_fiscal_int) if id_nota_fiscal_int is not None else None
    }
    
    await insert_job(job_type=job_type, dedupe_key=dedupe_key, event_id=None, payload=job_payload)
    
    return JSONResponse(content={"ok": True})


@app.post("/webhooks/a/vendas", response_model=WebhookResponse)
async def webhook_a_vendas(request: Request):
    return await process_webhook(request, source="A", topic="vendas")


@app.post("/webhooks/b/vendas", response_model=WebhookResponse)
async def webhook_b_vendas(request: Request):
    return await process_webhook(request, source="B", topic="vendas")


@app.post("/webhooks/b/notas", response_model=WebhookResponse)
async def webhook_b_notas(request: Request):
    return await process_webhook(request, source="B", topic="notas")


@app.post("/webhooks/b/enviados", response_model=WebhookResponse)
async def webhook_b_enviados(request: Request):
    return await process_webhook(request, source="B", topic="enviados")


@app.get("/health", response_model=HealthResponse)
async def health():
    events_total = await get_events_count()
    jobs_queued = await get_jobs_count_by_status('queued')
    jobs_failed = await get_jobs_count_by_status('failed')
    jobs_dead = await get_jobs_count_by_status('dead')
    last_event_at = await get_last_event_at()
    last_job_done_at = await get_last_job_done_at()
    
    return HealthResponse(
        events_total=events_total,
        jobs_queued=jobs_queued,
        jobs_failed=jobs_failed,
        jobs_dead=jobs_dead,
        last_event_at=last_event_at,
        last_job_done_at=last_job_done_at
    )


@app.get("/admin/jobs", response_model=JobsListResponse)
async def admin_jobs(status: str = "queued", limit: int = 50):
    jobs = await get_jobs_list(status=status, limit=limit)
    return JobsListResponse(
        jobs=[JobItem(**job) for job in jobs]
    )


@app.post("/admin/jobs/run", response_model=RunJobsResponse)
async def admin_run_jobs(limit: int = 50):
    processed = await run_worker_once(limit=limit)
    return RunJobsResponse(processed=processed)
