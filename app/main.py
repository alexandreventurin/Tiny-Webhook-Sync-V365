import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.db import init_db, close_db, insert_event, insert_job, get_events_count, get_jobs_queued_count, get_jobs_list
from app.schemas import WebhookResponse, HealthResponse, JobsListResponse, JobItem
from app.utils import generate_event_key, generate_dedupe_key, determine_job_type


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Tiny Webhooks Receiver", lifespan=lifespan)


async def process_webhook(request: Request, source: str, topic: str) -> JSONResponse:
    payload = await request.json()
    
    dados = payload.get("dados", {})
    venda_id = dados.get("id")
    codigo_situacao = dados.get("codigoSituacao")
    id_nota_fiscal = dados.get("idNotaFiscal")
    
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    
    event_key = generate_event_key(
        source=source,
        topic=topic,
        venda_id=venda_id,
        codigo_situacao=codigo_situacao,
        id_nota_fiscal=id_nota_fiscal,
        payload=payload
    )
    
    event_id = await insert_event(
        event_key=event_key,
        source=source,
        topic=topic,
        venda_id=venda_id,
        codigo_situacao=codigo_situacao,
        id_nota_fiscal=id_nota_fiscal,
        payload=payload_str
    )
    
    job_type = determine_job_type(source, topic, codigo_situacao)
    dedupe_key = generate_dedupe_key(source, topic, venda_id, job_type)
    
    await insert_job(job_type=job_type, dedupe_key=dedupe_key, event_id=event_id)
    
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
    jobs_queued = await get_jobs_queued_count()
    return HealthResponse(events_total=events_total, jobs_queued=jobs_queued)


@app.get("/admin/jobs", response_model=JobsListResponse)
async def admin_jobs(status: str = "queued", limit: int = 50):
    jobs = await get_jobs_list(status=status, limit=limit)
    return JobsListResponse(
        jobs=[JobItem(**job) for job in jobs]
    )
