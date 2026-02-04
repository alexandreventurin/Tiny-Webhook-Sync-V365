import json
import asyncio
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')

APP_BUILD = "2026-01-30-002"

from app.db import (
    init_db, close_db, insert_event, insert_job,
    get_events_count, get_jobs_count_by_status,
    get_jobs_list, get_last_event_at, get_last_job_done_at,
    get_orders_a_list, get_order_a_snapshot, get_orders_map_list
)
from app.schemas import (
    WebhookResponse, HealthResponse, JobsListResponse, JobItem, RunJobsResponse,
    OrderAItem, OrderAListResponse, OrderASnapshotResponse
)
from app.utils import generate_event_key, generate_dedupe_key, determine_job_type, to_int_or_none
from app.worker import worker_loop, stop_worker, run_worker_once, run_worker_once_detailed, WORKER_BUILD


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
    
    dados = payload.get("dados") or {}
    venda_id_raw = dados.get("id")
    codigo_situacao_raw = (
        dados.get("codigoSituacao") or dados.get("codigo_situacao") or 
        payload.get("codigoSituacao") or payload.get("codigo_situacao")
    )
    id_nota_fiscal_raw = dados.get("idNotaFiscal") or dados.get("id_nota_fiscal")
    
    venda_id_int = to_int_or_none(venda_id_raw)
    id_nota_fiscal_int = to_int_or_none(id_nota_fiscal_raw)
    codigo_situacao_str = str(codigo_situacao_raw).strip().lower() if codigo_situacao_raw not in (None, "") else None
    id_nota_fiscal_str = str(id_nota_fiscal_int) if id_nota_fiscal_int is not None else None
    
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    
    event_key = generate_event_key(
        source=source,
        topic=topic,
        venda_id=venda_id_int,
        codigo_situacao=codigo_situacao_str,
        id_nota_fiscal=id_nota_fiscal_int,
        payload=payload
    )
    
    await insert_event(
        event_key=event_key,
        source=source,
        topic=topic,
        venda_id=venda_id_int,
        codigo_situacao=codigo_situacao_str,
        id_nota_fiscal=id_nota_fiscal_str,
        payload=payload_str
    )
    
    if source == "B" and topic == "notas" and venda_id_int is None and id_nota_fiscal_int is None:
        return JSONResponse(content={"ok": True, "status": "ignored", "reason": "missing_venda_id_and_id_nota_fiscal"})
    
    job_type = determine_job_type(source, topic, codigo_situacao_str)
    dedupe_key = generate_dedupe_key(source, topic, venda_id_int, job_type)
    
    job_payload = {
        "source": source,
        "topic": topic,
        "venda_id": str(venda_id_int) if venda_id_int is not None else None,
        "codigo_situacao": codigo_situacao_str,
        "id_nota_fiscal": id_nota_fiscal_str
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


@app.post("/webhooks/b/notas_fiscais", response_model=WebhookResponse)
async def webhook_b_notas_fiscais(request: Request):
    payload = await request.json()
    
    dados = payload.get("dados") or {}
    id_nota_fiscal_raw = dados.get("idNotaFiscalTiny") or dados.get("id_nota_fiscal_tiny")
    url_danfe = dados.get("urlDanfe") or dados.get("url_danfe")
    
    id_nota_fiscal_int = to_int_or_none(id_nota_fiscal_raw)
    id_nota_fiscal_str = str(id_nota_fiscal_int) if id_nota_fiscal_int is not None else None
    
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    
    event_key = generate_event_key(
        source="B",
        topic="notas_fiscais",
        venda_id=None,
        codigo_situacao=None,
        id_nota_fiscal=id_nota_fiscal_int,
        payload=payload
    )
    
    await insert_event(
        event_key=event_key,
        source="B",
        topic="notas_fiscais",
        venda_id=None,
        codigo_situacao=None,
        id_nota_fiscal=id_nota_fiscal_str,
        payload=payload_str
    )
    
    if id_nota_fiscal_int is None:
        return JSONResponse(content={"ok": True, "status": "ignored", "reason": "missing_id_nota_fiscal"})
    
    job_type = "sync_nf_link"
    dedupe_key = f"B:notas_fiscais:{id_nota_fiscal_str}:{job_type}"
    
    job_payload = {
        "source": "B",
        "topic": "notas_fiscais",
        "venda_id": None,
        "codigo_situacao": None,
        "id_nota_fiscal": id_nota_fiscal_str,
        "url_danfe": url_danfe
    }
    
    await insert_job(job_type=job_type, dedupe_key=dedupe_key, event_id=None, payload=job_payload)
    
    return JSONResponse(content={"ok": True})


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
async def admin_jobs(status: str | None = None, job_type: str | None = None, limit: int = 50):
    jobs = await get_jobs_list(status=status, limit=limit, job_type=job_type)
    return JobsListResponse(
        jobs=[JobItem(**job) for job in jobs]
    )


@app.post("/admin/jobs/run", response_model=RunJobsResponse)
async def admin_run_jobs(limit: int = 50):
    processed = await run_worker_once(limit=limit)
    return RunJobsResponse(processed=processed)


@app.post("/admin/worker/run_once")
async def admin_worker_run_once(limit: int = 50):
    result = await run_worker_once_detailed(limit=limit)
    return result


@app.post("/admin/jobs/retry-failed")
async def admin_retry_failed_jobs(job_type: str = None):
    from app.db import retry_failed_jobs, get_failed_jobs_count
    
    before = await get_failed_jobs_count()
    retried = await retry_failed_jobs(job_type)
    after = await get_failed_jobs_count()
    
    return {
        "ok": True,
        "retried": retried,
        "job_type_filter": job_type,
        "failed_before": before,
        "failed_after": after
    }


@app.get("/admin/jobs/failed-count")
async def admin_failed_jobs_count():
    from app.db import get_failed_jobs_count
    
    counts = await get_failed_jobs_count()
    total = sum(counts.values())
    
    return {
        "ok": True,
        "total": total,
        "by_type": counts
    }


@app.get("/admin/orders-a", response_model=OrderAListResponse)
async def admin_orders_a(limit: int = 50):
    orders = await get_orders_a_list(limit=limit)
    return OrderAListResponse(
        orders=[OrderAItem(**order) for order in orders]
    )


@app.get("/admin/orders-a/{venda_a_id}", response_model=OrderASnapshotResponse)
async def admin_order_a_detail(venda_a_id: str):
    order = await get_order_a_snapshot(venda_a_id)
    if not order:
        return JSONResponse(status_code=404, content={"error": "not_found"})
    return OrderASnapshotResponse(**order)


@app.get("/admin/runtime")
async def admin_runtime():
    return {
        "app_build": APP_BUILD,
        "worker_build": WORKER_BUILD
    }


@app.get("/admin/orders-map")
async def admin_orders_map(limit: int = 50):
    orders = await get_orders_map_list(limit=limit)
    return {"orders": orders}


@app.get("/admin/tiny_a/ping")
async def admin_tiny_a_ping(venda_id: str):
    from app.tiny_oauth import ensure_access_token
    import httpx
    
    token = await ensure_access_token("A")
    if not token:
        return {"ok": False, "error": "No valid token for account A"}
    
    url = f"https://api.tiny.com.br/public-api/v3/pedidos/{venda_id}"
    headers = {"Authorization": f"Bearer {token}"}
    
    async with httpx.AsyncClient(timeout=30.0) as http_client:
        response = await http_client.get(url, headers=headers)
        
        return {
            "ok": response.status_code == 200,
            "status_code": response.status_code,
            "url": url,
            "token_preview": token[:50] + "..." if token else None,
            "response_body": response.text[:500] if response.text else None
        }


@app.get("/admin/tiny_a/test_contatos")
async def admin_tiny_a_test_contatos():
    from app.tiny_oauth import ensure_access_token
    import httpx
    
    token = await ensure_access_token("A")
    if not token:
        return {"ok": False, "error": "No valid token for account A"}
    
    url = "https://api.tiny.com.br/public-api/v3/contatos?limite=1"
    headers = {"Authorization": f"Bearer {token}"}
    
    async with httpx.AsyncClient(timeout=30.0) as http_client:
        response = await http_client.get(url, headers=headers)
        
        return {
            "ok": response.status_code == 200,
            "status_code": response.status_code,
            "url": url,
            "response_body": response.text[:500] if response.text else None
        }


@app.get("/admin/tiny_a/produtos")
async def admin_tiny_a_produtos():
    from app.tiny_oauth import ensure_access_token
    from app.tiny_client import TinyClient
    from app.worker import PRODUTO_ID_MAP
    
    token = await ensure_access_token("A")
    if not token:
        return {"ok": False, "error": "No valid token for account A"}
    
    client = TinyClient(token)
    try:
        products = await client.list_all_products()
        result = []
        for p in products:
            pid = p.get("id")
            situacao = p.get("situacao", "")
            ativo = situacao == "A" or situacao == "Ativo" or str(situacao).lower() == "ativo"
            result.append({
                "id": pid,
                "sku": p.get("sku") or p.get("codigo") or "",
                "descricao": p.get("descricao") or p.get("nome") or "",
                "situacao": situacao,
                "ativo": ativo,
                "id_b": PRODUTO_ID_MAP.get(pid),
                "mapeado": pid in PRODUTO_ID_MAP
            })
        ativos = [r for r in result if r["ativo"]]
        return {
            "ok": True,
            "total": len(result),
            "ativos": len(ativos),
            "inativos": len(result) - len(ativos),
            "mapeados": sum(1 for r in result if r["mapeado"]),
            "nao_mapeados": sum(1 for r in result if not r["mapeado"]),
            "produtos": result
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/admin/tiny_a/produtos/sync")
async def admin_tiny_a_produtos_sync():
    from app.tiny_oauth import ensure_access_token
    from app.tiny_client import TinyClient
    from app.db import upsert_product_map
    
    token = await ensure_access_token("A")
    if not token:
        return {"ok": False, "error": "No valid token for account A"}
    
    client = TinyClient(token)
    try:
        products = await client.list_all_products()
        updated = 0
        for p in products:
            pid = p.get("id")
            sku = p.get("sku") or p.get("codigo") or ""
            descricao = p.get("descricao") or p.get("nome") or ""
            situacao = p.get("situacao", "")
            ativo = situacao == "A" or situacao == "Ativo" or str(situacao).lower() == "ativo"
            await upsert_product_map(pid, sku, descricao, situacao, ativo)
            updated += 1
        return {
            "ok": True,
            "message": f"Synchronized {updated} products from Tiny A to products_map table",
            "updated": updated
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/admin/products_map")
async def admin_products_map():
    from app.db import get_products_map_list
    
    try:
        products = await get_products_map_list()
        mapeados = [p for p in products if p.get("id_b")]
        ativos = [p for p in products if p.get("ativo")]
        return {
            "ok": True,
            "total": len(products),
            "mapeados": len(mapeados),
            "nao_mapeados": len(products) - len(mapeados),
            "ativos": len(ativos),
            "produtos": products
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/admin/tiny_b/ping")
async def admin_tiny_b_ping(venda_id: str):
    from app.tiny_oauth import ensure_access_token
    from app.tiny_client import TinyClient
    
    token = await ensure_access_token("B")
    if not token:
        return {"ok": False, "error": "No valid token for account B"}
    
    client = TinyClient(token)
    result = await client.ping_order(venda_id)
    return {
        "ok": result.ok,
        "status_code": result.status_code,
        "excerpt": result.excerpt,
        "error": result.error
    }


@app.get("/admin/tokens")
async def admin_tokens():
    from app.tiny_oauth import list_token_status
    tokens = await list_token_status()
    return {"tokens": tokens}


@app.get("/auth/a/start")
async def auth_a_start():
    from fastapi.responses import RedirectResponse
    from app.tiny_oauth import build_auth_url
    url = build_auth_url("A")
    if not url:
        return {"error": "TINY_A_CLIENT_ID not configured"}
    return RedirectResponse(url=url, status_code=302)


@app.get("/auth/b/start")
async def auth_b_start():
    from fastapi.responses import RedirectResponse
    from app.tiny_oauth import build_auth_url
    url = build_auth_url("B")
    if not url:
        return {"error": "TINY_B_CLIENT_ID not configured"}
    return RedirectResponse(url=url, status_code=302)


@app.get("/auth/a/callback")
async def auth_a_callback(code: str | None = None, error: str | None = None, error_description: str | None = None):
    from app.tiny_oauth import exchange_code_for_tokens, save_tokens_to_db
    
    if error:
        return {"error": error, "description": error_description}
    
    if not code:
        return {"error": "missing code parameter"}
    
    try:
        tokens = await exchange_code_for_tokens("A", code)
        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        expires_in = tokens.get("expires_in", 3600)
        
        if not access_token or not refresh_token:
            return {"error": "missing tokens in response", "raw": tokens}
        
        await save_tokens_to_db("A", access_token, refresh_token, expires_in)
        return {"ok": True, "account": "A", "expires_in": expires_in}
    except Exception as e:
        return {"error": str(e)}


@app.get("/auth/b/callback")
async def auth_b_callback(code: str | None = None, error: str | None = None, error_description: str | None = None):
    from app.tiny_oauth import exchange_code_for_tokens, save_tokens_to_db
    
    if error:
        return {"error": error, "description": error_description}
    
    if not code:
        return {"error": "missing code parameter"}
    
    try:
        tokens = await exchange_code_for_tokens("B", code)
        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        expires_in = tokens.get("expires_in", 3600)
        
        if not access_token or not refresh_token:
            return {"error": "missing tokens in response", "raw": tokens}
        
        await save_tokens_to_db("B", access_token, refresh_token, expires_in)
        return {"ok": True, "account": "B", "expires_in": expires_in}
    except Exception as e:
        return {"error": str(e)}
