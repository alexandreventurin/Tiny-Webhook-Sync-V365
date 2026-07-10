import json
import asyncio
import logging
import sys
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')

APP_BUILD = "2026-02-05-001"
SERVER_STARTED_AT = datetime.now(timezone.utc)

from app.db import (
    init_db, close_db, insert_event, insert_job,
    get_events_count, get_jobs_count_by_status,
    get_jobs_list, get_last_event_at, get_last_job_done_at,
    get_orders_a_list, get_order_a_snapshot, get_orders_map_list,
    check_is_echo, update_event_action_result,
    get_order_mapping_by_a, get_order_mapping_by_c
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
    from app.db import recover_stale_import_runs
    recovered = await recover_stale_import_runs()
    if recovered:
        logging.getLogger(__name__).info(f"Recovered {recovered} stale import run(s) from previous restart")
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


# Threshold: worker deve pulsar dentro dessa janela. Além disso = considerar travado.
WORKER_HEARTBEAT_MAX_AGE_SECONDS = 30 * 60  # 30 minutos


@app.get("/health")
async def health():
    """
    Healthcheck usado pelo Fly.io para detectar worker travado.
    - 200 OK: worker pulsou nas últimas 4h → tudo bem
    - 500: heartbeat velho ou ausente → Fly reinicia a máquina automaticamente
    """
    from app.db import get_worker_heartbeat_age_seconds
    age = await get_worker_heartbeat_age_seconds()
    if age is None:
        return JSONResponse(
            {"ok": False, "reason": "no_heartbeat_yet", "threshold_seconds": WORKER_HEARTBEAT_MAX_AGE_SECONDS},
            status_code=500,
        )
    if age > WORKER_HEARTBEAT_MAX_AGE_SECONDS:
        return JSONResponse(
            {"ok": False, "reason": "worker_stale", "heartbeat_age_seconds": age, "threshold_seconds": WORKER_HEARTBEAT_MAX_AGE_SECONDS},
            status_code=500,
        )
    return {"ok": True, "heartbeat_age_seconds": round(age, 1), "threshold_seconds": WORKER_HEARTBEAT_MAX_AGE_SECONDS}


async def process_webhook(request: Request, source: str, topic: str) -> JSONResponse:
    logger = logging.getLogger(__name__)
    raw_body = await request.body()
    if not raw_body:
        logger.info(f"process_webhook [{source}/{topic}] empty body (ping from Tiny), ignoring")
        return JSONResponse(content={"ok": True, "status": "ignored", "reason": "empty_body"})

    try:
        payload = json.loads(raw_body)
    except Exception:
        logger.warning(f"process_webhook [{source}/{topic}] invalid JSON body: {raw_body[:500]}")
        return JSONResponse(content={"ok": True, "status": "ignored", "reason": "invalid_json"})

    try:
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
        
        event_id = await insert_event(
            event_key=event_key,
            source=source,
            topic=topic,
            venda_id=venda_id_int,
            codigo_situacao=codigo_situacao_str,
            id_nota_fiscal=id_nota_fiscal_str,
            payload=payload_str
        )
        
        if source == "B" and topic == "notas" and venda_id_int is None and id_nota_fiscal_int is None:
            if event_id:
                await update_event_action_result(event_id, "noop")
            return JSONResponse(content={"ok": True, "status": "ignored", "reason": "missing_venda_id_and_id_nota_fiscal"})
        
        job_type = determine_job_type(source, topic, codigo_situacao_str)
        
        if job_type == "noop":
            if event_id:
                await update_event_action_result(event_id, "noop")
            return JSONResponse(content={"ok": True, "status": "ignored", "reason": f"noop for {source}/{topic}/{codigo_situacao_str}"})
        
        if job_type == "sync_status" and venda_id_int and codigo_situacao_str:
            is_echo = await check_is_echo(source, str(venda_id_int), codigo_situacao_str)
            if is_echo:
                if event_id:
                    await update_event_action_result(event_id, "echo")
                logger.info(f"Echo detected: {source} venda {venda_id_int} {codigo_situacao_str} (ignored)")
                return JSONResponse(content={"ok": True, "status": "ignored", "reason": "echo"})

            venda_str = str(venda_id_int)
            if source == "A":
                mapping = await get_order_mapping_by_a(venda_str)
            else:
                mapping = await get_order_mapping_by_c(venda_str)
            if not mapping:
                noop_payload = {
                    "source": source, "topic": topic,
                    "venda_id": venda_str, "codigo_situacao": codigo_situacao_str
                }
                dedupe_key = generate_dedupe_key(source, topic, venda_id_int, "noop", codigo_situacao=codigo_situacao_str)
                await insert_job(job_type="noop", dedupe_key=dedupe_key, event_id=None, payload=noop_payload)
                if event_id:
                    await update_event_action_result(event_id, "noop:no_orders_map")
                logger.info(f"sync_status skipped: no orders_map for {source} venda {venda_id_int}")
                return JSONResponse(content={"ok": True, "status": "ignored", "reason": "no_orders_map"})
        
        dedupe_key = generate_dedupe_key(source, topic, venda_id_int, job_type, codigo_situacao=codigo_situacao_str)
        
        job_payload = {
            "source": source,
            "topic": topic,
            "venda_id": str(venda_id_int) if venda_id_int is not None else None,
            "codigo_situacao": codigo_situacao_str,
            "id_nota_fiscal": id_nota_fiscal_str
        }
        
        # sync_tracking_c_to_a: delay 1 min para dar tempo da transportadora popular o código de rastreio
        initial_delay = 1 if job_type == "sync_tracking_c_to_a" else 0
        await insert_job(job_type=job_type, dedupe_key=dedupe_key, event_id=None, payload=job_payload, delay_minutes=initial_delay)
        
        if event_id:
            await update_event_action_result(event_id, f"job:{job_type}")
        
        return JSONResponse(content={"ok": True})

    except Exception as exc:
        logger.error(f"process_webhook [{source}/{topic}] unhandled error: {exc} | payload: {raw_body[:500]}", exc_info=True)
        return JSONResponse(content={"ok": True, "status": "error_logged"})


@app.post("/webhooks/a/vendas", response_model=WebhookResponse)
async def webhook_a_vendas(request: Request):
    return await process_webhook(request, source="A", topic="vendas")


@app.post("/webhooks/rejuderme/vendas", response_model=WebhookResponse)
async def webhook_rejuderme_vendas(request: Request):
    return await process_webhook(request, source="A", topic="vendas")


@app.post("/webhooks/c/vendas", response_model=WebhookResponse)
async def webhook_c_vendas(request: Request):
    return await process_webhook(request, source="B", topic="vendas")


@app.post("/webhooks/c/notas", response_model=WebhookResponse)
async def webhook_c_notas(request: Request):
    return await process_webhook(request, source="B", topic="notas")


@app.post("/webhooks/c/enviados", response_model=WebhookResponse)
async def webhook_c_enviados(request: Request):
    return await process_webhook(request, source="B", topic="enviados")


@app.post("/webhooks/c/notas_fiscais", response_model=WebhookResponse)
async def webhook_c_notas_fiscais(request: Request):
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
    dedupe_key = f"C:notas_fiscais:{id_nota_fiscal_str}:{job_type}"
    
    nf_numero = dados.get("numero")
    nf_serie = dados.get("serie")
    nf_chave_acesso = dados.get("chaveAcesso") or dados.get("chave_acesso")
    nf_data_emissao = dados.get("dataEmissao") or dados.get("data_emissao")
    nf_valor_nota = dados.get("valorNota") or dados.get("valor_nota")

    job_payload = {
        "source": "B",
        "topic": "notas_fiscais",
        "venda_id": None,
        "codigo_situacao": None,
        "id_nota_fiscal": id_nota_fiscal_str,
        "url_danfe": url_danfe,
        "nf_numero": str(nf_numero) if nf_numero is not None else None,
        "nf_serie": str(nf_serie) if nf_serie is not None else None,
        "nf_chave_acesso": nf_chave_acesso,
        "nf_data_emissao": nf_data_emissao,
        "nf_valor_nota": nf_valor_nota,
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


@app.post("/admin/jobs/retry-waiting-sku")
async def admin_retry_waiting_sku():
    from app.db import retry_waiting_sku_jobs
    count = await retry_waiting_sku_jobs()
    return {"ok": True, "requeued": count}


@app.post("/admin/jobs/retry-tracking-skipped")
async def admin_retry_tracking_skipped(days: int | None = None):
    """
    Recoloca na fila pedidos cujo sync de rastreio terminou sem código após retries.
    Sem parâmetro `days` = reprocessa TODOS (escalonado em 10/min).
    Com `days=N` = só dos últimos N dias.
    """
    from app.db import retry_tracking_skipped
    if days is not None and (days < 1 or days > 365):
        return {"ok": False, "error": "days deve estar entre 1 e 365 (ou omitido para todos)"}
    count = await retry_tracking_skipped(days)
    estimated_minutes = (count * 6) // 60
    return {"ok": True, "requeued": count, "days": days, "estimated_duration_minutes": estimated_minutes, "rate": "10 jobs/min"}


@app.get("/admin/jobs/fix-numero-compra-count")
async def admin_fix_numero_compra_count():
    """Quantos pedidos podem ser atualizados (sem disparar nada)."""
    from app.db import count_numero_compra_candidates
    counts = await count_numero_compra_candidates()
    return {"ok": True, **counts}


@app.post("/admin/jobs/fix-numero-compra")
async def admin_fix_numero_compra(limit: int = 10):
    """
    Cria N jobs para atualizar numeroOrdemCompra em C com numeroPedidoEcommerce de A.
    Safe: idempotente (não recria jobs já existentes). Recomenda-se começar com limit pequeno (ex: 10) para teste.
    """
    from app.db import create_numero_compra_fix_jobs
    if limit < 1 or limit > 10000:
        return {"ok": False, "error": "limit deve estar entre 1 e 10000"}
    result = await create_numero_compra_fix_jobs(limit)
    return {"ok": True, **result}


@app.get("/admin/jobs/fix-numero-compra-report")
async def admin_fix_numero_compra_report():
    """Relatório do progresso do batch."""
    from app.db import get_numero_compra_fix_report
    report = await get_numero_compra_fix_report()
    return {"ok": True, **report}


@app.post("/admin/jobs/fix-empty-tracking")
async def admin_fix_empty_tracking():
    """Cria novos jobs escalonados para reprocessar tracking vazio. Safe: não altera jobs antigos."""
    from app.db import create_tracking_fix_jobs
    result = await create_tracking_fix_jobs()
    return {"ok": True, **result}


@app.get("/admin/jobs/tracking-fix-report")
async def admin_tracking_fix_report():
    """Relatório do progresso do batch de fix de tracking. João pode acompanhar aqui."""
    from app.db import get_tracking_fix_report
    report = await get_tracking_fix_report()
    return {"ok": True, **report}


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


@app.get("/admin/replication-status")
async def admin_replication_status():
    from app.db import count_orders_replicated_to_c
    from app.settings import MAX_ORDERS_TO_REPLICATE, EXECUTE_TINY_C
    
    current_count = await count_orders_replicated_to_c()
    limit = MAX_ORDERS_TO_REPLICATE
    
    return {
        "ok": True,
        "replicated_count": current_count,
        "limit": limit,
        "remaining": max(0, limit - current_count) if limit > 0 else "unlimited",
        "limit_reached": current_count >= limit if limit > 0 else False,
        "execute_tiny_c": EXECUTE_TINY_C
    }


@app.post("/admin/jobs/backfill")
async def admin_backfill_jobs(limit: int = 100):
    from app.db import get_pool
    
    p = await get_pool()
    async with p.acquire() as conn:
        orphan_events = await conn.fetch("""
            SELECT DISTINCT ON (e.venda_id) e.venda_id, e.codigo_situacao, e.id_nota_fiscal
            FROM public.events e
            WHERE e.source = 'A' 
              AND e.topic = 'vendas'
              AND e.codigo_situacao IN ('aprovado')
              AND NOT EXISTS (
                SELECT 1 FROM public.jobs j 
                WHERE j.dedupe_key = 'A:vendas:' || e.venda_id || ':fetch_order_a'
              )
            ORDER BY e.venda_id, e.created_at DESC
            LIMIT $1
        """, limit)
        
        created = 0
        for row in orphan_events:
            venda_id = row['venda_id']
            codigo_situacao = row['codigo_situacao']
            id_nota_fiscal = row['id_nota_fiscal']
            
            job_type = "fetch_order_a"
            dedupe_key = f"A:vendas:{venda_id}:fetch_order_a"
            
            job_payload = {
                "source": "A",
                "topic": "vendas",
                "venda_id": str(venda_id),
                "codigo_situacao": codigo_situacao,
                "id_nota_fiscal": str(id_nota_fiscal) if id_nota_fiscal else None,
                "from_backfill": True
            }
            
            await insert_job(job_type=job_type, dedupe_key=dedupe_key, event_id=None, payload=job_payload)
            created += 1
        
        return {
            "ok": True,
            "orphan_events_found": len(orphan_events),
            "jobs_created": created
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
                "id_c": PRODUTO_ID_MAP.get(pid),
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


@app.get("/admin/tiny_c/order")
async def admin_tiny_c_order(venda_id: str):
    from app.tiny_oauth import ensure_access_token
    import httpx

    token = await ensure_access_token("B")
    if not token:
        return {"ok": False, "error": "No valid token for Tiny C — refaça o OAuth em /auth/c/start"}

    url = f"https://api.tiny.com.br/public-api/v3/pedidos/{venda_id}"
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        response = await http_client.get(url, headers=headers)
        try:
            body = response.json()
        except Exception:
            body = response.text
        return {
            "ok": response.status_code == 200,
            "status_code": response.status_code,
            "venda_id": venda_id,
            "data": body
        }


@app.get("/admin/tiny_c/orders")
async def admin_tiny_c_orders(ids: str):
    """Busca múltiplos pedidos de Tiny C. ids = IDs separados por vírgula."""
    from app.tiny_oauth import ensure_access_token
    import httpx

    token = await ensure_access_token("B")
    if not token:
        return {"ok": False, "error": "No valid token for Tiny C — refaça o OAuth em /auth/c/start"}

    headers = {"Authorization": f"Bearer {token}"}
    results = []

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        for venda_id in [i.strip() for i in ids.split(",") if i.strip()]:
            url = f"https://api.tiny.com.br/public-api/v3/pedidos/{venda_id}"
            response = await http_client.get(url, headers=headers)
            try:
                body = response.json()
            except Exception:
                body = response.text
            results.append({
                "venda_id": venda_id,
                "status_code": response.status_code,
                "data": body
            })

    return {"ok": True, "total": len(results), "results": results}


@app.get("/admin/products_map")
async def admin_products_map():
    from app.db import get_products_map_list
    
    try:
        products = await get_products_map_list()
        mapeados = [p for p in products if p.get("id_c")]
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


@app.get("/admin/tiny_c/ping")
async def admin_tiny_c_ping(venda_id: str):
    from app.tiny_oauth import ensure_access_token
    from app.tiny_client import TinyClient
    
    token = await ensure_access_token("B")
    if not token:
        return {"ok": False, "error": "No valid token for account C (V365)"}
    
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
    for t in tokens:
        for k in ("expires_at", "updated_at"):
            if t.get(k) and hasattr(t[k], "isoformat"):
                t[k] = t[k].isoformat()
    return {"tokens": tokens}


@app.get("/admin/tokens/health")
async def admin_tokens_health():
    from app.tiny_oauth import ensure_access_token
    from app.tiny_client import TinyClient
    import asyncio
    results = {}
    async def check_account(account):
        try:
            token = await ensure_access_token(account)
            if not token:
                return {"ok": False, "error": "no_token"}
            client = TinyClient(token)
            ping = await client.ping_light()
            return {"ok": ping.ok, "status_code": ping.status_code, "error": ping.error}
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
    results["A"], results["B"] = await asyncio.gather(
        check_account("A"), check_account("B")
    )
    return results


@app.get("/dashboard")
async def dashboard():
    html_path = Path(__file__).parent / "static" / "dashboard.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"), headers={"Cache-Control": "no-cache"})


@app.get("/admin/server-info")
async def admin_server_info():
    return {"started_at": SERVER_STARTED_AT.isoformat()}


@app.get("/admin/flags")
async def admin_get_flags():
    from app.db import get_all_feature_flags
    flags = await get_all_feature_flags()
    for f in flags:
        if f.get('updated_at'):
            f['updated_at'] = f['updated_at'].isoformat()
    return {"flags": flags}


@app.post("/admin/flags")
async def admin_set_flag(request: Request):
    from app.db import set_feature_flag
    body = await request.json()
    key = body.get("key")
    enabled = body.get("enabled", False)
    if not key:
        return JSONResponse(status_code=400, content={"error": "missing key"})
    ok = await set_feature_flag(key, enabled)
    return {"ok": ok, "key": key, "enabled": enabled}


@app.get("/admin/jobs-dashboard")
async def admin_jobs_dashboard(limit: int = 10, status: str | None = None, job_type: str | None = None, exclude_status: str | None = None, exclude_noop: bool = False):
    from app.db import get_pool
    limit = min(limit, 50)
    p = await get_pool()
    async with p.acquire() as conn:
        query = "SELECT id, job_type, status, created_at, updated_at, action_preview, last_error, attempts, payload FROM public.jobs"
        conditions = []
        args = []
        idx = 1
        if status:
            conditions.append(f"status = ${idx}")
            args.append(status)
            idx += 1
        if exclude_status and not status:
            conditions.append(f"status != ${idx}")
            args.append(exclude_status)
            idx += 1
        if exclude_noop and not status:
            conditions.append("job_type != 'noop'")
        if job_type:
            conditions.append(f"job_type = ${idx}")
            args.append(job_type)
            idx += 1
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += f" ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ${idx}"
        args.append(limit)
        rows = await conn.fetch(query, *args)
        jobs = []
        for r in rows:
            d = dict(r)
            for k in ("created_at", "updated_at"):
                if d.get(k) and hasattr(d[k], "isoformat"):
                    d[k] = d[k].isoformat()
            for jk in ("action_preview", "payload"):
                if d.get(jk) and not isinstance(d[jk], (dict, list)):
                    try:
                        d[jk] = json.loads(str(d[jk]))
                    except Exception:
                        pass
            jobs.append(d)
        return {"jobs": jobs}


@app.get("/admin/events")
async def admin_events(limit: int = 5):
    from app.db import get_pool
    limit = min(limit, 50)
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, source, topic, venda_id, codigo_situacao, id_nota_fiscal, created_at, action_result
            FROM public.events
            ORDER BY created_at DESC
            LIMIT $1
        """, limit)
        events = []
        for r in rows:
            d = dict(r)
            if d.get("created_at"):
                d["created_at"] = d["created_at"].isoformat()
            events.append(d)
        return {"events": events}


@app.get("/admin/dashboard")
async def admin_dashboard():
    from app.db import get_dashboard_data
    data = await get_dashboard_data()
    if data.get("last_event") and data["last_event"].get("created_at"):
        data["last_event"]["created_at"] = data["last_event"]["created_at"].isoformat()
    for j in data.get("recent_jobs", []):
        for k in ("created_at", "updated_at"):
            if j.get(k):
                j[k] = j[k].isoformat()
        if j.get("action_preview") and not isinstance(j["action_preview"], (dict, list)):
            try:
                j["action_preview"] = json.loads(str(j["action_preview"]))
            except Exception:
                pass
    return data


@app.get("/auth/a/start")
async def auth_a_start():
    from fastapi.responses import RedirectResponse
    from app.tiny_oauth import build_auth_url
    url = build_auth_url("A")
    if not url:
        return {"error": "TINY_A_CLIENT_ID not configured"}
    return RedirectResponse(url=url, status_code=302)


@app.get("/auth/c/start")
async def auth_c_start():
    from fastapi.responses import RedirectResponse
    from app.tiny_oauth import build_auth_url
    url = build_auth_url("B")
    if not url:
        return {"error": "TINY_C_CLIENT_ID not configured"}
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


@app.get("/import")
async def import_page():
    return FileResponse("app/static/import.html")


@app.post("/admin/import/start")
async def admin_import_start(
    data_inicio: str = "2025-12-15",
    data_fim: str | None = None,
    dias: int | None = None,
    direction: str = "desc",
    limit_orders: int | None = None
):
    from app.db import has_running_import
    from app.backfill import start_import, compute_data_fim

    if await has_running_import():
        return JSONResponse(status_code=409, content={"error": "already_running", "message": "Já existe uma importação em andamento"})

    if dias and not data_fim:
        data_fim = compute_data_fim(data_inicio, dias)
    elif not data_fim:
        data_fim = "2026-03-19"

    run_id = await start_import(data_inicio, data_fim, direction, limit_orders)
    return {"ok": True, "run_id": run_id, "data_inicio": data_inicio, "data_fim": data_fim}


@app.get("/admin/import/{run_id}")
async def admin_import_detail(run_id: int, items_limit: int = 200, items_offset: int = 0):
    from app.db import get_import_run, get_import_run_items, count_import_run_created_in_c, count_requeueable_import_jobs

    run = await get_import_run(run_id)
    if not run:
        return JSONResponse(status_code=404, content={"error": "not_found"})

    items = await get_import_run_items(run_id, limit=items_limit, offset=items_offset)
    run["created_in_c"] = await count_import_run_created_in_c(run_id)
    run["requeueable"] = await count_requeueable_import_jobs(run_id)
    for key in ['started_at', 'finished_at', 'created_at']:
        if run.get(key):
            run[key] = str(run[key])
    for item in items:
        if item.get('created_at'):
            item['created_at'] = str(item['created_at'])

    return {"run": run, "items": items}


@app.get("/admin/import")
async def admin_import_list(limit: int = 5, offset: int = 0):
    from app.db import get_import_runs_list, count_import_run_created_in_c

    runs, total = await get_import_runs_list(limit=limit, offset=offset)
    for run in runs:
        run["created_in_c"] = await count_import_run_created_in_c(run["id"])
        for key in ['started_at', 'finished_at', 'created_at']:
            if run.get(key):
                run[key] = str(run[key])
    return {"runs": runs, "total": total}


@app.post("/admin/import/{run_id}/requeue")
async def admin_import_requeue(run_id: int):
    from app.db import requeue_import_run_jobs
    count = await requeue_import_run_jobs(run_id)
    return {"ok": True, "requeued": count}


@app.post("/admin/import/{run_id}/cancel")
async def admin_import_cancel(run_id: int):
    from app.backfill import cancel_import
    cancelled = cancel_import(run_id)
    return {"ok": cancelled, "run_id": run_id}


@app.get("/auth/c/callback")
async def auth_c_callback(code: str | None = None, error: str | None = None, error_description: str | None = None):
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
        return {"ok": True, "account": "C", "expires_in": expires_in}
    except Exception as e:
        return {"error": str(e)}
