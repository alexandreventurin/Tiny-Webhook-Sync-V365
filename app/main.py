import json
import asyncio
import base64
import hashlib
import hmac
import html
import logging
import os
import sys
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, RedirectResponse

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')

APP_BUILD = "2026-02-05-001"
SERVER_STARTED_AT = datetime.now(timezone.utc)

from app.db import (
    init_db, close_db, insert_event, insert_job,
    get_events_count, get_jobs_count_by_status,
    get_jobs_list, get_last_event_at, get_last_job_done_at,
    get_orders_a_list, get_order_a_snapshot, get_orders_map_list,
    check_is_echo, update_event_action_result,
    get_order_mapping_by_a, get_order_mapping_by_c,
    upsert_cancelled_order_review, mark_cancelled_order_reviews
)
from app.schemas import (
    WebhookResponse, HealthResponse, JobsListResponse, JobItem, RunJobsResponse,
    OrderAItem, OrderAListResponse, OrderASnapshotResponse
)
from app.utils import generate_event_key, generate_dedupe_key, determine_job_type, normalize_status, to_int_or_none
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

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "adm.muybela")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "Jesus!123456")
ADMIN_SESSION_COOKIE = "tiny_admin_session"
ADMIN_SESSION_MAX_AGE = int(os.getenv("ADMIN_SESSION_MAX_AGE", str(8 * 60 * 60)))
ADMIN_SESSION_SECRET = os.getenv("ADMIN_SESSION_SECRET") or hashlib.sha256(
    f"{ADMIN_USERNAME}:{ADMIN_PASSWORD}:{APP_BUILD}".encode("utf-8")
).hexdigest()
PROTECTED_PATHS = ("/admin", "/dashboard", "/import", "/orders-panel")


def _sign_session(message: str) -> str:
    return hmac.new(ADMIN_SESSION_SECRET.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def _encode_session(username: str) -> str:
    issued_at = str(int(time.time()))
    message = f"{username}:{issued_at}"
    token = f"{message}:{_sign_session(message)}"
    return base64.urlsafe_b64encode(token.encode("utf-8")).decode("ascii")


def _decode_session(cookie_value: str | None) -> str | None:
    if not cookie_value:
        return None
    try:
        raw = base64.urlsafe_b64decode(cookie_value.encode("ascii")).decode("utf-8")
        username, issued_at, signature = raw.rsplit(":", 2)
        message = f"{username}:{issued_at}"
        if not hmac.compare_digest(signature, _sign_session(message)):
            return None
        if int(time.time()) - int(issued_at) > ADMIN_SESSION_MAX_AGE:
            return None
        if not hmac.compare_digest(username, ADMIN_USERNAME):
            return None
        return username
    except Exception:
        return None


def _is_admin_authenticated(request: Request) -> bool:
    return _decode_session(request.cookies.get(ADMIN_SESSION_COOKIE)) is not None


def _login_url_for(request: Request) -> str:
    next_url = request.url.path
    if request.url.query:
        next_url += f"?{request.url.query}"
    return "/login?next=" + urllib.parse.quote(next_url, safe="")


def _wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept or request.url.path in ("/dashboard", "/import", "/orders-panel")


def approval_delay_minutes(now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    weekday = now.weekday()
    if weekday <= 3:
        days = 1
    elif weekday == 4:
        days = 3
    elif weekday == 5:
        days = 3
    else:
        days = 2
    target = now + timedelta(days=days)
    return max(1, int((target - now).total_seconds() // 60))


@app.middleware("http")
async def require_admin_login(request: Request, call_next):
    path = request.url.path
    is_protected = any(path == prefix or path.startswith(prefix + "/") for prefix in PROTECTED_PATHS)
    if is_protected and not _is_admin_authenticated(request):
        if _wants_html(request):
            return RedirectResponse(_login_url_for(request), status_code=303)
        return JSONResponse(status_code=401, content={"ok": False, "error": "login_required"})
    return await call_next(request)


def _login_html(error: str = "", next_url: str = "/orders-panel") -> str:
    error_html = f'<div class="error">{error}</div>' if error else ""
    safe_next = html.escape(next_url, quote=True)
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Login - Tiny Integrator</title>
<style>
*{{box-sizing:border-box}}body{{margin:0;min-height:100vh;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#eef2f7;color:#172033;display:grid;place-items:center;padding:24px}}
.login{{width:100%;max-width:380px;background:#fff;border:1px solid #d8e0ea;border-radius:8px;padding:28px;box-shadow:0 20px 60px rgba(31,41,55,.12)}}
h1{{margin:0 0 6px;font-size:22px;color:#111827}}p{{margin:0 0 22px;color:#607086;font-size:14px}}label{{display:block;font-size:12px;font-weight:700;color:#475569;margin:14px 0 6px;text-transform:uppercase;letter-spacing:.04em}}
input{{width:100%;height:42px;border:1px solid #cbd5e1;border-radius:6px;padding:0 12px;font-size:15px;color:#111827;background:#fbfdff}}input:focus{{outline:2px solid #93c5fd;border-color:#2563eb}}
button{{width:100%;height:42px;margin-top:18px;border:0;border-radius:6px;background:#1f3a5f;color:white;font-weight:700;font-size:14px;cursor:pointer}}button:hover{{background:#172c49}}
.error{{background:#fff1f2;border:1px solid #fecdd3;color:#be123c;border-radius:6px;padding:10px 12px;font-size:13px;margin-bottom:14px}}
</style>
</head>
<body>
<main class="login">
  <h1>Tiny Integrator</h1>
  <p>Acesso administrativo</p>
  {error_html}
  <form method="post" action="/login">
    <input type="hidden" name="next" value="{safe_next}">
    <label for="username">Login</label>
    <input id="username" name="username" autocomplete="username" autofocus>
    <label for="password">Senha</label>
    <input id="password" name="password" type="password" autocomplete="current-password">
    <button type="submit">Entrar</button>
  </form>
</main>
</body>
</html>"""


@app.get("/login")
async def login_page(next: str = "/orders-panel"):
    return HTMLResponse(_login_html(next_url=next), headers={"Cache-Control": "no-cache"})


@app.post("/login")
async def login_submit(request: Request):
    body = (await request.body()).decode("utf-8")
    form = urllib.parse.parse_qs(body)
    username = form.get("username", [""])[0]
    password = form.get("password", [""])[0]
    next_url = form.get("next", ["/orders-panel"])[0] or "/orders-panel"
    if not next_url.startswith("/"):
        next_url = "/orders-panel"
    if hmac.compare_digest(username, ADMIN_USERNAME) and hmac.compare_digest(password, ADMIN_PASSWORD):
        response = RedirectResponse(next_url, status_code=303)
        secure_cookie = request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https"
        response.set_cookie(
            ADMIN_SESSION_COOKIE,
            _encode_session(username),
            max_age=ADMIN_SESSION_MAX_AGE,
            httponly=True,
            secure=secure_cookie,
            samesite="lax",
        )
        return response
    return HTMLResponse(_login_html("Login ou senha inválidos.", next_url=next_url), status_code=401)


@app.get("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(ADMIN_SESSION_COOKIE)
    return response


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

        if source == "A" and topic == "vendas" and venda_id_int and normalize_status(codigo_situacao_str) == "cancelado":
            await upsert_cancelled_order_review(str(venda_id_int))
        
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
        initial_delay = approval_delay_minutes() if job_type == "approve_order_a" else (1 if job_type == "sync_tracking_c_to_a" else 0)
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


def _json_payload(value):
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


def _dig(data, path: str, default=None):
    current = data
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            idx = int(part)
            current = current[idx] if idx < len(current) else None
        else:
            return default
        if current is None:
            return default
    return current


def _normalize_text(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value).strip()


def _first_present(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _normalize_address(address: dict) -> dict:
    if not isinstance(address, dict):
        return {}
    return {
        "cep": _normalize_text(address.get("cep")).lower(),
        "uf": _normalize_text(address.get("uf")).lower(),
        "cidade": _normalize_text(address.get("municipio") or address.get("cidade")).lower(),
        "endereco": _normalize_text(address.get("endereco") or address.get("logradouro")).lower(),
        "numero": _normalize_text(address.get("numero")).lower(),
        "bairro": _normalize_text(address.get("bairro")).lower(),
        "complemento": _normalize_text(address.get("complemento")).lower(),
    }


def _object_name(value):
    if isinstance(value, dict):
        return _first_present(value.get("nome"), value.get("descricao"), value.get("formaEnvio"), value.get("formaFrete"))
    return value if value not in (None, "") else None


STATUS_LABELS = {
    "em_aberto": "em aberto",
    "faturado": "faturado",
    "cancelado": "cancelado",
    "aprovado": "aprovado",
    "preparando_envio": "preparando envio",
    "enviado": "enviado",
    "entregue": "entregue",
    "pronto_envio": "pronto envio",
    "dados_incompletos": "dados incompletos",
    "nao_entregue": "não entregue",
}


def _status_label(value) -> str | None:
    normalized = normalize_status(value)
    if not normalized:
        return None
    return STATUS_LABELS.get(normalized, str(normalized).replace("_", " "))


def _format_order_date(value) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    for separator in ("T", " "):
        if separator in text:
            text = text.split(separator)[0]
    try:
        return datetime.fromisoformat(text).strftime("%d/%m/%Y")
    except Exception:
        pass
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return f"{text[8:10]}/{text[5:7]}/{text[0:4]}"
    return text


def _money_value(value):
    if value in (None, ""):
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return value


def _order_display_fields(payload: dict) -> dict:
    payload = payload if isinstance(payload, dict) else {}
    cliente = payload.get("cliente") if isinstance(payload.get("cliente"), dict) else {}
    ecommerce = payload.get("ecommerce") if isinstance(payload.get("ecommerce"), dict) else {}
    endereco_entrega = payload.get("enderecoEntrega") if isinstance(payload.get("enderecoEntrega"), dict) else {}
    transportador = payload.get("transportador") if isinstance(payload.get("transportador"), dict) else {}
    forma_envio = payload.get("formaEnvio") if isinstance(payload.get("formaEnvio"), dict) else transportador.get("formaEnvio")
    forma_frete = payload.get("formaFrete") if isinstance(payload.get("formaFrete"), dict) else transportador.get("formaFrete")
    nota_fiscal = payload.get("notaFiscal") if isinstance(payload.get("notaFiscal"), dict) else {}
    itens = payload.get("itens") if isinstance(payload.get("itens"), list) else []

    total_produtos = _first_present(
        payload.get("valorProdutos"),
        payload.get("totalProdutos"),
        payload.get("valor_total_produtos"),
    )
    if total_produtos is None and itens:
        total = 0
        for item in itens:
            if not isinstance(item, dict):
                continue
            qtd = item.get("quantidade") or 0
            unit = item.get("valorUnitario") or item.get("valor_unitario") or 0
            try:
                total += float(qtd) * float(unit)
            except (TypeError, ValueError):
                pass
        total_produtos = total if total else None

    return {
        "nota_fiscal": _first_present(nota_fiscal.get("numero"), payload.get("numeroNotaFiscal"), payload.get("idNotaFiscal")),
        "numero_pedido": _first_present(payload.get("numero"), payload.get("numeroPedido")),
        "id_pedido": payload.get("id"),
        "data": _format_order_date(_first_present(payload.get("data"), payload.get("dataPedido"), payload.get("dataCriacao"))),
        "nome": cliente.get("nome"),
        "cpf": cliente.get("cpfCnpj") or cliente.get("cpf_cnpj"),
        "situacao": _status_label(payload.get("situacao")),
        "cep_entrega": endereco_entrega.get("cep"),
        "cidade_entrega": endereco_entrega.get("municipio") or endereco_entrega.get("cidade"),
        "uf_entrega": endereco_entrega.get("uf"),
        "numero_endereco": endereco_entrega.get("numero"),
        "complemento_endereco": endereco_entrega.get("complemento"),
        "itens": len(itens),
        "total_produtos": _money_value(total_produtos),
        "forma_envio": _first_present(_object_name(forma_envio), _object_name(transportador.get("formaEnvio"))),
        "forma_frete": _first_present(_object_name(forma_frete), _object_name(transportador.get("formaFrete"))),
        "codigo_rastreamento": _first_present(
            payload.get("codigoRastreamento"),
            payload.get("codigo_rastreamento"),
            payload.get("rastreamento"),
            transportador.get("codigoRastreamento"),
            transportador.get("codigo_rastreamento"),
        ),
        "ecommerce_nome": _first_present(
            ecommerce.get("nome"),
            ecommerce.get("nomeEcommerce"),
            ecommerce.get("canalVenda"),
            ecommerce.get("plataforma"),
            payload.get("ecommerceNome"),
        ),
        "numero_ecommerce": _first_present(
            ecommerce.get("numeroPedidoEcommerce"),
            ecommerce.get("numeroPedido"),
            ecommerce.get("pedido"),
            payload.get("numeroPedidoEcommerce"),
            payload.get("numeroOrdemCompra"),
        ),
    }


def _comparison_fields(origin: dict, destination: dict) -> list[dict]:
    origin_fields = _order_display_fields(origin)
    destination_fields = _order_display_fields(destination)
    definitions = [
        ("nota_fiscal", "Nota fiscal", False),
        ("numero_pedido", "Número do pedido", False),
        ("id_pedido", "ID do pedido", False),
        ("data", "Data", False),
        ("nome", "Nome", True),
        ("cpf", "CPF/CNPJ", True),
        ("situacao", "Situação", True),
        ("cep_entrega", "CEP entrega", True),
        ("cidade_entrega", "Cidade", True),
        ("uf_entrega", "UF", True),
        ("numero_endereco", "Nº endereço", True),
        ("complemento_endereco", "Complemento", True),
        ("itens", "Itens", True),
        ("total_produtos", "Total dos produtos", False),
        ("forma_envio", "Forma de envio", True),
        ("forma_frete", "Forma de frete", True),
        ("codigo_rastreamento", "Código de rastreamento", True),
        ("ecommerce_nome", "Nome do ecommerce", False),
        ("numero_ecommerce", "Número no ecommerce", True),
    ]
    rows = []
    for key, label, compare in definitions:
        origin_value = origin_fields.get(key)
        destination_value = destination_fields.get(key)
        differs = _normalize_text(origin_value) != _normalize_text(destination_value)
        rows.append({
            "key": key,
            "label": label,
            "origin": origin_value,
            "destination": destination_value,
            "differs": differs,
            "divergent": bool(compare and differs),
        })
    return rows


def _order_summary_from_snapshot(row: dict, mapped_product_ids: set[int] | None = None) -> dict:
    mapped_product_ids = mapped_product_ids or set()
    payload = _json_payload(row.get("fetched_payload")) or _json_payload(row.get("webhook_payload"))
    cliente = payload.get("cliente") if isinstance(payload, dict) else {}
    ecommerce = payload.get("ecommerce") if isinstance(payload, dict) else {}
    endereco_entrega = payload.get("enderecoEntrega") if isinstance(payload, dict) else {}
    endereco_faturamento = payload.get("endereco") if isinstance(payload, dict) else {}
    if not isinstance(endereco_faturamento, dict):
        endereco_faturamento = cliente.get("endereco") if isinstance(cliente, dict) else {}
    forma_envio = payload.get("formaEnvio") if isinstance(payload, dict) else {}
    forma_frete = payload.get("formaFrete") if isinstance(payload, dict) else {}
    transportador = payload.get("transportador") if isinstance(payload, dict) else {}
    if not isinstance(forma_envio, dict) and isinstance(transportador, dict):
        forma_envio = transportador.get("formaEnvio") or {}
    if not isinstance(forma_frete, dict) and isinstance(transportador, dict):
        forma_frete = transportador.get("formaFrete") or {}
    itens = payload.get("itens") if isinstance(payload, dict) else []
    if not isinstance(itens, list):
        itens = []
    if not isinstance(cliente, dict):
        cliente = {}
    if not isinstance(ecommerce, dict):
        ecommerce = {}
    if not isinstance(endereco_entrega, dict):
        endereco_entrega = {}
    if not isinstance(endereco_faturamento, dict):
        endereco_faturamento = {}
    if not isinstance(forma_envio, dict):
        forma_envio = {}
    if not isinstance(forma_frete, dict):
        forma_frete = {}
    if not isinstance(transportador, dict):
        transportador = {}

    billing_address = _normalize_address(endereco_faturamento)
    delivery_address = _normalize_address(endereco_entrega)
    has_delivery_address = any(delivery_address.values())
    address_differs = has_delivery_address and billing_address != delivery_address

    adjustment_reasons = []
    block_reasons = []
    if not cliente.get("nome"):
        adjustment_reasons.append("cliente_sem_nome")
    if not cliente.get("cpfCnpj"):
        adjustment_reasons.append("cliente_sem_cpf_cnpj")
    if not itens:
        adjustment_reasons.append("sem_itens")
    if not payload:
        adjustment_reasons.append("sem_detalhes_do_pedido")

    deposito = payload.get("deposito") if isinstance(payload, dict) else {}
    deposito_id = deposito.get("id") if isinstance(deposito, dict) else None
    if deposito_id and str(deposito_id) != "336403602":
        block_reasons.append("deposito_nao_exportavel")

    status_normalized = normalize_status(payload.get("situacao")) if isinstance(payload, dict) else None
    if status_normalized and status_normalized not in {"aprovado", "pronto_envio", "enviado", "entregue"}:
        block_reasons.append(f"status_nao_exportavel:{status_normalized}")

    missing_skus = []
    for item in itens:
        produto = item.get("produto") if isinstance(item, dict) else {}
        produto_id = produto.get("id") if isinstance(produto, dict) else None
        try:
            produto_id_int = int(produto_id) if produto_id is not None else None
        except (TypeError, ValueError):
            produto_id_int = None
        if produto_id_int and mapped_product_ids and produto_id_int not in mapped_product_ids:
            missing_skus.append({
                "id": produto_id_int,
                "sku": produto.get("sku") if isinstance(produto, dict) else None,
            })
    if missing_skus:
        adjustment_reasons.append("produto_sem_mapeamento")

    if block_reasons:
        export_category = "do_not_export"
    elif adjustment_reasons:
        export_category = "needs_adjustment"
    else:
        export_category = "valid"

    return {
        "venda_a_id": str(row.get("venda_a_id") or ""),
        "numero": payload.get("numero") or payload.get("numeroPedido") or ecommerce.get("numeroPedidoEcommerce"),
        "numero_ecommerce": _first_present(
            ecommerce.get("numeroPedidoEcommerce"),
            ecommerce.get("numeroPedido"),
            ecommerce.get("pedido"),
            payload.get("numeroPedidoEcommerce"),
        ),
        "ecommerce_nome": _first_present(
            ecommerce.get("nome"),
            ecommerce.get("nomeEcommerce"),
            ecommerce.get("canalVenda"),
            ecommerce.get("plataforma"),
            payload.get("ecommerceNome"),
        ),
        "cliente": cliente.get("nome"),
        "cpf_cnpj": cliente.get("cpfCnpj"),
        "situacao": payload.get("situacao"),
        "situacao_normalized": status_normalized,
        "situacao_label": _status_label(payload.get("situacao")),
        "data": _format_order_date(payload.get("data") or payload.get("dataPedido")),
        "hora": _first_present(payload.get("hora"), payload.get("horaPedido"), payload.get("horario")),
        "data_hora": _first_present(
            payload.get("dataHora"),
            payload.get("dataHoraPedido"),
            payload.get("dataCriacao"),
            payload.get("dataAtualizacao"),
            " ".join([str(x) for x in [payload.get("data") or payload.get("dataPedido"), _first_present(payload.get("hora"), payload.get("horaPedido"), payload.get("horario"))] if x]),
        ),
        "cidade": endereco_entrega.get("municipio") or endereco_entrega.get("cidade"),
        "uf": endereco_entrega.get("uf"),
        "forma_envio": _first_present(
            _object_name(forma_envio),
            _object_name(transportador.get("formaEnvio") if isinstance(transportador, dict) else None),
        ),
        "forma_frete": _first_present(
            _object_name(forma_frete),
            _object_name(transportador.get("formaFrete") if isinstance(transportador, dict) else None),
        ),
        "codigo_rastreamento": _first_present(
            payload.get("codigoRastreamento"),
            payload.get("codigo_rastreamento"),
            payload.get("rastreamento"),
            transportador.get("codigoRastreamento"),
            transportador.get("codigo_rastreamento"),
        ),
        "endereco_faturamento_diferente_entrega": address_differs,
        "itens": len(itens),
        "updated_at": row.get("updated_at"),
        "webhook_received_at": row.get("webhook_received_at") or row.get("created_at") or row.get("updated_at"),
        "approval_scheduled_at": row.get("approval_scheduled_at"),
        "transfer_scheduled_at": row.get("transfer_scheduled_at") or row.get("approval_scheduled_at"),
        "valid_for_export": export_category == "valid",
        "export_category": export_category,
        "adjustment_reasons": adjustment_reasons,
        "block_reasons": block_reasons,
        "missing_skus": missing_skus,
        "reasons": adjustment_reasons + block_reasons,
    }


def _diff_orders(origin: dict, destination: dict) -> list[dict]:
    checks = [
        ("Cliente", "cliente.nome", "cliente.nome"),
        ("CPF/CNPJ", "cliente.cpfCnpj", "cliente.cpfCnpj"),
        ("Situação", "situacao", "situacao"),
        ("CEP entrega", "enderecoEntrega.cep", "enderecoEntrega.cep"),
        ("UF entrega", "enderecoEntrega.uf", "enderecoEntrega.uf"),
        ("Cidade entrega", "enderecoEntrega.municipio", "enderecoEntrega.municipio"),
        ("Número e-commerce", "ecommerce.numeroPedidoEcommerce", "numeroOrdemCompra"),
        ("Qtd. itens", "itens", "itens"),
    ]
    differences = []
    for label, origin_path, destination_path in checks:
        origin_value = _dig(origin, origin_path)
        destination_value = _dig(destination, destination_path)
        if label == "Qtd. itens":
            origin_value = len(origin_value) if isinstance(origin_value, list) else 0
            destination_value = len(destination_value) if isinstance(destination_value, list) else 0
        if _normalize_text(origin_value) != _normalize_text(destination_value):
            differences.append({
                "label": label,
                "origin_path": origin_path,
                "destination_path": destination_path,
                "origin": origin_value,
                "destination": destination_value,
            })
    return differences


@app.get("/orders-panel")
async def orders_panel():
    return FileResponse("app/static/orders_panel.html")


@app.get("/admin/orders-panel/data")
async def admin_orders_panel_data(limit: int = 240, days: int = 30, divergence_limit: int = 80):
    from app.db import get_pool, upsert_orders_c_fetched, upsert_orders_c_fetch_error
    from app.tiny_client import TinyClient
    from app.tiny_oauth import ensure_access_token
    limit = max(1, min(limit, 500))
    days = max(1, min(days, 365))
    divergence_limit = max(0, min(divergence_limit, 120))
    p = await get_pool()
    async with p.acquire() as conn:
        origin_rows = await conn.fetch("""
            WITH source_orders AS (
                SELECT oas.*,
                       CASE
                           WHEN oas.fetched_payload::jsonb->>'data' ~ '^\\d{4}-\\d{2}-\\d{2}'
                           THEN substring(oas.fetched_payload::jsonb->>'data' from 1 for 10)::date
                           ELSE NULL
                       END AS order_date
                FROM public.orders_a_snapshot oas
            )
            SELECT so.venda_a_id, so.webhook_payload, so.fetched_payload, so.created_at, so.updated_at,
                   latest_event.created_at AS webhook_received_at,
                   approve_job.run_after AS approval_scheduled_at,
                   create_job.run_after AS transfer_scheduled_at
            FROM source_orders so
            LEFT JOIN public.orders_map om ON om.venda_a_id::text = so.venda_a_id
            LEFT JOIN LATERAL (
                SELECT created_at
                FROM public.events
                WHERE source = 'A'
                  AND topic = 'vendas'
                  AND venda_id = so.venda_a_id
                ORDER BY created_at DESC
                LIMIT 1
            ) latest_event ON true
            LEFT JOIN LATERAL (
                SELECT run_after
                FROM public.jobs
                WHERE dedupe_key = 'A:vendas:' || so.venda_a_id || ':approve_order_a'
                  AND status IN ('queued', 'running')
                ORDER BY created_at DESC
                LIMIT 1
            ) approve_job ON true
            LEFT JOIN LATERAL (
                SELECT run_after
                FROM public.jobs
                WHERE dedupe_key = 'A:vendas:' || so.venda_a_id || ':create_order_c'
                  AND status IN ('queued', 'running')
                ORDER BY created_at DESC
                LIMIT 1
            ) create_job ON true
            WHERE om.venda_a_id IS NULL
              AND (so.order_date IS NULL OR so.order_date >= CURRENT_DATE - ($2::int || ' days')::interval)
            ORDER BY COALESCE(so.order_date, so.updated_at::date) DESC, so.updated_at DESC
            LIMIT $1
        """, limit, days)
        synced_rows = await conn.fetch("""
            WITH mapped_orders AS (
                SELECT om.external_key, om.venda_a_id, om.venda_c_id, om.created_at, om.updated_at,
                       om.last_sync_status, om.last_sync_at, oas.fetched_payload,
                       ocs.fetched_payload AS fetched_payload_c, ocs.fetched_at AS fetched_at_c,
                       CASE
                           WHEN oas.fetched_payload::jsonb->>'data' ~ '^\\d{4}-\\d{2}-\\d{2}'
                           THEN substring(oas.fetched_payload::jsonb->>'data' from 1 for 10)::date
                           ELSE NULL
                       END AS order_date
                FROM public.orders_map om
                LEFT JOIN public.orders_a_snapshot oas ON oas.venda_a_id = om.venda_a_id::text
                LEFT JOIN public.orders_c_snapshot ocs ON ocs.venda_c_id = om.venda_c_id::text
            )
            SELECT external_key, venda_a_id, venda_c_id, created_at, updated_at,
                   last_sync_status, last_sync_at, fetched_payload, fetched_payload_c, fetched_at_c
            FROM mapped_orders
            WHERE order_date IS NULL OR order_date >= CURRENT_DATE - ($2::int || ' days')::interval
            ORDER BY COALESCE(order_date, updated_at::date) DESC, updated_at DESC
            LIMIT $1
        """, limit, days)
        cancelled_review_rows = await conn.fetch("""
            SELECT venda_a_id, status, created_at, reviewed_at, updated_at
            FROM public.cancelled_order_reviews
            ORDER BY updated_at DESC
            LIMIT $1
        """, limit)
        error_rows = await conn.fetch("""
            SELECT id, job_type, dedupe_key, status, payload, attempts, last_error, action_preview, created_at, updated_at
            FROM public.jobs
            WHERE status IN ('failed', 'dead', 'waiting_sku', 'skipped_not_mapped')
            ORDER BY updated_at DESC
            LIMIT $1
        """, limit)
        mapped_product_rows = await conn.fetch("""
            SELECT id_a
            FROM public.products_map
            WHERE id_c IS NOT NULL
        """)

    mapped_product_ids = {int(row["id_a"]) for row in mapped_product_rows if row["id_a"] is not None}
    cancelled_reviews = {str(row["venda_a_id"]): dict(row) for row in cancelled_review_rows}
    origin = [_order_summary_from_snapshot(dict(row), mapped_product_ids) for row in origin_rows]
    synced = []
    token_c_for_counts = await ensure_access_token("B") if divergence_limit else None
    client_c_for_counts = TinyClient(token_c_for_counts) if token_c_for_counts else None
    for row in synced_rows:
        item = dict(row)
        payload = _json_payload(item.get("fetched_payload"))
        origin_summary = _order_summary_from_snapshot({
            "venda_a_id": str(item.get("venda_a_id") or ""),
            "fetched_payload": payload,
            "updated_at": item.get("updated_at"),
        }, mapped_product_ids)
        item.update(origin_summary)
        destination_payload = _json_payload(item.get("fetched_payload_c"))
        if client_c_for_counts and len(synced) < divergence_limit and item.get("venda_c_id") and not destination_payload:
            try:
                destination_payload = await client_c_for_counts.get_order_details(str(item["venda_c_id"]))
                await upsert_orders_c_fetched(str(item["venda_c_id"]), destination_payload)
            except Exception as exc:
                await upsert_orders_c_fetch_error(str(item["venda_c_id"]), getattr(exc, "status_code", None), str(exc))
                item["destination_fetch_error"] = str(exc)[:160]
        destination_fields = _order_display_fields(destination_payload)
        destination_status = normalize_status(destination_payload.get("situacao") if isinstance(destination_payload, dict) else None)
        destination_status = destination_status or normalize_status(item.get("last_sync_status")) or "em_aberto"
        item["situacao_destino"] = destination_status
        item["situacao_destino_label"] = _status_label(destination_status)
        item["nota_fiscal_destino"] = destination_fields.get("nota_fiscal")
        item["numero_destino"] = destination_fields.get("numero_pedido")
        item["destination_snapshot_at"] = item.get("fetched_at_c")
        item["forma_envio_divergent"] = (
            _normalize_text(origin_summary.get("forma_envio")) != _normalize_text(destination_fields.get("forma_envio"))
            or _normalize_text(origin_summary.get("forma_frete")) != _normalize_text(destination_fields.get("forma_frete"))
        )
        item["codigo_rastreamento_divergent"] = (
            _normalize_text(origin_summary.get("codigo_rastreamento")) != _normalize_text(destination_fields.get("codigo_rastreamento"))
        )
        item["situacao_a"] = payload.get("situacao") if isinstance(payload, dict) else None
        item["situacao_a_label"] = _status_label(item.get("situacao_a"))
        item["last_job_status"] = item.get("last_sync_status")
        review = cancelled_reviews.get(str(item.get("venda_a_id")))
        if review:
            item["cancel_review_status"] = review.get("status")
            item["cancel_reviewed_at"] = review.get("reviewed_at")
            item["cancel_review_created_at"] = review.get("created_at")
        item["action_preview"] = {}
        item["divergence_count"] = None
        if destination_payload:
            try:
                fields = _comparison_fields(payload, destination_payload)
                item["divergence_count"] = len([field for field in fields if field["divergent"]])
            except Exception as exc:
                item["divergence_count_error"] = str(exc)[:160]
        synced.append(item)

    errors = []
    for row in error_rows:
        item = dict(row)
        item["payload"] = _json_payload(item.get("payload"))
        item["action_preview"] = _json_payload(item.get("action_preview"))
        errors.append(item)

    return {
        "origin": {
            "valid": [item for item in origin if item["valid_for_export"]],
            "needs_adjustment": [item for item in origin if item["export_category"] == "needs_adjustment"],
            "do_not_export": [item for item in origin if item["export_category"] == "do_not_export"],
        },
        "synced": synced,
        "cancelled": {
            "pending": [item for item in synced if item.get("cancel_review_status") == "pending"],
            "reviewed": [item for item in synced if item.get("cancel_review_status") == "reviewed"],
        },
        "errors": errors,
        "period_days": days,
    }


@app.post("/admin/orders-panel/cancelled/mark-reviewed")
async def admin_orders_panel_cancelled_mark_reviewed(request: Request):
    payload = await request.json()
    ids = payload.get("venda_a_ids") if isinstance(payload, dict) else []
    ids = [str(item) for item in ids if item]
    updated = await mark_cancelled_order_reviews(ids)
    return {"ok": True, "updated": updated}


@app.get("/admin/orders-panel/detail")
async def admin_orders_panel_detail(venda_a_id: str | None = None, venda_c_id: str | None = None, job_id: int | None = None):
    from app.db import get_pool, upsert_orders_c_fetched, upsert_orders_c_fetch_error
    p = await get_pool()
    origin_payload = {}
    destination_snapshot_payload = {}
    mapping = None
    related_jobs = []

    async with p.acquire() as conn:
        if job_id and not venda_a_id and not venda_c_id:
            job = await conn.fetchrow("SELECT payload FROM public.jobs WHERE id = $1", job_id)
            payload = _json_payload(job["payload"]) if job else {}
            venda_a_id = payload.get("venda_a_id") or payload.get("venda_id")
            venda_c_id = payload.get("venda_c_id")
        if venda_a_id:
            origin_row = await conn.fetchrow("""
                SELECT fetched_payload, webhook_payload
                FROM public.orders_a_snapshot
                WHERE venda_a_id = $1
            """, str(venda_a_id))
            if origin_row:
                origin_payload = _json_payload(origin_row["fetched_payload"]) or _json_payload(origin_row["webhook_payload"])
            mapping = await conn.fetchrow("""
                SELECT external_key, venda_a_id, venda_c_id, created_at, updated_at, last_sync_status, last_sync_at
                FROM public.orders_map
                WHERE venda_a_id::text = $1
            """, str(venda_a_id))
            if mapping and not venda_c_id:
                venda_c_id = str(mapping["venda_c_id"]) if mapping["venda_c_id"] else None
        if venda_c_id:
            destination_row = await conn.fetchrow("""
                SELECT fetched_payload
                FROM public.orders_c_snapshot
                WHERE venda_c_id = $1
            """, str(venda_c_id))
            if destination_row:
                destination_snapshot_payload = _json_payload(destination_row["fetched_payload"])
        related_jobs = await conn.fetch("""
            SELECT id, job_type, status, attempts, last_error, action_preview, created_at, updated_at
            FROM public.jobs
            WHERE ($1::text IS NOT NULL AND (payload::jsonb->>'venda_id' = $1 OR payload::jsonb->>'venda_a_id' = $1))
               OR ($2::text IS NOT NULL AND payload::jsonb->>'venda_c_id' = $2)
            ORDER BY updated_at DESC
            LIMIT 20
        """, str(venda_a_id) if venda_a_id else None, str(venda_c_id) if venda_c_id else None)

    destination_payload = {}
    destination_error = None
    if venda_c_id:
        try:
            from app.tiny_oauth import ensure_access_token
            from app.tiny_client import TinyClient
            token = await ensure_access_token("B")
            if token:
                destination_payload = await TinyClient(token).get_order_details(str(venda_c_id))
                await upsert_orders_c_fetched(str(venda_c_id), destination_payload)
            else:
                destination_error = "Token do Tiny destino indisponível."
        except Exception as exc:
            await upsert_orders_c_fetch_error(str(venda_c_id), getattr(exc, "status_code", None), str(exc))
            destination_error = str(exc)
        if not destination_payload and destination_snapshot_payload:
            destination_payload = destination_snapshot_payload

    comparison_fields = _comparison_fields(origin_payload, destination_payload) if destination_payload else _comparison_fields(origin_payload, {})
    divergence_count = len([field for field in comparison_fields if field["divergent"]])

    return {
        "venda_a_id": venda_a_id,
        "venda_c_id": venda_c_id,
        "mapping": dict(mapping) if mapping else None,
        "origin": origin_payload,
        "destination": destination_payload,
        "destination_error": destination_error,
        "fields": comparison_fields,
        "divergence_count": divergence_count,
        "differences": [field for field in comparison_fields if field["divergent"]],
        "jobs": [dict(row) for row in related_jobs],
    }


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
