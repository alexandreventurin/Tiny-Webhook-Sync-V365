import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from app.db import (
    fetch_and_lock_jobs,
    update_job_done,
    update_job_failed,
    upsert_orders_map,
    upsert_orders_a_snapshot,
    upsert_orders_a_fetched,
    upsert_orders_map_with_b,
    get_order_a_snapshot,
    get_snapshot_fetched_at,
    insert_job,
    reset_stale_locks
)
from app.settings import (
    TINY_A_TOKEN, TINY_B_TOKEN, 
    ENABLE_FETCH_A, EXECUTE_TINY_B, 
    ALLOW_VENDA_IDS, FETCH_CACHE_MINUTES
)
from app.tiny_client import TinyClient

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

WORKER_BUILD = "2026-01-30-002"

worker_running = False


async def process_job(job: dict) -> None:
    job_id = job['id']
    job_type = job['job_type']
    payload = job.get('payload')
    attempts = (job.get('attempts') or 0) + 1
    
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    elif payload is None:
        payload = {}
    
    source = payload.get('source')
    venda_id = payload.get('venda_id')
    codigo_situacao = payload.get('codigo_situacao')
    id_nota_fiscal = payload.get('id_nota_fiscal')
    topic = payload.get('topic')
    
    try:
        if job_type == 'create_order_b':
            if not venda_id:
                await update_job_failed(job_id, "missing_venda_id", attempts)
                logger.warning(f"Job {job_id} failed: missing_venda_id")
                return
            
            external_key = f"A:{venda_id}"
            snapshot = await get_order_a_snapshot(str(venda_id))
            fetched_payload = snapshot.get('fetched_payload') if snapshot else None
            
            if fetched_payload and isinstance(fetched_payload, str):
                fetched_payload = json.loads(fetched_payload)
            
            order_data = fetched_payload or {}
            cliente = order_data.get('cliente') or {}
            endereco = order_data.get('endereco') or cliente.get('endereco') or {}
            itens = order_data.get('itens') or []
            
            missing_fields = []
            if not cliente.get('nome'):
                missing_fields.append('cliente.nome')
            if not endereco.get('endereco') and not endereco.get('logradouro'):
                missing_fields.append('endereco')
            if not itens:
                missing_fields.append('itens')
            
            if not EXECUTE_TINY_B:
                await upsert_orders_map(external_key=external_key, venda_a_id=str(venda_id))
                action_preview = {
                    "would": "create_order_in_B",
                    "situacao_target": 8,
                    "venda_a_id": venda_id,
                    "external_key": external_key,
                    "dry_run": True,
                    "has_fetched_payload": fetched_payload is not None,
                    "missing_fields": missing_fields if missing_fields else None,
                    "note": "EXECUTE_TINY_B=false"
                }
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} completed: create_order_b dry-run for {external_key}")
                return
            
            if missing_fields:
                await update_job_failed(job_id, f"missing_required_fields: {missing_fields}", attempts)
                logger.warning(f"Job {job_id} failed: missing required fields {missing_fields}")
                return
            
            if not TINY_B_TOKEN:
                await update_job_failed(job_id, "TINY_B_TOKEN not configured", attempts)
                logger.warning(f"Job {job_id} failed: TINY_B_TOKEN not set")
                return
            
            order_payload_b = {
                "situacao": 8,
                "cliente": cliente,
                "endereco": endereco,
                "itens": itens,
                "observacoes": f"Importado de A:{venda_id}"
            }
            
            client_b = TinyClient(TINY_B_TOKEN)
            result = await client_b.create_order(order_payload_b)
            venda_b_id = str(result.get('id') or result.get('idPedido') or '')
            
            await upsert_orders_map_with_b(external_key=external_key, venda_a_id=str(venda_id), venda_b_id=venda_b_id)
            
            action_preview = {
                "would": "create_order_in_B",
                "situacao_target": 8,
                "venda_a_id": venda_id,
                "venda_b_id": venda_b_id,
                "external_key": external_key,
                "created": True
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: created order in B with id {venda_b_id}")
        
        elif job_type == 'fetch_order_a':
            if not venda_id:
                await update_job_failed(job_id, "missing_venda_id", attempts)
                logger.warning(f"Job {job_id} failed: missing_venda_id")
                return
            
            webhook_payload_raw = payload.get('webhook_payload') or payload
            await upsert_orders_a_snapshot(venda_a_id=str(venda_id), webhook_payload=webhook_payload_raw)
            
            if not ENABLE_FETCH_A:
                action_preview = {
                    "would": "fetch_order_a",
                    "venda_a_id": venda_id,
                    "skipped": True,
                    "reason": "ENABLE_FETCH_A=false"
                }
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: ENABLE_FETCH_A=false")
                return
            
            if ALLOW_VENDA_IDS and str(venda_id) not in ALLOW_VENDA_IDS:
                action_preview = {
                    "would": "fetch_order_a",
                    "venda_a_id": venda_id,
                    "skipped": True,
                    "reason": "not_in_allowlist"
                }
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: venda_id not in allowlist")
                return
            
            fetched_at = await get_snapshot_fetched_at(str(venda_id))
            if fetched_at:
                age_minutes = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 60
                if age_minutes < FETCH_CACHE_MINUTES:
                    action_preview = {
                        "would": "fetch_order_a",
                        "venda_a_id": venda_id,
                        "skipped": True,
                        "reason": f"cached (fetched {int(age_minutes)} min ago)"
                    }
                    await update_job_done(job_id, action_preview)
                    logger.info(f"Job {job_id} skipped: cached fetch")
                    create_order_dedupe_key = f"A:vendas:{venda_id}:create_order_b"
                    create_order_payload = {
                        "source": "A", "topic": "vendas", "venda_id": str(venda_id),
                        "codigo_situacao": codigo_situacao, "id_nota_fiscal": id_nota_fiscal,
                        "from_fetch_order_a": True
                    }
                    await insert_job(job_type="create_order_b", dedupe_key=create_order_dedupe_key, event_id=None, payload=create_order_payload)
                    return
            
            if not TINY_A_TOKEN:
                action_preview = {
                    "would": "fetch_order_a",
                    "venda_a_id": venda_id,
                    "skipped": True,
                    "reason": "TINY_A_TOKEN not configured"
                }
                await update_job_done(job_id, action_preview)
                logger.warning(f"Job {job_id} skipped: TINY_A_TOKEN not set")
                create_order_dedupe_key = f"A:vendas:{venda_id}:create_order_b"
                create_order_payload = {
                    "source": "A", "topic": "vendas", "venda_id": str(venda_id),
                    "codigo_situacao": codigo_situacao, "id_nota_fiscal": id_nota_fiscal,
                    "from_fetch_order_a": True
                }
                await insert_job(job_type="create_order_b", dedupe_key=create_order_dedupe_key, event_id=None, payload=create_order_payload)
                return
            
            client_a = TinyClient(TINY_A_TOKEN)
            fetched_data = await client_a.get_order_details(str(venda_id))
            await upsert_orders_a_fetched(venda_a_id=str(venda_id), fetched_payload=fetched_data)
            
            action_preview = {
                "would": "fetch_order_a",
                "venda_a_id": venda_id,
                "fetched": True,
                "note": "fetched from Tiny A"
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: fetch_order_a for venda {venda_id}")
            
            create_order_dedupe_key = f"A:vendas:{venda_id}:create_order_b"
            create_order_payload = {
                "source": "A",
                "topic": "vendas",
                "venda_id": str(venda_id),
                "codigo_situacao": codigo_situacao,
                "id_nota_fiscal": id_nota_fiscal,
                "from_fetch_order_a": True
            }
            await insert_job(job_type="create_order_b", dedupe_key=create_order_dedupe_key, event_id=None, payload=create_order_payload)
            logger.info(f"Chained create_order_b job for venda {venda_id}")
        
        elif job_type == 'sync_status':
            action_preview = {
                "would": "sync_status",
                "source": source,
                "topic": topic,
                "venda_id": venda_id,
                "codigo_situacao": codigo_situacao,
                "id_nota_fiscal": id_nota_fiscal
            }
            
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: sync_status")
        
        elif job_type == 'sync_nf_link':
            url_danfe = payload.get('url_danfe')
            action_preview = {
                "would": "sync_nf_link",
                "id_nota_fiscal": id_nota_fiscal,
                "url_danfe": url_danfe,
                "note": "later we will call GET /notas/{idNota}/link or /notas/{idNota} to resolve venda"
            }
            
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: sync_nf_link for NF {id_nota_fiscal}")
        
        elif job_type == 'noop':
            action_preview = {
                "would": "noop",
                "received_status": codigo_situacao,
                "source": source,
                "topic": topic
            }
            
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: noop")
        
        else:
            action_preview = {
                "would": "unknown",
                "job_type": job_type,
                "source": source,
                "topic": topic
            }
            await update_job_done(job_id, action_preview)
            logger.warning(f"Job {job_id} completed with unknown job_type: {job_type}")
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Job {job_id} failed: {error_msg}")
        await update_job_failed(job_id, error_msg, attempts)


async def run_worker_once(limit: int = 25) -> int:
    await reset_stale_locks()
    
    jobs = await fetch_and_lock_jobs(limit=limit)
    
    if not jobs:
        return 0
    
    for job in jobs:
        await process_job(job)
    
    return len(jobs)


async def run_worker_once_detailed(limit: int = 50) -> dict:
    await reset_stale_locks()
    
    jobs = await fetch_and_lock_jobs(limit=limit)
    locked = len(jobs)
    done = 0
    failed = 0
    dead = 0
    
    for job in jobs:
        job_id = job['id']
        attempts = (job.get('attempts') or 0) + 1
        try:
            await process_job(job)
            done += 1
        except Exception as e:
            logger.error(f"Job {job_id} exception: {e}")
            if attempts >= 5:
                dead += 1
            else:
                failed += 1
            await update_job_failed(job_id, str(e), attempts)
    
    return {"locked": locked, "done": done, "failed": failed, "dead": dead}


async def worker_loop():
    global worker_running
    worker_running = True
    logger.info("Worker started")
    
    while worker_running:
        try:
            processed = await run_worker_once(limit=25)
            if processed > 0:
                logger.info(f"Worker processed {processed} jobs")
        except Exception as e:
            logger.error(f"Worker error: {e}")
        
        await asyncio.sleep(5)
    
    logger.info("Worker stopped")


def stop_worker():
    global worker_running
    worker_running = False
