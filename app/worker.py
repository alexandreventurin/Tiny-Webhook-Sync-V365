import asyncio
import json
import logging
import sys
from typing import Any

from app.db import (
    fetch_and_lock_jobs,
    update_job_done,
    update_job_failed,
    upsert_orders_map,
    upsert_orders_a_snapshot,
    insert_job,
    reset_stale_locks
)

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

WORKER_BUILD = "2026-01-30-001"

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
            
            await upsert_orders_map(external_key=external_key, venda_a_id=str(venda_id))
            
            required_fields = ["nome_cliente", "endereco", "itens"]
            payload_fields_missing = [f for f in required_fields if f not in payload or not payload.get(f)]
            
            action_preview = {
                "would": "create_order_in_B",
                "status_target": "dados_incompletos",
                "venda_a_id": venda_id,
                "external_key": external_key,
                "payload_fields_missing": payload_fields_missing,
                "note": "dry-run: not calling Tiny B API yet"
            }
            
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: create_order_b for {external_key}")
        
        elif job_type == 'fetch_order_a':
            if not venda_id:
                await update_job_failed(job_id, "missing_venda_id", attempts)
                logger.warning(f"Job {job_id} failed: missing_venda_id")
                return
            
            webhook_payload_raw = payload.get('webhook_payload') or payload
            await upsert_orders_a_snapshot(venda_a_id=str(venda_id), webhook_payload=webhook_payload_raw)
            
            action_preview = {
                "would": "fetch_order_a",
                "venda_a_id": venda_id,
                "note": "dry-run: not calling Tiny yet"
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
