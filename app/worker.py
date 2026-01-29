import asyncio
import json
import logging
from typing import Any

from app.db import (
    fetch_and_lock_jobs,
    update_job_done,
    update_job_failed,
    upsert_orders_map
)

logger = logging.getLogger(__name__)

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
    
    try:
        if job_type == 'create_order_b':
            venda_id = None
            if isinstance(payload, dict):
                dados = payload.get('dados', {})
                if isinstance(dados, dict):
                    venda_id = dados.get('id')
                if venda_id is None:
                    venda_id = payload.get('venda_id')
            
            external_key = f"A:{venda_id}" if venda_id else "A:unknown"
            
            await upsert_orders_map(external_key=external_key, venda_a_id=venda_id)
            
            action_preview = {
                "would": "create_order_in_B",
                "external_key": external_key,
                "venda_a_id": venda_id
            }
            
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: create_order_b for {external_key}")
        
        elif job_type == 'sync_status':
            action_preview = {
                "would": "sync_status",
                "details": payload
            }
            
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: sync_status")
        
        elif job_type == 'noop':
            action_preview = {"would": "noop"}
            
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: noop")
        
        else:
            action_preview = {"would": "unknown", "job_type": job_type}
            await update_job_done(job_id, action_preview)
            logger.warning(f"Job {job_id} completed with unknown job_type: {job_type}")
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Job {job_id} failed: {error_msg}")
        await update_job_failed(job_id, error_msg, attempts)


async def run_worker_once(limit: int = 25) -> int:
    jobs = await fetch_and_lock_jobs(limit=limit)
    
    if not jobs:
        return 0
    
    for job in jobs:
        await process_job(job)
    
    return len(jobs)


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
