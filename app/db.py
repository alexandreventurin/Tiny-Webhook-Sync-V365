import asyncpg
import ssl
import json
import logging
from typing import Optional
from datetime import datetime

from app.settings import settings

pool: Optional[asyncpg.Pool] = None
logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 5


async def init_db():
    global pool
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    try:
        pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=1,
            max_size=10,
            ssl=ssl_context,
            command_timeout=60,
            timeout=30,
            statement_cache_size=0
        )
        
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.events (
                    id SERIAL PRIMARY KEY,
                    event_key TEXT UNIQUE NOT NULL,
                    source TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    venda_id INTEGER,
                    codigo_situacao INTEGER,
                    id_nota_fiscal INTEGER,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.jobs (
                    id SERIAL PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    dedupe_key TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    event_id INTEGER REFERENCES public.events(id),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.orders_map (
                    id SERIAL PRIMARY KEY,
                    external_key TEXT UNIQUE NOT NULL,
                    venda_a_id INTEGER,
                    venda_b_id INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON public.jobs(status)
            """)
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS event_id INTEGER")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS payload JSONB")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS attempts INTEGER DEFAULT 0")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS run_after TIMESTAMPTZ DEFAULT NOW()")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS last_error TEXT")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS locked_by TEXT")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.jobs ADD COLUMN IF NOT EXISTS action_preview JSONB")
            except Exception:
                pass
                
        logger.info("Database connected and tables created")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


async def close_db():
    global pool
    if pool:
        await pool.close()


async def get_pool() -> asyncpg.Pool:
    if pool is None:
        raise RuntimeError("Database pool not initialized")
    return pool


async def insert_event(
    event_key: str,
    source: str,
    topic: str,
    venda_id: int | None,
    codigo_situacao: int | None,
    id_nota_fiscal: int | None,
    payload: str
) -> int | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO public.events (event_key, source, topic, venda_id, codigo_situacao, id_nota_fiscal, payload)
            VALUES ($1::text, $2::text, $3::text, $4::integer, $5::integer, $6::integer, $7::jsonb)
            ON CONFLICT (event_key) DO NOTHING
            RETURNING id
        """, event_key, source, topic, venda_id, codigo_situacao, id_nota_fiscal, payload)
        return row["id"] if row else None


async def insert_job(job_type: str, dedupe_key: str, event_id: int | None, payload: dict | None = None) -> bool:
    p = await get_pool()
    payload_str = json.dumps(payload) if payload else None
    async with p.acquire() as conn:
        try:
            result = await conn.execute("""
                INSERT INTO public.jobs (job_type, dedupe_key, status, event_id, payload)
                VALUES ($1::text, $2::text, 'queued', $3::integer, $4::jsonb)
                ON CONFLICT (dedupe_key) DO NOTHING
            """, job_type, dedupe_key, event_id, payload_str)
            return result == "INSERT 0 1"
        except Exception:
            try:
                result = await conn.execute("""
                    INSERT INTO public.jobs (job_type, dedupe_key, status)
                    VALUES ($1::text, $2::text, 'queued')
                    ON CONFLICT (dedupe_key) DO NOTHING
                """, job_type, dedupe_key)
                return result == "INSERT 0 1"
            except Exception as e:
                logger.error(f"Failed to insert job: {e}")
                return False


async def fetch_and_lock_jobs(limit: int = 25) -> list[dict]:
    p = await get_pool()
    locked_jobs = []
    
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, job_type, dedupe_key, payload, attempts
            FROM public.jobs
            WHERE status = 'queued' AND (run_after IS NULL OR run_after <= NOW())
            ORDER BY created_at ASC
            LIMIT $1
        """, limit)
        
        for row in rows:
            result = await conn.execute("""
                UPDATE public.jobs
                SET status = 'running', locked_at = NOW(), locked_by = 'worker'
                WHERE id = $1 AND status = 'queued'
            """, row['id'])
            
            if result == "UPDATE 1":
                locked_jobs.append(dict(row))
    
    return locked_jobs


async def update_job_done(job_id, action_preview: dict) -> None:
    p = await get_pool()
    action_preview_str = json.dumps(action_preview)
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                UPDATE public.jobs
                SET status = 'done', action_preview = $2::jsonb
                WHERE id = $1
            """, job_id, action_preview_str)
        except Exception:
            try:
                await conn.execute("""
                    UPDATE public.jobs
                    SET status = 'done'
                    WHERE id = $1
                """, job_id)
            except Exception as e:
                logger.error(f"Failed to update job done: {e}")


async def update_job_failed(job_id, error: str, attempts: int) -> None:
    p = await get_pool()
    new_status = 'dead' if attempts >= MAX_ATTEMPTS else 'failed'
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                UPDATE public.jobs
                SET status = $2, last_error = $3, attempts = $4, last_attempt_at = NOW()
                WHERE id = $1
            """, job_id, new_status, error, attempts)
        except Exception:
            try:
                await conn.execute("""
                    UPDATE public.jobs
                    SET status = $2
                    WHERE id = $1
                """, job_id, new_status)
            except Exception as e:
                logger.error(f"Failed to update job failed status: {e}")


async def upsert_orders_map(external_key: str, venda_a_id: int | None) -> None:
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO public.orders_map (external_key, venda_a_id)
                VALUES ($1::text, $2::integer)
                ON CONFLICT (external_key) DO UPDATE SET venda_a_id = $2::integer
            """, external_key, venda_a_id)
        except Exception as e:
            logger.error(f"Failed to upsert orders_map: {e}")


async def get_events_count() -> int:
    p = await get_pool()
    async with p.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM public.events")


async def get_jobs_count_by_status(status: str) -> int:
    p = await get_pool()
    async with p.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM public.jobs WHERE status = $1", status)


async def get_last_event_at() -> datetime | None:
    p = await get_pool()
    async with p.acquire() as conn:
        return await conn.fetchval("SELECT MAX(created_at) FROM public.events")


async def get_last_job_done_at() -> datetime | None:
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            return await conn.fetchval("SELECT MAX(created_at) FROM public.jobs WHERE status = 'done'")
        except Exception:
            return None


async def get_jobs_list(status: str, limit: int) -> list[dict]:
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, job_type, dedupe_key, status, created_at
            FROM public.jobs
            WHERE status = $1
            ORDER BY created_at DESC
            LIMIT $2
        """, status, limit)
        return [dict(row) for row in rows]
