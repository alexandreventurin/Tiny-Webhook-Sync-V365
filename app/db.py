import asyncpg
from contextlib import asynccontextmanager
from typing import Optional

from app.settings import settings

pool: Optional[asyncpg.Pool] = None


async def init_db():
    global pool
    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=10)
    
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
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON public.jobs(status)
        """)


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
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            ON CONFLICT (event_key) DO NOTHING
            RETURNING id
        """, event_key, source, topic, venda_id, codigo_situacao, id_nota_fiscal, payload)
        return row["id"] if row else None


async def insert_job(job_type: str, dedupe_key: str, event_id: int | None) -> bool:
    p = await get_pool()
    async with p.acquire() as conn:
        result = await conn.execute("""
            INSERT INTO public.jobs (job_type, dedupe_key, status, event_id)
            VALUES ($1, $2, 'queued', $3)
            ON CONFLICT (dedupe_key) DO NOTHING
        """, job_type, dedupe_key, event_id)
        return result == "INSERT 0 1"


async def get_events_count() -> int:
    p = await get_pool()
    async with p.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM public.events")


async def get_jobs_queued_count() -> int:
    p = await get_pool()
    async with p.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM public.jobs WHERE status = 'queued'")


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
