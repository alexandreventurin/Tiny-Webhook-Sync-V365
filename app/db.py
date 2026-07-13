import asyncpg
import ssl
import json
import logging
import os
from typing import Optional
from datetime import datetime

from app.settings import DATABASE_URL

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
            DATABASE_URL,
            min_size=1,
            max_size=int(os.getenv("DB_POOL_MAX_SIZE", "3")),
            ssl=ssl_context,
            command_timeout=int(os.getenv("DB_COMMAND_TIMEOUT_SECONDS", "30")),
            timeout=int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "20")),
            statement_cache_size=0
        )

        if os.getenv("RUN_DB_SCHEMA_ON_STARTUP", "0").lower() not in ("1", "true", "yes"):
            logger.info("Database connected; schema initialization skipped on startup")
            return
        
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.events (
                    id SERIAL PRIMARY KEY,
                    event_key TEXT UNIQUE NOT NULL,
                    source TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    venda_id TEXT,
                    codigo_situacao TEXT,
                    id_nota_fiscal TEXT,
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
                    venda_c_id INTEGER,
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
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.orders_a_snapshot (
                    id SERIAL PRIMARY KEY,
                    venda_a_id TEXT UNIQUE NOT NULL,
                    webhook_payload JSONB,
                    fetched_payload JSONB,
                    fetched_at TIMESTAMPTZ,
                    needs_fetch BOOLEAN DEFAULT true,
                    last_error JSONB,
                    notes TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.orders_c_snapshot (
                    id SERIAL PRIMARY KEY,
                    venda_c_id TEXT UNIQUE NOT NULL,
                    fetched_payload JSONB,
                    fetched_at TIMESTAMPTZ,
                    last_error JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.cancelled_order_reviews (
                    id SERIAL PRIMARY KEY,
                    venda_a_id TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    reviewed_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            
            try:
                await conn.execute("ALTER TABLE public.orders_map ADD COLUMN IF NOT EXISTS last_sync_status TEXT")
            except Exception:
                pass
            try:
                await conn.execute("ALTER TABLE public.orders_map ADD COLUMN IF NOT EXISTS last_sync_at TIMESTAMPTZ")
            except Exception:
                pass

            try:
                await conn.execute("ALTER TABLE public.events ADD COLUMN IF NOT EXISTS action_result TEXT")
            except Exception:
                pass

            try:
                await conn.execute("ALTER TABLE public.orders_a_snapshot ADD COLUMN IF NOT EXISTS last_error JSONB")
            except Exception:
                pass
            
            try:
                await conn.execute("ALTER TABLE public.orders_a_snapshot ADD COLUMN IF NOT EXISTS needs_fetch BOOLEAN DEFAULT true")
            except Exception:
                pass
                
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.feature_flags (
                    key TEXT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT false,
                    functional BOOLEAN NOT NULL DEFAULT false,
                    label TEXT,
                    description TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.products_map (
                    id_a INTEGER PRIMARY KEY,
                    id_c INTEGER,
                    sku TEXT,
                    descricao TEXT,
                    situacao TEXT,
                    ativo BOOLEAN DEFAULT true,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            try:
                await conn.execute("ALTER TABLE public.products_map ADD COLUMN IF NOT EXISTS preco NUMERIC")
            except Exception:
                pass

            await conn.execute("""
                UPDATE public.products_map SET preco = CASE sku
                    WHEN 'Rosto-5' THEN 19.55
                    WHEN 'Te' THEN 18.50
                    WHEN 'Pescoco' THEN 9.65
                    WHEN 'Rosto-1t' THEN 12.25
                    WHEN 'Rosto-2o' THEN 12.25
                END
                WHERE sku IN ('Rosto-5', 'Te', 'Pescoco', 'Rosto-1t', 'Rosto-2o') AND preco IS NULL
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.tiny_tokens (
                    account TEXT PRIMARY KEY,
                    access_token TEXT,
                    refresh_token TEXT,
                    expires_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            
            try:
                await conn.execute("ALTER TABLE public.jobs DROP CONSTRAINT IF EXISTS jobs_job_type_check")
                await conn.execute("""
                    ALTER TABLE public.jobs ADD CONSTRAINT jobs_job_type_check
                    CHECK (job_type = ANY (ARRAY['noop','create_order_c','sync_status','fetch_label','fetch_nf_link','sync_nf_link','fetch_order_a','add_tag_c','add_tag_a','sync_tracking_c_to_a','update_numero_compra','approve_order_a']))
                """)
            except Exception:
                pass

            await conn.execute("""
                INSERT INTO public.feature_flags (key, enabled, functional, label, description) VALUES
                    ('replicate_orders', false, true, 'Replicar Pedidos (Webhook)', 'Cria pedidos em C quando A é aprovado via webhook'),
                    ('sync_status_enviado', false, true, 'Sync Status: Enviado', 'Espelha status enviado de A para C'),
                    ('sync_status_entregue', false, true, 'Sync Status: Entregue', 'Espelha status entregue de A para C'),
                    ('sync_status_cancelado', false, true, 'Sync Status: Cancelado', 'Espelha status cancelado entre A e C'),
                    ('sync_status_faturado', false, true, 'Sync Status: Faturado', 'Espelha status faturado de C para A'),
                    ('sync_nf_link', false, true, 'Enviar NF', 'Envia dados da NF de C para observações de A'),
                    ('sync_tracking_pronto_envio', true, true, 'Sync Rastreio C→A (Pronto Envio)', 'Quando C entra em pronto_envio, copia código/URL de rastreio para A e avança status')
                ON CONFLICT (key) DO UPDATE SET functional = EXCLUDED.functional, label = EXCLUDED.label, description = EXCLUDED.description
            """)

            await conn.execute("""
                INSERT INTO public.feature_flags (key, enabled, functional, label, description)
                VALUES ('auto_approve_open_orders', true, true, 'Aprovar pedidos em aberto', 'Aprova pedidos A em aberto no mesmo horario apos 1 dia util')
                ON CONFLICT (key) DO UPDATE SET functional = EXCLUDED.functional, label = EXCLUDED.label, description = EXCLUDED.description
            """)

            await conn.execute("""
                INSERT INTO public.feature_flags (key, enabled, functional, label, description)
                VALUES ('sync_status_nao_entregue', true, true, 'Sync Status: Nao entregue', 'Espelha status nao entregue de C para A')
                ON CONFLICT (key) DO UPDATE SET functional = EXCLUDED.functional, label = EXCLUDED.label, description = EXCLUDED.description
            """)
            # Heartbeat do worker — usado pelo /health para detectar worker travado
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS public.worker_heartbeat (
                    id INT PRIMARY KEY DEFAULT 1,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                INSERT INTO public.worker_heartbeat (id, updated_at) VALUES (1, NOW())
                ON CONFLICT (id) DO NOTHING
            """)
            try:
                await conn.execute("ALTER TABLE public.worker_heartbeat ENABLE ROW LEVEL SECURITY")
            except Exception:
                pass
        logger.info("Database connected and tables created")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)


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
    codigo_situacao: str | None,
    id_nota_fiscal: str | None,
    payload: str
) -> str | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO public.events (event_key, source, topic, venda_id, codigo_situacao, id_nota_fiscal, payload)
            VALUES ($1::text, $2::text, $3::text, $4::integer, $5::text, $6::text, $7::jsonb)
            ON CONFLICT (event_key) DO NOTHING
            RETURNING id
        """, event_key, source, topic, venda_id, codigo_situacao, id_nota_fiscal, payload)
        return str(row["id"]) if row else None


async def insert_job(job_type: str, dedupe_key: str, event_id: str | None, payload: dict | None = None, delay_minutes: int = 0) -> bool:
    p = await get_pool()
    payload_str = json.dumps(payload) if payload else '{}'
    async with p.acquire() as conn:
        try:
            if delay_minutes > 0:
                result = await conn.execute("""
                    INSERT INTO public.jobs (job_type, dedupe_key, status, payload, run_after)
                    VALUES ($1::text, $2::text, 'queued', $3::jsonb, NOW() + ($4::int || ' minutes')::interval)
                    ON CONFLICT (dedupe_key) DO UPDATE
                    SET status = 'queued',
                        payload = COALESCE(NULLIF($3::jsonb, '{}'::jsonb), public.jobs.payload),
                        attempts = 0,
                        last_error = NULL,
                        run_after = NOW() + ($4::int || ' minutes')::interval,
                        locked_at = NULL,
                        locked_by = NULL,
                        updated_at = NOW()
                    WHERE public.jobs.status IN ('failed', 'dead')
                """, job_type, dedupe_key, payload_str, delay_minutes)
            else:
                result = await conn.execute("""
                    INSERT INTO public.jobs (job_type, dedupe_key, status, payload)
                    VALUES ($1::text, $2::text, 'queued', $3::jsonb)
                    ON CONFLICT (dedupe_key) DO UPDATE
                    SET status = 'queued',
                        payload = COALESCE(NULLIF($3::jsonb, '{}'::jsonb), public.jobs.payload),
                        attempts = 0,
                        last_error = NULL,
                        run_after = NOW(),
                        locked_at = NULL,
                        locked_by = NULL,
                        updated_at = NOW()
                    WHERE public.jobs.status IN ('failed', 'dead')
                """, job_type, dedupe_key, payload_str)
            return "INSERT" in result or "UPDATE" in result
        except Exception as e:
            logger.error(f"Failed to insert job: {e}")
            try:
                result = await conn.execute("""
                    INSERT INTO public.jobs (job_type, dedupe_key, status)
                    VALUES ($1::text, $2::text, 'queued')
                    ON CONFLICT (dedupe_key) DO NOTHING
                """, job_type, dedupe_key)
                return "INSERT" in result
            except Exception as e2:
                logger.error(f"Failed to insert job fallback: {e2}")
                return False


async def reset_stale_locks() -> int:
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            result = await conn.execute("""
                UPDATE public.jobs
                SET status = 'queued', 
                    attempts = COALESCE(attempts, 0) + 1,
                    last_error = 'stale_lock_reset',
                    locked_at = NULL,
                    locked_by = NULL,
                    run_after = NOW()
                WHERE status = 'running' AND locked_at < NOW() - INTERVAL '2 minutes'
            """)
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                logger.info(f"Reset {count} stale locked jobs")
            return count
        except Exception as e:
            logger.error(f"Failed to reset stale locks: {e}")
            return 0


async def fetch_and_lock_jobs(limit: int = 25) -> list[dict]:
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            UPDATE public.jobs
            SET status = 'running', locked_at = NOW(), locked_by = 'worker'
            WHERE id IN (
                SELECT id FROM public.jobs
                WHERE status = 'queued' AND (run_after IS NULL OR run_after <= NOW())
                ORDER BY created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, job_type, dedupe_key, payload, attempts
        """, limit)
        return [dict(row) for row in rows]


async def update_job_done(job_id, action_preview: dict) -> None:
    p = await get_pool()
    action_preview_str = json.dumps(action_preview)
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                UPDATE public.jobs
                SET status = 'done', action_preview = $2::jsonb, locked_at = NULL, locked_by = NULL, updated_at = NOW()
                WHERE id = $1
            """, job_id, action_preview_str)
        except Exception:
            try:
                await conn.execute("""
                    UPDATE public.jobs
                    SET status = 'done', locked_at = NULL, locked_by = NULL, updated_at = NOW()
                    WHERE id = $1
                """, job_id)
            except Exception as e:
                logger.error(f"Failed to update job done: {e}")


async def update_job_skipped_not_mapped(job_id, action_preview: dict) -> None:
    p = await get_pool()
    action_preview_str = json.dumps(action_preview)
    async with p.acquire() as conn:
        try:
            result = await conn.execute("""
                UPDATE public.jobs
                SET status = 'skipped_not_mapped', action_preview = $2::jsonb, locked_at = NULL, locked_by = NULL, updated_at = NOW()
                WHERE id = $1
            """, job_id, action_preview_str)
            if result != "UPDATE 1":
                logger.warning(f"update_job_skipped_not_mapped({job_id}): unexpected result '{result}'")
        except Exception as e1:
            logger.warning(f"update_job_skipped_not_mapped primary failed for {job_id}: {e1}")
            try:
                await conn.execute("""
                    UPDATE public.jobs
                    SET status = 'skipped_not_mapped', locked_at = NULL, locked_by = NULL, updated_at = NOW()
                    WHERE id = $1
                """, job_id)
            except Exception as e2:
                logger.error(f"Failed to update job skipped_not_mapped: {e2}")


async def update_job_waiting_sku(job_id, action_preview: dict) -> None:
    p = await get_pool()
    action_preview_str = json.dumps(action_preview)
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                UPDATE public.jobs
                SET status = 'waiting_sku', action_preview = $2::jsonb, locked_at = NULL, locked_by = NULL, updated_at = NOW()
                WHERE id = $1
            """, job_id, action_preview_str)
        except Exception as e1:
            logger.warning(f"update_job_waiting_sku primary failed for {job_id}: {e1}")
            try:
                await conn.execute("""
                    UPDATE public.jobs
                    SET status = 'waiting_sku', locked_at = NULL, locked_by = NULL, updated_at = NOW()
                    WHERE id = $1
                """, job_id)
            except Exception as e2:
                logger.error(f"Failed to update job waiting_sku: {e2}")


async def count_tracking_skipped_recent(days: int | None = None) -> int:
    """Quantos sync_tracking_c_to_a foram skipped por falta de código.
    Se days=None, conta todos. Senão, só dos últimos N dias."""
    p = await get_pool()
    async with p.acquire() as conn:
        if days is None:
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM public.jobs
                WHERE job_type = 'sync_tracking_c_to_a'
                  AND status = 'done'
                  AND action_preview::jsonb->>'reason' = 'no_tracking_code_after_retries'
            """)
        else:
            count = await conn.fetchval(f"""
                SELECT COUNT(*) FROM public.jobs
                WHERE job_type = 'sync_tracking_c_to_a'
                  AND status = 'done'
                  AND action_preview::jsonb->>'reason' = 'no_tracking_code_after_retries'
                  AND updated_at >= NOW() - INTERVAL '{int(days)} days'
            """)
        return count or 0


async def retry_tracking_skipped(days: int | None = None) -> int:
    """
    Recoloca na fila jobs sync_tracking_c_to_a marcados como skipped (sem código).
    Se days=None, pega TODOS. Senão, só dos últimos N dias.
    Escalona run_after (10 jobs/min) para não sobrecarregar a API.
    """
    p = await get_pool()
    async with p.acquire() as conn:
        if days is None:
            where_clause = """
                job_type = 'sync_tracking_c_to_a'
                  AND status = 'done'
                  AND action_preview::jsonb->>'reason' = 'no_tracking_code_after_retries'
            """
        else:
            where_clause = f"""
                job_type = 'sync_tracking_c_to_a'
                  AND status = 'done'
                  AND action_preview::jsonb->>'reason' = 'no_tracking_code_after_retries'
                  AND updated_at >= NOW() - INTERVAL '{int(days)} days'
            """
        # Atualiza com run_after escalonado (6 segundos entre cada = 10/min)
        # ORDER BY updated_at DESC garante que os mais recentes processam primeiro
        result = await conn.execute(f"""
            WITH ordered AS (
                SELECT id, ROW_NUMBER() OVER (ORDER BY updated_at DESC) - 1 as seq
                FROM public.jobs
                WHERE {where_clause}
            )
            UPDATE public.jobs j
            SET status = 'queued',
                attempts = 0,
                action_preview = NULL,
                last_error = NULL,
                locked_at = NULL,
                locked_by = NULL,
                run_after = NOW() + (ordered.seq * 6 || ' seconds')::interval,
                updated_at = NOW()
            FROM ordered
            WHERE j.id = ordered.id
        """)
        count = int(result.split()[-1]) if result else 0
        logger.info(f"retry_tracking_skipped: requeued {count} jobs (days={days})")
        return count


async def retry_waiting_sku_jobs() -> int:
    p = await get_pool()
    async with p.acquire() as conn:
        result = await conn.execute("""
            UPDATE public.jobs
            SET status = 'queued', locked_at = NULL, locked_by = NULL, run_after = NULL, updated_at = NOW()
            WHERE status = 'waiting_sku'
        """)
        count = int(result.split()[-1]) if result else 0
        logger.info(f"retry_waiting_sku_jobs: requeued {count} jobs")
        return count


async def update_job_failed(job_id, error: str, attempts: int) -> None:
    p = await get_pool()
    new_status = 'dead' if attempts >= MAX_ATTEMPTS else 'failed'
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                UPDATE public.jobs
                SET status = $2, last_error = $3, attempts = $4, last_attempt_at = NOW(), locked_at = NULL, locked_by = NULL
                WHERE id = $1
            """, job_id, new_status, error, attempts)
        except Exception:
            try:
                await conn.execute("""
                    UPDATE public.jobs
                    SET status = $2, locked_at = NULL, locked_by = NULL
                    WHERE id = $1
                """, job_id, new_status)
            except Exception as e:
                logger.error(f"Failed to update job failed status: {e}")


async def reschedule_job_with_backoff(job_id, attempts: int, delay_minutes: int, last_error: str) -> None:
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                UPDATE public.jobs
                SET status = 'queued',
                    attempts = $2,
                    last_error = $3,
                    last_attempt_at = NOW(),
                    run_after = NOW() + ($4::int || ' minutes')::interval,
                    locked_at = NULL,
                    locked_by = NULL
                WHERE id = $1
            """, job_id, attempts, last_error, delay_minutes)
        except Exception as e:
            logger.error(f"Failed to reschedule job {job_id}: {e}")


async def upsert_orders_map(external_key: str, venda_a_id: str | None) -> None:
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO public.orders_map (external_key, venda_a_id, updated_at)
                VALUES ($1::text, $2::text, NOW())
                ON CONFLICT (external_key) DO UPDATE SET 
                    venda_a_id = EXCLUDED.venda_a_id,
                    updated_at = NOW()
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


async def get_jobs_list(status: str | None, limit: int, job_type: str | None = None) -> list[dict]:
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            if status and job_type:
                rows = await conn.fetch("""
                    SELECT id, job_type, dedupe_key, status, created_at, payload, action_preview
                    FROM public.jobs
                    WHERE status = $1 AND job_type = $2
                    ORDER BY created_at DESC
                    LIMIT $3
                """, status, job_type, limit)
            elif status:
                rows = await conn.fetch("""
                    SELECT id, job_type, dedupe_key, status, created_at, payload, action_preview
                    FROM public.jobs
                    WHERE status = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """, status, limit)
            elif job_type:
                rows = await conn.fetch("""
                    SELECT id, job_type, dedupe_key, status, created_at, payload, action_preview
                    FROM public.jobs
                    WHERE job_type = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """, job_type, limit)
            else:
                rows = await conn.fetch("""
                    SELECT id, job_type, dedupe_key, status, created_at, payload, action_preview
                    FROM public.jobs
                    ORDER BY created_at DESC
                    LIMIT $1
                """, limit)
            return [dict(row) for row in rows]
        except Exception:
            if status:
                rows = await conn.fetch("""
                    SELECT id, job_type, dedupe_key, status, created_at
                    FROM public.jobs
                    WHERE status = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """, status, limit)
            else:
                rows = await conn.fetch("""
                    SELECT id, job_type, dedupe_key, status, created_at
                    FROM public.jobs
                    ORDER BY created_at DESC
                    LIMIT $1
                """, limit)
            return [dict(row) for row in rows]


async def upsert_orders_a_snapshot(venda_a_id: str, webhook_payload: dict) -> None:
    p = await get_pool()
    payload_str = json.dumps(webhook_payload)
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.orders_a_snapshot (venda_a_id, webhook_payload, updated_at)
            VALUES ($1::text, $2::jsonb, NOW())
            ON CONFLICT (venda_a_id) DO UPDATE SET 
                webhook_payload = EXCLUDED.webhook_payload,
                updated_at = NOW()
        """, venda_a_id, payload_str)


async def get_orders_a_list(limit: int) -> list[dict]:
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT venda_a_id, needs_fetch, updated_at, created_at
            FROM public.orders_a_snapshot
            ORDER BY updated_at DESC
            LIMIT $1
        """, limit)
        return [dict(row) for row in rows]


async def get_order_a_snapshot(venda_a_id: str) -> dict | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT venda_a_id, created_at, updated_at, webhook_payload, fetched_payload, needs_fetch, notes
            FROM public.orders_a_snapshot
            WHERE venda_a_id = $1
        """, venda_a_id)
        return dict(row) if row else None


async def get_snapshot_fetched_at(venda_a_id: str) -> datetime | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT fetched_at FROM public.orders_a_snapshot WHERE venda_a_id = $1
        """, venda_a_id)
        return row['fetched_at'] if row and row['fetched_at'] else None


async def upsert_orders_a_fetched(venda_a_id: str, fetched_payload: dict) -> None:
    p = await get_pool()
    payload_str = json.dumps(fetched_payload)
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.orders_a_snapshot (venda_a_id, fetched_payload, fetched_at, needs_fetch, last_error, updated_at)
            VALUES ($1::text, $2::jsonb, NOW(), false, NULL, NOW())
            ON CONFLICT (venda_a_id) DO UPDATE SET 
                fetched_payload = EXCLUDED.fetched_payload,
                fetched_at = NOW(),
                needs_fetch = false,
                last_error = NULL,
                updated_at = NOW()
        """, venda_a_id, payload_str)


async def upsert_orders_a_fetch_error(venda_a_id: str, status_code: int, error_body: str) -> None:
    p = await get_pool()
    last_error = json.dumps({"status_code": status_code, "body": error_body[:500], "at": datetime.utcnow().isoformat()})
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.orders_a_snapshot (venda_a_id, needs_fetch, last_error, updated_at)
            VALUES ($1::text, true, $2::jsonb, NOW())
            ON CONFLICT (venda_a_id) DO UPDATE SET 
                needs_fetch = true,
                last_error = $2::jsonb,
                updated_at = NOW()
        """, venda_a_id, last_error)


async def upsert_orders_c_fetched(venda_c_id: str, fetched_payload: dict) -> None:
    p = await get_pool()
    payload_str = json.dumps(fetched_payload)
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.orders_c_snapshot (venda_c_id, fetched_payload, fetched_at, last_error, updated_at)
            VALUES ($1::text, $2::jsonb, NOW(), NULL, NOW())
            ON CONFLICT (venda_c_id) DO UPDATE SET
                fetched_payload = EXCLUDED.fetched_payload,
                fetched_at = NOW(),
                last_error = NULL,
                updated_at = NOW()
        """, venda_c_id, payload_str)


async def upsert_orders_c_fetch_error(venda_c_id: str, status_code: int | None, error_body: str) -> None:
    p = await get_pool()
    last_error = json.dumps({"status_code": status_code, "body": str(error_body)[:500], "at": datetime.utcnow().isoformat()})
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.orders_c_snapshot (venda_c_id, last_error, updated_at)
            VALUES ($1::text, $2::jsonb, NOW())
            ON CONFLICT (venda_c_id) DO UPDATE SET
                last_error = $2::jsonb,
                updated_at = NOW()
        """, venda_c_id, last_error)


async def get_order_c_snapshot(venda_c_id: str) -> dict | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT venda_c_id, fetched_payload, fetched_at, last_error, created_at, updated_at
            FROM public.orders_c_snapshot
            WHERE venda_c_id = $1
        """, venda_c_id)
        return dict(row) if row else None


async def upsert_cancelled_order_review(venda_a_id: str) -> None:
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.cancelled_order_reviews (venda_a_id, status, updated_at)
            VALUES ($1::text, 'pending', NOW())
            ON CONFLICT (venda_a_id) DO UPDATE SET
                status = CASE
                    WHEN public.cancelled_order_reviews.status = 'reviewed' THEN public.cancelled_order_reviews.status
                    ELSE 'pending'
                END,
                updated_at = NOW()
        """, venda_a_id)


async def mark_cancelled_order_reviews(ids: list[str]) -> int:
    if not ids:
        return 0
    p = await get_pool()
    async with p.acquire() as conn:
        result = await conn.execute("""
            UPDATE public.cancelled_order_reviews
            SET status = 'reviewed', reviewed_at = NOW(), updated_at = NOW()
            WHERE venda_a_id = ANY($1::text[])
              AND status <> 'reviewed'
        """, ids)
        return int(result.split()[-1]) if result else 0


async def upsert_orders_map_with_c(external_key: str, venda_a_id: str, venda_c_id: str | None) -> None:
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.orders_map (external_key, venda_a_id, venda_c_id, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (external_key) DO UPDATE SET 
                venda_c_id = EXCLUDED.venda_c_id,
                updated_at = NOW()
        """, external_key, int(venda_a_id), int(venda_c_id) if venda_c_id else None)


async def get_orders_map_list(limit: int) -> list[dict]:
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT external_key, venda_a_id, venda_c_id, created_at, updated_at
            FROM public.orders_map
            ORDER BY updated_at DESC
            LIMIT $1
        """, limit)
        return [dict(row) for row in rows]


async def get_order_mapping_by_a(venda_a_id: str) -> dict | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT external_key, venda_a_id, venda_c_id
            FROM public.orders_map
            WHERE venda_a_id = $1 AND venda_c_id IS NOT NULL
        """, int(venda_a_id))
        return dict(row) if row else None


async def get_nf_event_payload(id_nota_fiscal: str) -> dict | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT payload
            FROM public.events
            WHERE source = 'B' AND topic = 'notas_fiscais'
              AND id_nota_fiscal = $1
            ORDER BY created_at DESC
            LIMIT 1
        """, id_nota_fiscal)
        if row and row["payload"]:
            import json
            try:
                return json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
            except (json.JSONDecodeError, TypeError):
                return None
        return None


async def get_venda_c_by_nota_fiscal(id_nota_fiscal: str) -> str | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT venda_id
            FROM public.events
            WHERE source = 'B' AND topic = 'vendas'
              AND id_nota_fiscal = $1
              AND venda_id IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
        """, id_nota_fiscal)
        return str(row["venda_id"]) if row else None


async def get_order_mapping_by_c(venda_c_id: str) -> dict | None:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT external_key, venda_a_id, venda_c_id
            FROM public.orders_map
            WHERE venda_c_id::text = $1
        """, venda_c_id)
        return dict(row) if row else None


async def update_orders_map_sync(venda_a_id: str, venda_c_id: str, sync_status: str) -> None:
    """Atualiza last_sync_status e last_sync_at no orders_map após sync_status executar."""
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                UPDATE public.orders_map
                SET last_sync_status = $3, last_sync_at = NOW(), updated_at = NOW()
                WHERE venda_a_id::text = $1 OR venda_c_id::text = $2
            """, venda_a_id, venda_c_id, sync_status)
        except Exception as e:
            logger.error(f"Failed to update orders_map sync: {e}")


async def check_is_echo(source: str, venda_id: str, codigo_situacao: str) -> bool:
    """Verifica se um webhook é eco de um sync_status recente (últimos 5 minutos)."""
    p = await get_pool()
    async with p.acquire() as conn:
        if source == "A":
            row = await conn.fetchrow("""
                SELECT last_sync_status, last_sync_at FROM public.orders_map
                WHERE venda_a_id::text = $1
                  AND last_sync_status = $2
                  AND last_sync_at > NOW() - INTERVAL '5 minutes'
            """, venda_id, codigo_situacao)
        elif source == "B":
            row = await conn.fetchrow("""
                SELECT last_sync_status, last_sync_at FROM public.orders_map
                WHERE venda_c_id::text = $1
                  AND last_sync_status = $2
                  AND last_sync_at > NOW() - INTERVAL '5 minutes'
            """, venda_id, codigo_situacao)
        else:
            return False
        return row is not None


async def update_event_action_result(event_id: str, action_result: str) -> None:
    """Atualiza o action_result de um evento."""
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            uid = int(event_id) if isinstance(event_id, str) else event_id
            await conn.execute("""
                UPDATE public.events SET action_result = $2 WHERE id = $1
            """, uid, action_result)
        except Exception as e:
            logger.error(f"Failed to update event action_result: {e}")


async def count_orders_replicated_to_c() -> int:
    """Conta quantos pedidos foram replicados para C (orders_map com venda_c_id preenchido)."""
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT COUNT(*) as cnt FROM public.orders_map WHERE venda_c_id IS NOT NULL
        """)
        return row['cnt'] if row else 0


async def upsert_partial_product(id_a: int, sku: str) -> None:
    """Insere linha parcial em products_map (id_c=NULL, ativo=false) se não existir."""
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO public.products_map (id_a, sku, id_c, ativo, updated_at)
                VALUES ($1, $2, NULL, false, NOW())
                ON CONFLICT (id_a) DO NOTHING
            """, id_a, sku)
        except Exception as e:
            logger.warning(f"upsert_partial_product({id_a}, {sku}): {e}")


async def load_products_map() -> dict[int, int]:
    """Carrega mapeamento de produtos A -> C da tabela products_map."""
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id_a, id_c FROM public.products_map WHERE id_c IS NOT NULL
        """)
        return {row['id_a']: row['id_c'] for row in rows}


async def load_products_prices() -> dict[str, float]:
    """Carrega preços por SKU da tabela products_map."""
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT sku, preco FROM public.products_map WHERE preco IS NOT NULL AND sku IS NOT NULL
        """)
        return {row['sku']: float(row['preco']) for row in rows}


async def get_products_map_list() -> list[dict]:
    """Lista todos os produtos da tabela products_map."""
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id_a, id_c, sku, descricao, situacao, ativo, preco, updated_at
            FROM public.products_map
            ORDER BY id_a
        """)
        return [dict(row) for row in rows]


async def upsert_product_map(id_a: int, sku: str, descricao: str, situacao: str, ativo: bool):
    """Insere ou atualiza um produto na tabela products_map."""
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.products_map (id_a, sku, descricao, situacao, ativo)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id_a) DO UPDATE SET
                sku = EXCLUDED.sku,
                descricao = EXCLUDED.descricao,
                situacao = EXCLUDED.situacao,
                ativo = EXCLUDED.ativo,
                updated_at = NOW()
        """, id_a, sku, descricao, situacao, ativo)


async def retry_failed_jobs(job_type: str | None = None) -> int:
    """Recoloca jobs falhos na fila para reprocessamento."""
    p = await get_pool()
    async with p.acquire() as conn:
        if job_type:
            result = await conn.execute("""
                UPDATE public.jobs
                SET status = 'queued',
                    attempts = 0,
                    last_error = NULL,
                    locked_at = NULL,
                    locked_by = NULL,
                    run_after = NOW()
                WHERE status = 'failed' AND job_type = $1
            """, job_type)
        else:
            result = await conn.execute("""
                UPDATE public.jobs
                SET status = 'queued',
                    attempts = 0,
                    last_error = NULL,
                    locked_at = NULL,
                    locked_by = NULL,
                    run_after = NOW()
                WHERE status = 'failed'
            """)
        count = int(result.split()[-1]) if result else 0
        logger.info(f"Retried {count} failed jobs")
        return count


async def get_failed_jobs_count() -> dict:
    """Conta jobs falhos por tipo."""
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT job_type, COUNT(*) as count
            FROM public.jobs
            WHERE status = 'failed'
            GROUP BY job_type
        """)
        return {row['job_type']: row['count'] for row in rows}


async def get_all_feature_flags() -> list[dict]:
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT key, enabled, functional, label, description, updated_at
            FROM public.feature_flags
            ORDER BY key
        """)
        return [dict(row) for row in rows]


async def get_feature_flag(key: str) -> bool:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT enabled, functional FROM public.feature_flags WHERE key = $1
        """, key)
        if not row:
            return False
        return row['enabled'] and row['functional']


async def set_feature_flag(key: str, enabled: bool) -> bool:
    p = await get_pool()
    async with p.acquire() as conn:
        result = await conn.execute("""
            UPDATE public.feature_flags
            SET enabled = $2, updated_at = NOW()
            WHERE key = $1 AND functional = true
        """, key, enabled)
        return "UPDATE 1" in result


async def get_dashboard_data() -> dict:
    p = await get_pool()
    async with p.acquire() as conn:
        events_total = await conn.fetchval("SELECT COUNT(*) FROM public.events")

        last_event = await conn.fetchrow("""
            SELECT source, topic, venda_id, codigo_situacao, created_at
            FROM public.events ORDER BY created_at DESC LIMIT 1
        """)

        job_counts = {}
        for status in ['queued', 'running', 'done', 'failed', 'dead', 'waiting_sku']:
            job_counts[status] = await conn.fetchval(
                "SELECT COUNT(*) FROM public.jobs WHERE status = $1", status
            )

        today_done = await conn.fetchval("""
            SELECT COUNT(*) FROM public.jobs
            WHERE status = 'done' AND updated_at >= CURRENT_DATE
        """)
        today_failed = await conn.fetchval("""
            SELECT COUNT(*) FROM public.jobs
            WHERE status IN ('failed', 'dead') AND updated_at >= CURRENT_DATE
        """)

        recent_jobs = await conn.fetch("""
            SELECT id, job_type, status, dedupe_key, created_at, updated_at,
                   action_preview, last_error, attempts
            FROM public.jobs
            ORDER BY COALESCE(updated_at, created_at) DESC
            LIMIT 15
        """)

        replicated = await conn.fetchval("""
            SELECT COUNT(*) FROM public.orders_map WHERE venda_c_id IS NOT NULL
        """)

        # Sync de rastreio sem código (candidatos a reprocessar) — total all-time
        tracking_skipped_recent = await conn.fetchval("""
            SELECT COUNT(*) FROM public.jobs
            WHERE job_type = 'sync_tracking_c_to_a'
              AND status = 'done'
              AND action_preview::jsonb->>'reason' = 'no_tracking_code_after_retries'
        """)

        # Idade do heartbeat do worker (segundos desde último pulso)
        worker_heartbeat_age = await conn.fetchval(
            "SELECT EXTRACT(EPOCH FROM (NOW() - updated_at)) FROM public.worker_heartbeat WHERE id = 1"
        )

        return {
            "events_total": events_total,
            "last_event": dict(last_event) if last_event else None,
            "job_counts": job_counts,
            "today_done": today_done,
            "today_failed": today_failed,
            "recent_jobs": [dict(r) for r in recent_jobs],
            "orders_replicated": replicated,
            "tracking_skipped_recent": tracking_skipped_recent or 0,
            "worker_heartbeat_age_seconds": float(worker_heartbeat_age) if worker_heartbeat_age is not None else None,
        }


async def create_tracking_fix_jobs() -> dict:
    """
    Cria novos jobs para reprocessar sync_tracking_c_to_a que escreveram código vazio.
    Usa INSERT em batch (único SQL) para evitar timeout.
    Escalonados: 10 jobs por minuto (1 a cada 6 segundos) para respeitar rate limits.
    """
    p = await get_pool()
    async with p.acquire() as conn:
        # Faz tudo em um único SQL: seleciona afetados e insere jobs escalonados
        result = await conn.execute("""
            WITH affected AS (
                SELECT DISTINCT ON (payload::jsonb->>'venda_id')
                    id as original_job_id,
                    payload::jsonb->>'venda_id' as venda_c_id,
                    ROW_NUMBER() OVER (ORDER BY payload::jsonb->>'venda_id') - 1 as seq
                FROM public.jobs
                WHERE job_type = 'sync_tracking_c_to_a'
                  AND status = 'done'
                  AND (action_preview::jsonb->>'skipped' IS NULL OR action_preview::jsonb->>'skipped' = 'false')
                  AND (action_preview::jsonb->>'codigo_rastreamento' IS NULL OR action_preview::jsonb->>'codigo_rastreamento' = '')
                  AND payload::jsonb->>'venda_id' IS NOT NULL
                ORDER BY payload::jsonb->>'venda_id', created_at DESC
            )
            INSERT INTO public.jobs (job_type, dedupe_key, status, payload, run_after)
            SELECT
                'sync_tracking_c_to_a',
                'tracking_fix:' || venda_c_id,
                'queued',
                jsonb_build_object(
                    'source', 'B',
                    'topic', 'vendas',
                    'venda_id', venda_c_id,
                    'codigo_situacao', '7',
                    'reprocess', 'tracking_fix_20260508',
                    'original_job_id', original_job_id
                ),
                NOW() + (seq * 6 || ' seconds')::interval
            FROM affected
            ON CONFLICT (dedupe_key) DO NOTHING
        """)
        created = int(result.split()[-1]) if result else 0

        # Conta total de afetados para o relatório
        total = await conn.fetchval("""
            SELECT COUNT(DISTINCT payload::jsonb->>'venda_id')
            FROM public.jobs
            WHERE job_type = 'sync_tracking_c_to_a'
              AND status = 'done'
              AND (action_preview::jsonb->>'skipped' IS NULL OR action_preview::jsonb->>'skipped' = 'false')
              AND (action_preview::jsonb->>'codigo_rastreamento' IS NULL OR action_preview::jsonb->>'codigo_rastreamento' = '')
        """)

        estimated_minutes = (created * 6) // 60
        logger.info(f"Tracking fix: created={created}, total_affected={total}, estimated={estimated_minutes}min")
        return {
            "created": created,
            "skipped_duplicates": total - created if total else 0,
            "total_affected": total,
            "estimated_duration_minutes": estimated_minutes,
            "rate": "10 jobs/min",
        }


async def get_tracking_fix_report() -> dict:
    """
    Relatório dos jobs do batch tracking_fix para verificação.
    Retorna contagens + lista de pedidos com resultado.
    """
    p = await get_pool()
    async with p.acquire() as conn:
        # Contagem por status
        summary = await conn.fetch("""
            SELECT
                status,
                COUNT(*) as count
            FROM public.jobs
            WHERE dedupe_key LIKE 'tracking_fix:%'
            GROUP BY status
            ORDER BY count DESC
        """)

        # Detalhes dos concluídos (com e sem código)
        done_jobs = await conn.fetch("""
            SELECT
                id,
                payload::jsonb->>'venda_id' as venda_c_id,
                action_preview::jsonb->>'venda_a_id' as venda_a_id,
                action_preview::jsonb->>'codigo_rastreamento' as codigo_rastreamento,
                action_preview::jsonb->>'url_rastreamento' as url_rastreamento,
                action_preview::jsonb->>'skipped' as skipped,
                action_preview::jsonb->>'reason' as reason,
                action_preview::jsonb->>'status_updated' as status_updated,
                updated_at
            FROM public.jobs
            WHERE dedupe_key LIKE 'tracking_fix:%'
              AND status = 'done'
            ORDER BY updated_at DESC
        """)

        # Separar sucesso vs falha
        success = []
        still_empty = []
        skipped_list = []
        for r in done_jobs:
            item = {
                "job_id": r['id'],
                "venda_c_id": r['venda_c_id'],
                "venda_a_id": r['venda_a_id'],
                "codigo_rastreamento": r['codigo_rastreamento'] or "",
                "updated_at": r['updated_at'].isoformat() if r['updated_at'] else None,
            }
            if r['skipped'] == 'true':
                item["reason"] = r['reason']
                skipped_list.append(item)
            elif r['codigo_rastreamento']:
                success.append(item)
            else:
                still_empty.append(item)

        # Jobs pendentes
        pending = await conn.fetchval("""
            SELECT COUNT(*) FROM public.jobs
            WHERE dedupe_key LIKE 'tracking_fix:%'
              AND status IN ('queued', 'running')
        """)

        failed = await conn.fetchval("""
            SELECT COUNT(*) FROM public.jobs
            WHERE dedupe_key LIKE 'tracking_fix:%'
              AND status = 'failed'
        """)

        return {
            "summary": {s['status']: s['count'] for s in summary},
            "pending": pending,
            "failed": failed,
            "success_count": len(success),
            "still_empty_count": len(still_empty),
            "skipped_count": len(skipped_list),
            "success_sample": success[:20],
            "still_empty_sample": still_empty[:20],
            "skipped_sample": skipped_list[:20],
        }


async def count_numero_compra_candidates() -> dict:
    """Conta quantos pedidos podem ter o numeroOrdemCompra atualizado."""
    p = await get_pool()
    async with p.acquire() as conn:
        total_eligible = await conn.fetchval("""
            SELECT COUNT(*)
            FROM public.orders_map om
            JOIN public.orders_a_snapshot oas ON oas.venda_a_id = om.venda_a_id::text
            WHERE om.venda_c_id IS NOT NULL
              AND oas.fetched_payload::jsonb->'ecommerce'->>'numeroPedidoEcommerce' IS NOT NULL
              AND oas.fetched_payload::jsonb->'ecommerce'->>'numeroPedidoEcommerce' != ''
              AND NOT EXISTS (
                  SELECT 1 FROM public.jobs j
                  WHERE j.dedupe_key = 'update_numero_compra:' || om.venda_c_id::text
              )
        """)
        already_queued_or_done = await conn.fetchval("""
            SELECT COUNT(*) FROM public.jobs
            WHERE job_type = 'update_numero_compra'
        """)
        return {
            "eligible_remaining": total_eligible,
            "jobs_already_created": already_queued_or_done,
        }


async def create_numero_compra_fix_jobs(limit: int) -> dict:
    """
    Cria jobs para atualizar numeroOrdemCompra em C com o numeroPedidoEcommerce de A.
    Escalonados: 10 jobs por minuto (1 a cada 6 segundos).
    Não recria jobs já existentes (idempotente).
    """
    p = await get_pool()
    async with p.acquire() as conn:
        result = await conn.execute("""
            WITH candidates AS (
                SELECT
                    om.venda_a_id,
                    om.venda_c_id,
                    oas.fetched_payload::jsonb->'ecommerce'->>'numeroPedidoEcommerce' as nro_ecom,
                    ROW_NUMBER() OVER (ORDER BY om.venda_c_id DESC) - 1 as seq
                FROM public.orders_map om
                JOIN public.orders_a_snapshot oas ON oas.venda_a_id = om.venda_a_id::text
                WHERE om.venda_c_id IS NOT NULL
                  AND oas.fetched_payload::jsonb->'ecommerce'->>'numeroPedidoEcommerce' IS NOT NULL
                  AND oas.fetched_payload::jsonb->'ecommerce'->>'numeroPedidoEcommerce' != ''
                  AND NOT EXISTS (
                      SELECT 1 FROM public.jobs j
                      WHERE j.dedupe_key = 'update_numero_compra:' || om.venda_c_id::text
                  )
                ORDER BY om.venda_c_id DESC
                LIMIT $1
            )
            INSERT INTO public.jobs (job_type, dedupe_key, status, payload, run_after)
            SELECT
                'update_numero_compra',
                'update_numero_compra:' || venda_c_id::text,
                'queued',
                jsonb_build_object(
                    'venda_a_id', venda_a_id::text,
                    'venda_c_id', venda_c_id::text,
                    'numero_pedido_ecommerce', nro_ecom,
                    'batch', 'numero_compra_fix_20260518'
                ),
                NOW() + (seq * 6 || ' seconds')::interval
            FROM candidates
            ON CONFLICT (dedupe_key) DO NOTHING
        """, limit)
        created = int(result.split()[-1]) if result else 0

        # Remaining após criação
        remaining = await conn.fetchval("""
            SELECT COUNT(*)
            FROM public.orders_map om
            JOIN public.orders_a_snapshot oas ON oas.venda_a_id = om.venda_a_id::text
            WHERE om.venda_c_id IS NOT NULL
              AND oas.fetched_payload::jsonb->'ecommerce'->>'numeroPedidoEcommerce' IS NOT NULL
              AND oas.fetched_payload::jsonb->'ecommerce'->>'numeroPedidoEcommerce' != ''
              AND NOT EXISTS (
                  SELECT 1 FROM public.jobs j
                  WHERE j.dedupe_key = 'update_numero_compra:' || om.venda_c_id::text
              )
        """)

        estimated_minutes = (created * 6) // 60
        logger.info(f"Numero compra fix: created={created}, remaining_after={remaining}, estimated={estimated_minutes}min")
        return {
            "created": created,
            "remaining_to_create": remaining,
            "estimated_duration_minutes": estimated_minutes,
            "rate": "10 jobs/min",
        }


async def get_numero_compra_fix_report() -> dict:
    """Relatório do progresso do batch de fix de numeroOrdemCompra."""
    p = await get_pool()
    async with p.acquire() as conn:
        summary = await conn.fetch("""
            SELECT status, COUNT(*) as count
            FROM public.jobs
            WHERE job_type = 'update_numero_compra'
            GROUP BY status
            ORDER BY count DESC
        """)

        done_jobs = await conn.fetch("""
            SELECT
                id,
                payload::jsonb->>'venda_c_id' as venda_c_id,
                payload::jsonb->>'venda_a_id' as venda_a_id,
                payload::jsonb->>'numero_pedido_ecommerce' as nro_ecom,
                action_preview::jsonb->>'skipped' as skipped,
                action_preview::jsonb->>'reason' as reason,
                updated_at
            FROM public.jobs
            WHERE job_type = 'update_numero_compra' AND status = 'done'
            ORDER BY updated_at DESC
        """)

        success = []
        skipped_list = []
        for r in done_jobs:
            item = {
                "job_id": r['id'],
                "venda_c_id": r['venda_c_id'],
                "venda_a_id": r['venda_a_id'],
                "numero_pedido_ecommerce": r['nro_ecom'],
                "updated_at": r['updated_at'].isoformat() if r['updated_at'] else None,
            }
            if r['skipped'] == 'true':
                item["reason"] = r['reason']
                skipped_list.append(item)
            else:
                success.append(item)

        pending = await conn.fetchval("""
            SELECT COUNT(*) FROM public.jobs
            WHERE job_type = 'update_numero_compra' AND status IN ('queued', 'running')
        """)
        failed = await conn.fetchval("""
            SELECT COUNT(*) FROM public.jobs
            WHERE job_type = 'update_numero_compra' AND status = 'failed'
        """)

        return {
            "summary": {s['status']: s['count'] for s in summary},
            "pending": pending,
            "failed": failed,
            "success_count": len(success),
            "skipped_count": len(skipped_list),
            "success_sample": success[:20],
            "skipped_sample": skipped_list[:20],
        }


async def update_worker_heartbeat() -> None:
    """Atualiza timestamp do heartbeat do worker. Usado pelo /health para detectar worker travado."""
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            await conn.execute("UPDATE public.worker_heartbeat SET updated_at = NOW() WHERE id = 1")
        except Exception as e:
            logger.error(f"Failed to update worker heartbeat: {e}")


async def get_worker_heartbeat_age_seconds() -> float | None:
    """Retorna quantos segundos se passaram desde o último heartbeat. None se nunca teve heartbeat."""
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            age = await conn.fetchval(
                "SELECT EXTRACT(EPOCH FROM (NOW() - updated_at)) FROM public.worker_heartbeat WHERE id = 1"
            )
            return float(age) if age is not None else None
        except Exception as e:
            logger.error(f"Failed to read worker heartbeat: {e}")
            return None
