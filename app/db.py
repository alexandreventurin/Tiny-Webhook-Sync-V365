import asyncpg
import ssl
import json
import logging
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
                    CHECK (job_type = ANY (ARRAY['noop','create_order_c','sync_status','fetch_label','fetch_nf_link','sync_nf_link','fetch_order_a','add_tag_c']))
                """)
            except Exception:
                pass

            await conn.execute("""
                INSERT INTO public.feature_flags (key, enabled, functional, label, description) VALUES
                    ('replicate_orders', false, true, 'Replicar Pedidos', 'Cria pedidos em C quando A é aprovado'),
                    ('sync_status_enviado', false, true, 'Sync Status: Enviado', 'Espelha status enviado de A para C'),
                    ('sync_status_entregue', false, true, 'Sync Status: Entregue', 'Espelha status entregue de A para C'),
                    ('sync_status_cancelado', false, true, 'Sync Status: Cancelado', 'Espelha status cancelado entre A e C'),
                    ('sync_status_faturado', false, true, 'Sync Status: Faturado', 'Espelha status faturado de C para A'),
                    ('sync_nf_link', false, true, 'Enviar NF', 'Envia dados da NF de C para observações de A')
                ON CONFLICT (key) DO UPDATE SET functional = EXCLUDED.functional, label = EXCLUDED.label, description = EXCLUDED.description
            """)
            
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
                    SET payload = COALESCE(NULLIF($3::jsonb, '{}'::jsonb), public.jobs.payload)
                """, job_type, dedupe_key, payload_str, delay_minutes)
            else:
                result = await conn.execute("""
                    INSERT INTO public.jobs (job_type, dedupe_key, status, payload)
                    VALUES ($1::text, $2::text, 'queued', $3::jsonb)
                    ON CONFLICT (dedupe_key) DO UPDATE 
                    SET payload = COALESCE(NULLIF($3::jsonb, '{}'::jsonb), public.jobs.payload)
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
                SET status = 'done', action_preview = $2::jsonb, locked_at = NULL, locked_by = NULL
                WHERE id = $1
            """, job_id, action_preview_str)
        except Exception:
            try:
                await conn.execute("""
                    UPDATE public.jobs
                    SET status = 'done', locked_at = NULL, locked_by = NULL
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


async def load_products_map() -> dict[int, int]:
    """Carrega mapeamento de produtos A -> C da tabela products_map."""
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id_a, id_c FROM public.products_map WHERE id_c IS NOT NULL
        """)
        return {row['id_a']: row['id_c'] for row in rows}


async def get_products_map_list() -> list[dict]:
    """Lista todos os produtos da tabela products_map."""
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id_a, id_c, sku, descricao, situacao, ativo, updated_at
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
        for status in ['queued', 'running', 'done', 'failed', 'dead']:
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

        return {
            "events_total": events_total,
            "last_event": dict(last_event) if last_event else None,
            "job_counts": job_counts,
            "today_done": today_done,
            "today_failed": today_failed,
            "recent_jobs": [dict(r) for r in recent_jobs],
            "orders_replicated": replicated,
        }
