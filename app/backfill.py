import asyncio
import logging
import math
from datetime import datetime, timedelta
from app.db import (
    create_import_run, update_import_run_progress, update_import_run_api_total,
    finish_import_run, insert_import_run_item, check_order_exists_in_map, insert_job
)
from app.tiny_client import TinyClient, TinyApiError
from app.tiny_oauth import ensure_access_token
from app.utils import normalize_status

logger = logging.getLogger(__name__)

ELIGIBLE_STATUSES = {"pronto_envio", "enviado", "entregue"}

_running_tasks: dict[int, asyncio.Task] = {}


def extract_status(order: dict) -> str | None:
    sit = order.get("situacao")
    if isinstance(sit, dict):
        raw = sit.get("valor") or sit.get("id")
    else:
        raw = sit
    return normalize_status(raw)


def compute_data_fim(data_inicio: str, dias: int) -> str:
    dt = datetime.strptime(data_inicio, "%Y-%m-%d")
    dt_fim = dt + timedelta(days=dias)
    return dt_fim.strftime("%Y-%m-%d")


async def run_import(run_id: int, data_inicio: str, data_fim: str, direction: str, limit_orders: int | None):
    logger.info(f"Import run {run_id} started: {data_inicio} -> {data_fim}, dir={direction}, limit_orders={limit_orders}")

    pages_fetched = 0
    orders_found = 0
    jobs_created = 0
    orders_skipped = 0
    orders_ignored = 0
    seen_ids: set[str] = set()
    total_pages = None

    try:
        token_a = await ensure_access_token("A")
        if not token_a:
            await finish_import_run(run_id, "error", "No valid OAuth token for account A")
            logger.error(f"Import run {run_id}: no token for A")
            return

        client_a = TinyClient(token_a)
        pagina = 1

        while True:
            if limit_orders and jobs_created >= limit_orders:
                logger.info(f"Import run {run_id}: reached order limit ({limit_orders})")
                break

            if total_pages is not None and pagina > total_pages:
                logger.info(f"Import run {run_id}: all {total_pages} pages fetched")
                break

            sort_param = "data-desc" if direction == "desc" else "data-asc"
            try:
                result = await client_a.list_orders(
                    pagina=pagina,
                    data_inicial=data_inicio,
                    data_final=data_fim,
                    limite=100,
                    sort=sort_param
                )
            except TinyApiError as e:
                if e.status_code == 401:
                    token_a = await ensure_access_token("A")
                    if not token_a:
                        await finish_import_run(run_id, "error", "Token refresh failed during import")
                        return
                    client_a = TinyClient(token_a)
                    try:
                        result = await client_a.list_orders(
                            pagina=pagina,
                            data_inicial=data_inicio,
                            data_final=data_fim,
                            limite=100,
                            sort=sort_param
                        )
                    except TinyApiError as e2:
                        await finish_import_run(run_id, "error", f"API error after token refresh: {e2.status_code}")
                        return
                else:
                    await finish_import_run(run_id, "error", f"API error page {pagina}: {e.status_code} {e.body[:200]}")
                    return

            itens = result.get("itens", [])
            if not itens:
                logger.info(f"Import run {run_id}: no more items at page {pagina}")
                break

            if total_pages is None:
                paginacao = result.get("paginacao", {})
                api_total = paginacao.get("total", 0)
                api_limit = paginacao.get("limit", 100) or 100
                if api_total > 0:
                    total_pages = math.ceil(api_total / api_limit)
                    logger.info(f"Import run {run_id}: API reports {api_total} total items, {total_pages} pages")
                    await update_import_run_api_total(run_id, api_total)
                else:
                    total_pages = 1
                    logger.info(f"Import run {run_id}: API total=0 or missing, assuming single page")

            page_ids = set()
            for order in itens:
                oid = str(order.get("id", ""))
                if oid:
                    page_ids.add(oid)

            new_ids_on_page = page_ids - seen_ids
            if page_ids and not new_ids_on_page:
                logger.warning(f"Import run {run_id}: page {pagina} returned only already-seen IDs, stopping (loop detected)")
                break

            seen_ids.update(page_ids)
            pages_fetched += 1

            for order in itens:
                orders_found += 1
                order_id = str(order.get("id", ""))
                numero = str(order.get("numero") or order.get("numeroPedido") or "")
                data_pedido = order.get("data") or order.get("dataPedido") or ""
                status_raw = extract_status(order)

                if status_raw not in ELIGIBLE_STATUSES:
                    orders_ignored += 1
                    motivo = f"status: {status_raw}" if status_raw else "status não identificado"
                    await insert_import_run_item(run_id, order_id, numero, data_pedido, status_raw, "ignored", motivo)
                    continue

                already_imported = await check_order_exists_in_map(order_id)
                if already_imported:
                    orders_skipped += 1
                    await insert_import_run_item(run_id, order_id, numero, data_pedido, status_raw, "already_imported")
                    continue

                dedupe_key = f"A:vendas:{order_id}:fetch_order_a"
                job_payload = {
                    "source": "A",
                    "topic": "vendas",
                    "venda_id": order_id,
                    "codigo_situacao": "aprovado",
                    "from_backfill": True,
                    "force_status_c": "entregue"
                }
                await insert_job(
                    job_type="fetch_order_a",
                    dedupe_key=dedupe_key,
                    event_id=None,
                    payload=job_payload
                )
                jobs_created += 1
                await insert_import_run_item(run_id, order_id, numero, data_pedido, status_raw, "job_created")

                if limit_orders and jobs_created >= limit_orders:
                    break

            await update_import_run_progress(run_id, pages_fetched, orders_found, jobs_created, orders_skipped, orders_ignored)

            pagina += 1
            await asyncio.sleep(0.5)

        await update_import_run_progress(run_id, pages_fetched, orders_found, jobs_created, orders_skipped, orders_ignored)
        await finish_import_run(run_id, "done")
        logger.info(f"Import run {run_id} done: {pages_fetched} pages, {orders_found} orders (unique={len(seen_ids)}), {jobs_created} jobs, {orders_skipped} skipped, {orders_ignored} ignored")

    except asyncio.CancelledError:
        await finish_import_run(run_id, "cancelled")
        logger.info(f"Import run {run_id} cancelled")
    except Exception as e:
        logger.error(f"Import run {run_id} error: {e}", exc_info=True)
        await finish_import_run(run_id, "error", str(e)[:500])
    finally:
        _running_tasks.pop(run_id, None)


async def start_import(data_inicio: str, data_fim: str, direction: str = "desc", limit_orders: int | None = None) -> int:
    run_id = await create_import_run(data_inicio, data_fim, direction, limit_orders)
    task = asyncio.create_task(run_import(run_id, data_inicio, data_fim, direction, limit_orders))
    _running_tasks[run_id] = task
    return run_id


def cancel_import(run_id: int) -> bool:
    task = _running_tasks.get(run_id)
    if task and not task.done():
        task.cancel()
        return True
    return False
