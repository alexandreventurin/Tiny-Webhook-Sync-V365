import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from app.db import (
    fetch_and_lock_jobs,
    update_job_done,
    update_job_failed,
    update_job_skipped_not_mapped,
    reschedule_job_with_backoff,
    upsert_orders_map,
    upsert_orders_a_snapshot,
    upsert_orders_a_fetched,
    upsert_orders_a_fetch_error,
    upsert_orders_c_fetched,
    upsert_orders_c_fetch_error,
    upsert_orders_map_with_c,
    get_order_a_snapshot,
    get_snapshot_fetched_at,
    get_order_mapping_by_a,
    get_order_mapping_by_c,
    get_venda_c_by_nota_fiscal,
    insert_job,
    reset_stale_locks,
    update_worker_heartbeat,
    count_orders_replicated_to_c,
    load_products_map,
    load_products_prices,
    get_feature_flag,
    update_orders_map_sync,
    update_job_waiting_sku,
    upsert_partial_product,
)
from app.settings import (
    TINY_A_TOKEN, TINY_C_TOKEN, 
    ENABLE_FETCH_A, EXECUTE_TINY_C, 
    ALLOW_VENDA_IDS, FETCH_CACHE_MINUTES,
    MAX_ORDERS_TO_REPLICATE
)
from app.tiny_client import TinyClient, TinyApiError
from app.tiny_oauth import ensure_access_token, force_refresh_token
from app.utils import normalize_status

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

WORKER_BUILD = "2026-03-19-001"

_rate_limit_cooldown_until: dict[str, float] = {}
RATE_LIMIT_RESERVE = 8  # stop when remaining <= this (1 order = ~6-7 API calls)

class RateLimitError(Exception):
    def __init__(self, account: str, status_code: int, retry_after: int = 60):
        self.account = account
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"Rate limited on account {account} (HTTP {status_code}), retry after {retry_after}s")

async def call_tiny(account: str, client: TinyClient, method: str, *args, **kwargs):
    """Call a TinyClient method with proactive rate limit control via x-ratelimit headers."""
    import time
    now = time.time()
    cooldown = _rate_limit_cooldown_until.get(account, 0)
    if now < cooldown:
        wait = cooldown - now
        logger.info(f"Rate limit cooldown for {account}: waiting {wait:.0f}s")
        raise RateLimitError(account, 429, int(wait))

    try:
        result = await getattr(client, method)(*args, **kwargs)

        # Proactive rate limit: check remaining quota after each call
        remaining = client.last_ratelimit_remaining
        reset_sec = client.last_ratelimit_reset or 60
        if remaining is not None and remaining <= RATE_LIMIT_RESERVE:
            _rate_limit_cooldown_until[account] = time.time() + reset_sec
            logger.info(f"Rate limit proactive pause for {account}: remaining={remaining}, waiting {reset_sec}s until reset")
            raise RateLimitError(account, 0, reset_sec)

        return result
    except TinyApiError as e:
        if e.status_code in (429, 403):
            reset_sec = client.last_ratelimit_reset or 60
            _rate_limit_cooldown_until[account] = time.time() + reset_sec
            logger.warning(f"Rate limit 429 on {method} for account {account}, cooldown {reset_sec}s")
            raise RateLimitError(account, e.status_code, reset_sec)
        if e.status_code != 401:
            raise
        logger.warning(f"Got 401 on {method} for account {account}, force-refreshing token...")
        new_token = await force_refresh_token(account)
        if not new_token:
            raise
        new_client = TinyClient(new_token)
        return await getattr(new_client, method)(*args, **kwargs)


async def refresh_order_c_snapshot(client_c: TinyClient, venda_c_id: str, account: str = "B") -> dict | None:
    try:
        details = await call_tiny(account, client_c, "get_order_details", str(venda_c_id))
        await upsert_orders_c_fetched(str(venda_c_id), details)
        return details
    except TinyApiError as exc:
        await upsert_orders_c_fetch_error(str(venda_c_id), exc.status_code, exc.body)
        logger.warning(f"Failed to refresh C snapshot for order {venda_c_id}: {exc.body[:160]}")
    except RateLimitError as exc:
        await upsert_orders_c_fetch_error(str(venda_c_id), exc.status_code, str(exc))
        logger.warning(f"Skipped C snapshot refresh for order {venda_c_id}: rate limit")
    except Exception as exc:
        await upsert_orders_c_fetch_error(str(venda_c_id), None, str(exc))
        logger.warning(f"Failed to refresh C snapshot for order {venda_c_id}: {exc}")
    return None


PRODUTO_ID_MAP: dict[int, int] = {}
SKU_PRICE_MAP: dict[str, float] = {}
_products_map_loaded = False

async def refresh_products_map():
    """Recarrega o mapeamento de produtos e preços do banco de dados."""
    global PRODUTO_ID_MAP, SKU_PRICE_MAP, _products_map_loaded
    try:
        PRODUTO_ID_MAP = await load_products_map()
        SKU_PRICE_MAP = await load_products_prices()
        _products_map_loaded = True
        logger.info(f"Products map reloaded: {len(PRODUTO_ID_MAP)} mappings, {len(SKU_PRICE_MAP)} prices")
        logger.debug(f"SKU_PRICE_MAP: {SKU_PRICE_MAP}")
    except Exception as e:
        logger.error(f"Failed to load products map: {e}")

DROPSHIPPING_DEPOSIT_ID_A = 336403602  # ID do depósito "Dropshipping (Muy Bela)" em Tiny A (Rejuderme)
DROPSHIPPING_DEPOSIT_ID_C = 888616671  # ID do depósito equivalente em V365 (Tiny C)

DEST1_FE_SEDEX_ID = 909868692   # formaEnvio: Rejuderme - Correios (Sedex) em V365
DEST1_FE_FM_ID = 909865320      # formaEnvio: Rejuderme - FM Transportes em V365
DEST1_FE_PAC_ID = 909863133     # formaEnvio: Rejuderme - Correios (PAC) em V365
DEST1_FE_ME_ID = 0
DEST1_PRICE_LIST_ID = 0         # Lista "Padrão" (ID 0 = padrão do sistema) em V365

DEST1_FF_FM_STANDARD_ID = 909867565  # formaFrete: Standard (FM Transportes) em V365
DEST1_FF_SEDEX_ID = 909868716        # formaFrete: SEDEX CONTRATO AG (03220) em V365
DEST1_FF_PAC_ID = 909863466          # formaFrete: PAC CONTRATO AG (03298) em V365

FORMA_ENVIO_MAP = {
    "FM Transportes": {
        "formaEnvioId": DEST1_FE_FM_ID,
        "fretePorConta": "R",
        "formaFreteMap": {
            "Standard": {"id": DEST1_FF_FM_STANDARD_ID, "nome": "Standard"},
            "FMSTD": {"id": DEST1_FF_FM_STANDARD_ID, "nome": "Standard"},
            "EXPRESSO": {"id": DEST1_FF_FM_STANDARD_ID, "nome": "Standard"},
            "FMEXP": {"id": DEST1_FF_FM_STANDARD_ID, "nome": "Standard"},
        },
        "defaultFormaFrete": {"id": DEST1_FF_FM_STANDARD_ID, "nome": "Standard"},
    },
    "Correios (Sedex)": {
        "formaEnvioId": DEST1_FE_SEDEX_ID,
        "fretePorConta": "R",
        "formaFreteMap": {
            "SEDEX CONTRATO AG (03220)": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
            "03220": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
            "SEDEX 12 CONTRATO AG (03140)": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
            "03140": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
            "SEDEX 10 CONTRATO AG (03158)": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
            "03158": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
            "SEDEX HOJE CONTRATO AG (03204)": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
            "03204": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
        },
        "defaultFormaFrete": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
    },
    "Correios (PAC)": {
        "formaEnvioId": DEST1_FE_PAC_ID,
        "fretePorConta": "R",
        "formaFreteMap": {
            "PAC CONTRATO AG (03298)": {"id": DEST1_FF_PAC_ID, "nome": "PAC CONTRATO AG (03298)"},
            "03298": {"id": DEST1_FF_PAC_ID, "nome": "PAC CONTRATO AG (03298)"},
            "CORREIOS MINI ENVIOS CTR AG (04227)": {"id": DEST1_FF_PAC_ID, "nome": "PAC CONTRATO AG (03298)"},
            "04227": {"id": DEST1_FF_PAC_ID, "nome": "PAC CONTRATO AG (03298)"},
        },
        "defaultFormaFrete": {"id": DEST1_FF_PAC_ID, "nome": "PAC CONTRATO AG (03298)"},
    },
    "Mercado Envios": {
        "formaEnvioId": DEST1_FE_ME_ID if DEST1_FE_ME_ID else None,
        "fretePorConta": "R",
        "formaFreteMap": {
            "PAC": {"id": DEST1_FF_PAC_ID, "nome": "PAC CONTRATO AG (03298)"},
            "21": {"id": DEST1_FF_PAC_ID, "nome": "PAC CONTRATO AG (03298)"},
            "Sedex": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
            "22": {"id": DEST1_FF_SEDEX_ID, "nome": "SEDEX CONTRATO AG (03220)"},
        },
        "defaultFormaFrete": None,
    },
}


def price_for(codigo: str | None, fallback: float) -> float:
    if codigo and codigo in SKU_PRICE_MAP:
        return float(SKU_PRICE_MAP[codigo])
    return float(fallback or 0)


def get_forma_envio_config(forma_envio_nome: str | None) -> dict:
    """Retorna a configuração de forma de envio para o destino B."""
    if not forma_envio_nome:
        logger.warning("get_forma_envio_config: forma_envio_nome is None")
        return {"formaEnvioId": None, "fretePorConta": "R", "formaFreteMap": {}, "defaultFormaFrete": None}
    config = FORMA_ENVIO_MAP.get(forma_envio_nome)
    if config:
        if not config.get("formaEnvioId"):
            logger.warning(f"get_forma_envio_config: '{forma_envio_nome}' configurado mas sem formaEnvioId (desabilitado)")
        return config
    logger.warning(f"get_forma_envio_config: '{forma_envio_nome}' não encontrado no FORMA_ENVIO_MAP")
    return {"formaEnvioId": None, "fretePorConta": "R", "formaFreteMap": {}, "defaultFormaFrete": None}


def map_forma_frete(forma_envio_nome: str | None, forma_frete_origem: str | None) -> dict | None:
    """Mapeia a forma de frete de A para B. Retorna dict {id, nome} ou None."""
    config = get_forma_envio_config(forma_envio_nome)
    frete_map = config.get("formaFreteMap", {})
    if forma_frete_origem and forma_frete_origem in frete_map:
        return frete_map[forma_frete_origem]
    return config.get("defaultFormaFrete")


async def build_itens_dest_v3(itens_src: list, retry_on_miss: bool = True) -> tuple:
    """Retorna (itens_mapeados, missing_skus). missing_skus = [(produto_id, sku), ...]"""
    global PRODUTO_ID_MAP
    out = []
    missing_ids = []

    for src in itens_src:
        if not isinstance(src, dict):
            continue
        produto = src.get("produto") or {}
        produto_id_origem = produto.get("id")
        if not produto_id_origem:
            logger.warning("Item sem produto.id, pulando")
            continue
        produto_id_destino = PRODUTO_ID_MAP.get(produto_id_origem)
        if not produto_id_destino:
            missing_ids.append((produto_id_origem, produto.get("sku") or "?"))
            continue
        sku = produto.get("sku") or ""
        quantidade = src.get("quantidade") or 1
        valor_unitario = src.get("valorUnitario") or 0
        valor_final = price_for(sku, valor_unitario)
        out.append({
            "produto": {"id": produto_id_destino},
            "quantidade": quantidade,
            "valorUnitario": float(valor_final),
            "infoAdicional": f"SKU: {sku} (Origem ID: {produto_id_origem})"
        })

    if missing_ids and retry_on_miss:
        logger.info(f"Found {len(missing_ids)} unmapped products, reloading from database...")
        await refresh_products_map()
        return await build_itens_dest_v3(itens_src, retry_on_miss=False)

    for pid, sku in missing_ids:
        logger.warning(f"Produto ID {pid} (SKU: {sku}) não mapeado, pulando")

    logger.info(f"build_itens_dest_v3: mapeados={len(out)} de {len(itens_src)}, missing={len(missing_ids)}")
    return out, missing_ids


def build_transportador_v3(
    forma_envio_origem: str | None,
    forma_frete_origem: str | None,
    codigo_rastreio: str | None,
    url_rastreio: str | None,
    volumes: int = 1
) -> dict:
    """Constrói o payload de transportador para criar pedido em B."""
    config = get_forma_envio_config(forma_envio_origem)
    forma_frete_dest = map_forma_frete(forma_envio_origem, forma_frete_origem)
    
    transportador = {
        "id": 0,
        "fretePorConta": config.get("fretePorConta", "R"),
        "codigoRastreamento": codigo_rastreio or "",
        "urlRastreamento": url_rastreio or "",
        "volumes": max(1, volumes),
    }
    
    forma_envio_id = config.get("formaEnvioId")
    if forma_envio_id:
        transportador["formaEnvio"] = {"id": forma_envio_id}
    if forma_frete_dest and isinstance(forma_frete_dest, dict) and forma_frete_dest.get("id"):
        transportador["formaFrete"] = forma_frete_dest
    
    logger.info(f"build_transportador_v3: forma_envio={forma_envio_origem}, forma_frete_origem={forma_frete_origem}, forma_frete_dest={forma_frete_dest}, volumes={volumes}")
    return transportador

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
        if job_type == 'approve_order_a':
            if not await get_feature_flag("auto_approve_open_orders"):
                action_preview = {"would": "approve_order_a", "skipped": True, "reason": "auto_approve_open_orders flag disabled"}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: auto_approve_open_orders flag disabled")
                return

            if not venda_id:
                await update_job_failed(job_id, "missing_venda_id", attempts)
                return

            token_a = await ensure_access_token("A")
            if not token_a:
                await update_job_failed(job_id, "No valid OAuth token for A", attempts)
                return

            client_a = TinyClient(token_a)
            a_details = await call_tiny("A", client_a, "get_order_details", str(venda_id))
            await upsert_orders_a_fetched(venda_a_id=str(venda_id), fetched_payload=a_details)
            current_status = normalize_status((a_details or {}).get("situacao"))
            if current_status != "em_aberto":
                action_preview = {
                    "would": "approve_order_a",
                    "skipped": True,
                    "reason": "status_not_open",
                    "venda_a_id": str(venda_id),
                    "current_status": current_status,
                }
                await update_job_done(job_id, action_preview)
                return

            await call_tiny("A", client_a, "update_order_status", str(venda_id), 3)
            refreshed = await call_tiny("A", client_a, "get_order_details", str(venda_id))
            await upsert_orders_a_fetched(venda_a_id=str(venda_id), fetched_payload=refreshed)

            create_order_dedupe_key = f"A:vendas:{venda_id}:create_order_c"
            create_order_payload = {
                "source": "A",
                "topic": "vendas",
                "venda_id": str(venda_id),
                "codigo_situacao": "aprovado",
            }
            create_job_created = await insert_job(job_type="create_order_c", dedupe_key=create_order_dedupe_key, event_id=None, payload=create_order_payload)
            action_preview = {
                "would": "approve_order_a",
                "done": True,
                "venda_a_id": str(venda_id),
                "next_job": "create_order_c",
                "create_job_created": create_job_created,
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} approved A:{venda_id} and queued create_order_c={create_job_created}")
            return

        if job_type == 'create_order_c':
            is_from_backfill = payload.get('from_backfill') or payload.get('force_status_c')
            flag_key = "replicate_imports" if is_from_backfill else "replicate_orders"
            if not await get_feature_flag(flag_key):
                action_preview = {"would": "create_order_in_C", "skipped": True, "reason": f"{flag_key} flag disabled"}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: {flag_key} flag disabled")
                return

            if not venda_id:
                await update_job_failed(job_id, "missing_venda_id", attempts)
                logger.warning(f"Job {job_id} failed: missing_venda_id")
                return
            
            max_orders = int(os.getenv("MAX_ORDERS_TO_REPLICATE", "0"))
            if max_orders > 0:
                current_count = await count_orders_replicated_to_c()
                if current_count >= max_orders:
                    action_preview = {
                        "would": "create_order_in_C",
                        "skipped": True,
                        "reason": f"MAX_ORDERS_TO_REPLICATE limit reached ({current_count}/{max_orders})",
                        "venda_a_id": venda_id
                    }
                    await update_job_done(job_id, action_preview)
                    logger.info(f"Job {job_id} skipped: MAX_ORDERS_TO_REPLICATE limit reached ({current_count}/{max_orders})")
                    return
            
            external_key = f"A:{venda_id}"

            # Sempre busca dados frescos de A para evitar snapshot obsoleto
            # (ex: CPF corrigido, endereço atualizado, etc.)
            token_a_fresh = await ensure_access_token("A")
            if token_a_fresh:
                try:
                    client_a_fresh = TinyClient(token_a_fresh)
                    fresh_data = await call_tiny("A", client_a_fresh, "get_order_details", str(venda_id))
                    await upsert_orders_a_fetched(venda_a_id=str(venda_id), fetched_payload=fresh_data)
                    order_data = fresh_data
                    logger.info(f"Job {job_id} create_order_c: refreshed snapshot from API for venda {venda_id}")
                except TinyApiError as e:
                    logger.warning(f"Job {job_id} create_order_c: fresh fetch failed ({e.status_code}), falling back to snapshot")
                    order_data = None
                except Exception as e:
                    logger.warning(f"Job {job_id} create_order_c: fresh fetch error ({e}), falling back to snapshot")
                    order_data = None
            else:
                logger.warning(f"Job {job_id} create_order_c: no token for A, falling back to snapshot")
                order_data = None

            # Fallback: se fetch fresco falhou, usa snapshot existente
            if not order_data:
                snapshot = await get_order_a_snapshot(str(venda_id))
                fetched_payload = snapshot.get('fetched_payload') if snapshot else None
                if fetched_payload and isinstance(fetched_payload, str):
                    fetched_payload = json.loads(fetched_payload)
                order_data = fetched_payload or {}

            if not order_data:
                await update_job_failed(job_id, "fetched_payload missing and fresh fetch failed", attempts)
                logger.warning(f"Job {job_id} failed: no data for venda {venda_id}")
                return

            deposito = order_data.get('deposito') or {}
            deposito_id = deposito.get('id')
            deposito_nome = deposito.get('nome', '')
            if deposito_id != DROPSHIPPING_DEPOSIT_ID_A:
                action_preview = {
                    "would": "create_order_in_C",
                    "skipped": True,
                    "reason": "deposit_not_allowed",
                    "venda_a_id": venda_id,
                    "deposito_id": deposito_id,
                    "deposito_nome": deposito_nome,
                    "expected_deposit_id": DROPSHIPPING_DEPOSIT_ID_A,
                    "note": f"Depósito '{deposito_nome}' não é Dropshipping (Rejuderme)"
                }
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: deposit_not_allowed (deposito={deposito_nome}, id={deposito_id})")
                return

            cliente = order_data.get('cliente') or {}
            endereco = order_data.get('enderecoEntrega') or order_data.get('endereco') or cliente.get('endereco') or {}
            itens_a = order_data.get('itens') or []
            
            cpf_cnpj = cliente.get('cpfCnpj') or cliente.get('cpf_cnpj') or ''
            
            missing_fields = []
            if not cliente.get('nome'):
                missing_fields.append('cliente.nome')
            if not cpf_cnpj:
                missing_fields.append('cliente.cpfCnpj')
            if not itens_a:
                missing_fields.append('itens')
            
            if not EXECUTE_TINY_C:
                await upsert_orders_map(external_key=external_key, venda_a_id=str(venda_id))
                action_preview = {
                    "would": "create_order_in_C",
                    "situacao_target": "em_aberto",
                    "venda_a_id": venda_id,
                    "external_key": external_key,
                    "dry_run": True,
                    "has_fetched_payload": fetched_payload is not None,
                    "cliente_nome": cliente.get('nome'),
                    "cpf_cnpj": cpf_cnpj,
                    "itens_count": len(itens_a),
                    "missing_fields": missing_fields if missing_fields else None,
                    "note": "EXECUTE_TINY_C=false"
                }
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} completed: create_order_c dry-run for {external_key}")
                return
            
            if missing_fields:
                await update_job_failed(job_id, f"missing_required_fields: {missing_fields}", attempts)
                logger.warning(f"Job {job_id} failed: missing required fields {missing_fields}")
                return
            
            token_c = await ensure_access_token("B")
            if not token_c:
                await update_job_failed(job_id, "No valid OAuth token for account B", attempts)
                logger.warning(f"Job {job_id} failed: No OAuth token for B")
                return
            
            client_c = TinyClient(token_c)
            
            id_contato_c = None
            contact_created = False
            contact_updated = False
            nome_raw = cliente.get('nome') or ''
            nome_truncado = nome_raw[:50] if len(nome_raw) > 50 else nome_raw
            if len(nome_raw) > 50:
                logger.warning(f"Job {job_id}: nome do contato truncado de {len(nome_raw)} para 50 chars: '{nome_raw}' -> '{nome_truncado}'")
            contact_payload = {
                "nome": nome_truncado,
                "cpfCnpj": cpf_cnpj,
                "tipoPessoa": cliente.get('tipoPessoa') or ('J' if len(cpf_cnpj.replace('.','').replace('-','').replace('/','')) > 11 else 'F'),
                "email": cliente.get('email'),
                "telefone": cliente.get('telefone') or cliente.get('fone'),
                "celular": cliente.get('celular'),
                "endereco": {
                    "endereco": endereco.get('endereco') or endereco.get('logradouro'),
                    "numero": endereco.get('enderecoNro') or endereco.get('numero'),
                    "complemento": endereco.get('complemento'),
                    "bairro": endereco.get('bairro'),
                    "municipio": endereco.get('municipio') or endereco.get('cidade'),
                    "cep": endereco.get('cep'),
                    "uf": endereco.get('uf')
                }
            }
            contact_payload = {k: v for k, v in contact_payload.items() if v is not None}
            if contact_payload.get('endereco'):
                contact_payload['endereco'] = {k: v for k, v in contact_payload['endereco'].items() if v is not None}

            try:
                contacts = await call_tiny("B", client_c, "search_contacts", cpf_cnpj)
                if contacts:
                    id_contato_c = contacts[0].get('id')
                    logger.info(f"Found existing contact in B: {id_contato_c}")
            except TinyApiError as e:
                logger.warning(f"Error searching contacts: {e}")
            
            if id_contato_c:
                try:
                    await call_tiny("B", client_c, "update_contact", str(id_contato_c), contact_payload)
                    contact_updated = True
                    logger.info(f"Updated contact in B with delivery address: {id_contato_c}")
                except TinyApiError as e:
                    await update_job_failed(job_id, f"Failed to update contact: {e.status_code} {e.body}", attempts)
                    logger.error(f"Job {job_id} failed to update contact {id_contato_c}: {e}")
                    return
            else:
                try:
                    contact_result = await call_tiny("B", client_c, "create_contact", contact_payload)
                    id_contato_c = contact_result.get('id')
                    contact_created = True
                    logger.info(f"Created contact in B: {id_contato_c}")
                except TinyApiError as e:
                    await update_job_failed(job_id, f"Failed to create contact: {e.status_code} {e.body}", attempts)
                    logger.error(f"Job {job_id} failed to create contact: {e}")
                    return
            
            itens_c, missing_skus = await build_itens_dest_v3(itens_a)

            if missing_skus:
                for pid, sku in missing_skus:
                    await upsert_partial_product(pid, sku)
                await update_job_waiting_sku(job_id, {
                    "would": "create_order_c",
                    "venda_a_id": venda_id,
                    "missing_skus": [{"id": pid, "sku": sku} for pid, sku in missing_skus],
                    "mapped_count": len(itens_c),
                    "total_count": len(itens_a),
                })
                logger.warning(f"Job {job_id} waiting_sku: {len(missing_skus)} unmapped SKUs for venda {venda_id}")
                return

            if not itens_c:
                await update_job_failed(job_id, "No products mapped from A to B (check products_map table)", attempts)
                logger.warning(f"Job {job_id} failed: no products mapped")
                return
            
            endereco_entrega = {
                "endereco": endereco.get('endereco') or endereco.get('logradouro'),
                "enderecoNro": endereco.get('enderecoNro') or endereco.get('numero'),
                "complemento": endereco.get('complemento'),
                "bairro": endereco.get('bairro'),
                "municipio": endereco.get('municipio') or endereco.get('cidade'),
                "cep": endereco.get('cep'),
                "uf": endereco.get('uf'),
                "nomeDestinatario": cliente.get('nome'),
                "cpfCnpj": cpf_cnpj,
                "fone": cliente.get('telefone') or cliente.get('fone')
            }
            endereco_entrega = {k: v for k, v in endereco_entrega.items() if v is not None}
            
            transportador_src = order_data.get('transportador') or {}
            forma_envio_obj = transportador_src.get('formaEnvio') or {}
            forma_envio_src = forma_envio_obj.get('nome')
            forma_frete_obj = transportador_src.get('formaFrete') or {}
            forma_frete_src = forma_frete_obj.get('nome') if isinstance(forma_frete_obj, dict) else forma_frete_obj
            codigo_rastreio = transportador_src.get('codigoRastreamento')
            url_rastreio = transportador_src.get('urlRastreamento')
            volumes_raw = transportador_src.get('volumes')
            if isinstance(volumes_raw, int):
                volumes_src = max(1, volumes_raw)
            elif isinstance(volumes_raw, str) and volumes_raw.isdigit():
                volumes_src = max(1, int(volumes_raw))
            else:
                volumes_src = 1
            
            ecommerce_src = order_data.get('ecommerce') or {}
            numero_pedido_ecommerce = ecommerce_src.get('numeroPedidoEcommerce') or ""
            
            obs_extra = f"[Origem: {forma_envio_src or 'N/A'}"
            if forma_frete_src:
                obs_extra += f" | Frete: {forma_frete_src}"
            obs_extra += "]"
            
            # numeroOrdemCompra recebe o numeroPedidoEcommerce de A (número da Shopify).
            # Se não houver, fica vazio — para tornar perceptível visualmente quando algo
            # deu errado (não fazemos fallback para numeroPedido).
            order_payload_c = {
                "data": order_data.get('data'),
                "idContato": id_contato_c,
                "numeroOrdemCompra": str(numero_pedido_ecommerce or ""),
                "itens": itens_c,
                "enderecoEntrega": endereco_entrega,
                "listaPreco": {"id": DEST1_PRICE_LIST_ID},
                "vendedor": {"id": 906538550},  # Rejuderme em V365
                "transportador": build_transportador_v3(
                    forma_envio_origem=forma_envio_src,
                    forma_frete_origem=forma_frete_src,
                    codigo_rastreio=codigo_rastreio,
                    url_rastreio=url_rastreio,
                    volumes=volumes_src
                ),
                "observacoes": f"Repasse Tiny - origem id {order_data.get('id')} nº {order_data.get('numeroPedido')} {obs_extra}",
                "valorFrete": 0,
                "valorDesconto": 0,
                "pagamento": {
                    "formaPagamento": {"id": 932361522, "nome": "Conta Rejuderme"},
                    "formaRecebimento": {"id": 932361522, "nome": "Conta Rejuderme"},
                    "meioPagamento": None,
                    "condicaoPagamento": "0",
                    "parcelas": [
                        {
                            "dias": 0,
                            "observacoes": "API REJUDERME",
                            "formaPagamento": {"id": 932361522, "nome": "Conta Rejuderme"},
                            "formaRecebimento": {"id": 932361522, "nome": "Conta Rejuderme"},
                            "meioPagamento": None
                        }
                    ]
                }
            }
            if numero_pedido_ecommerce:
                order_payload_c["ecommerce"] = {"id": 0, "numeroPedidoEcommerce": numero_pedido_ecommerce}
            order_payload_c = {k: v for k, v in order_payload_c.items() if v is not None}
            
            logger.info(f"create_order_c payload for venda {venda_id}: {json.dumps(order_payload_c, default=str)}")
            result = await call_tiny("B", client_c, "create_order", order_payload_c)
            venda_c_id = str(result.get('id') or result.get('numeroPedido') or '')
            
            await upsert_orders_map_with_c(external_key=external_key, venda_a_id=str(venda_id), venda_c_id=venda_c_id)
            if venda_c_id:
                await refresh_order_c_snapshot(client_c, venda_c_id)
            
            tag_added = False
            tag_job_created = False
            try:
                tag_added = await call_tiny("B", client_c, "add_order_tags", venda_c_id, ["API Rejuderme"])
            except RateLimitError:
                logger.warning(f"Job {job_id}: rate limited adding tag to C order {venda_c_id}, will create retry job")
            except Exception as e:
                logger.warning(f"Job {job_id}: failed to add tag to order {venda_c_id}: {e}")
            if not tag_added:
                tag_dedupe = f"C:tag:{venda_c_id}:add_tag_c"
                tag_payload = {"venda_c_id": venda_c_id, "tag": "API Rejuderme"}
                tag_job_created = await insert_job(job_type="add_tag_c", dedupe_key=tag_dedupe, event_id=None, payload=tag_payload, delay_minutes=1)
                if tag_job_created:
                    logger.info(f"Job {job_id}: tag failed, created add_tag_c job for order {venda_c_id}")

            tag_a_added = False
            tag_a_job_created = False
            try:
                token_a = await ensure_access_token("A")
                if token_a:
                    client_a = TinyClient(token_a)
                    tag_a_added = await call_tiny("A", client_a, "add_order_tags", str(venda_id), ["V365"])
                else:
                    logger.warning(f"Job {job_id}: no token for A, skipping tag V365 on order {venda_id}")
            except RateLimitError:
                logger.warning(f"Job {job_id}: rate limited adding tag V365 to A order {venda_id}, will create retry job")
            except Exception as e:
                logger.warning(f"Job {job_id}: failed to add tag V365 to A order {venda_id}: {e}")
            if not tag_a_added:
                tag_a_dedupe = f"A:tag:{venda_id}:add_tag_a"
                tag_a_payload = {"venda_a_id": str(venda_id), "tag": "V365"}
                tag_a_job_created = await insert_job(job_type="add_tag_a", dedupe_key=tag_a_dedupe, event_id=None, payload=tag_a_payload, delay_minutes=1)
                if tag_a_job_created:
                    logger.info(f"Job {job_id}: tag A failed, created add_tag_a job for order {venda_id}")
                else:
                    logger.warning(f"Job {job_id}: tag A failed and add_tag_a job was NOT created (dedupe or error)")

            status_a_normalized = normalize_status(order_data.get('situacao'))
            force_status_c = payload.get('force_status_c') or ("aprovado" if status_a_normalized == "aprovado" else None)
            force_status_applied = False
            if force_status_c and venda_c_id:
                FORCE_SITUACAO_CODE = {"aprovado": 3, "entregue": 6}
                force_code = FORCE_SITUACAO_CODE.get(force_status_c)
                if force_code:
                    try:
                        await call_tiny("B", client_c, "update_order_status", venda_c_id, force_code)
                        await refresh_order_c_snapshot(client_c, venda_c_id)
                        force_status_applied = True
                        logger.info(f"Job {job_id}: forced status '{force_status_c}' ({force_code}) on C order {venda_c_id}")
                    except Exception as e:
                        logger.warning(f"Job {job_id}: failed to force status '{force_status_c}' on C order {venda_c_id}: {e}")

            action_preview = {
                "would": "create_order_in_C",
                "situacao_target": force_status_c if force_status_applied else "em_aberto",
                "venda_a_id": venda_id,
                "venda_c_id": venda_c_id,
                "external_key": external_key,
                "id_contato_c": id_contato_c,
                "contact_created": contact_created,
                "contact_updated": contact_updated,
                "itens_mapped": len(itens_c),
                "forma_envio_origem": forma_envio_src,
                "forma_frete_origem": forma_frete_src,
                "volumes": volumes_src,
                "created": True,
                "tag_added": tag_added,
                "tag_job_created": tag_job_created if not tag_added else None,
                "tag_a_added": tag_a_added,
                "tag_a_job_created": tag_a_job_created if not tag_a_added else None,
                "force_status_c": force_status_c if force_status_c else None,
                "force_status_applied": force_status_applied if force_status_c else None,
                "payload_sent_to_c": order_payload_c
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: created order in C with id {venda_c_id} (envio={forma_envio_src}, frete={forma_frete_src}, tag={tag_added}, force={force_status_applied if force_status_c else 'n/a'})")
        
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
                    create_order_dedupe_key = f"A:vendas:{venda_id}:create_order_c"
                    create_order_payload = {
                        "source": "A", "topic": "vendas", "venda_id": str(venda_id),
                        "codigo_situacao": codigo_situacao, "id_nota_fiscal": id_nota_fiscal,
                        "from_fetch_order_a": True
                    }
                    if payload.get('force_status_c'):
                        create_order_payload["force_status_c"] = payload["force_status_c"]
                    if payload.get('from_backfill'):
                        create_order_payload["from_backfill"] = True
                    await insert_job(job_type="create_order_c", dedupe_key=create_order_dedupe_key, event_id=None, payload=create_order_payload)
                    return
            
            token_a = await ensure_access_token("A")
            if not token_a:
                await update_job_failed(job_id, "No valid OAuth token for account A", attempts)
                logger.warning(f"Job {job_id} failed: No OAuth token for A (will retry)")
                return
            
            client_a = TinyClient(token_a)
            try:
                fetched_data = await call_tiny("A", client_a, "get_order_details", str(venda_id))
            except TinyApiError as e:
                await upsert_orders_a_fetch_error(venda_a_id=str(venda_id), status_code=e.status_code, error_body=e.body)
                logger.error(f"Job {job_id} fetch_order_a failed: {e.status_code} {e.body[:100]}")
                raise
            
            await upsert_orders_a_fetched(venda_a_id=str(venda_id), fetched_payload=fetched_data)
            action_preview = {
                "would": "fetch_order_a",
                "venda_a_id": venda_id,
                "fetched": True,
                "note": "fetched from Tiny A"
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: fetch_order_a for venda {venda_id}")
            
            create_order_dedupe_key = f"A:vendas:{venda_id}:create_order_c"
            create_order_payload = {
                "source": "A",
                "topic": "vendas",
                "venda_id": str(venda_id),
                "codigo_situacao": codigo_situacao,
                "id_nota_fiscal": id_nota_fiscal,
                "from_fetch_order_a": True
            }
            if payload.get('force_status_c'):
                create_order_payload["force_status_c"] = payload["force_status_c"]
            if payload.get('from_backfill'):
                create_order_payload["from_backfill"] = True
            await insert_job(job_type="create_order_c", dedupe_key=create_order_dedupe_key, event_id=None, payload=create_order_payload)
            logger.info(f"Chained create_order_c job for venda {venda_id}")
        
        elif job_type == 'sync_status':
            flag_key = f"sync_status_{codigo_situacao}" if codigo_situacao else None
            if flag_key and not await get_feature_flag(flag_key):
                action_preview = {"would": "sync_status", "skipped": True, "reason": f"{flag_key} flag disabled", "situacao": codigo_situacao}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: {flag_key} flag disabled")
                return

            SITUACAO_CODE = {
                "aprovado": 3,
                "faturado": 1,
                "cancelado": 2,
                "enviado": 5,
                "entregue": 6,
                "nao_entregue": 9,
            }
            
            situacao_int = SITUACAO_CODE.get(codigo_situacao)
            if not situacao_int:
                action_preview = {"would": "sync_status", "skipped": True, "reason": f"unknown situacao '{codigo_situacao}'"}
                await update_job_done(job_id, action_preview)
                logger.warning(f"sync_status: unknown situacao '{codigo_situacao}' for venda {venda_id}")
                return
            
            if source == "A":
                mapping = await get_order_mapping_by_a(str(venda_id))
                if not mapping:
                    action_preview = {
                        "would": "sync_status",
                        "skipped": True,
                        "reason": "not_mapped",
                        "source": source,
                        "venda_a_id": str(venda_id),
                        "note": "Pedido de A sem replicação em B"
                    }
                    await update_job_skipped_not_mapped(job_id, action_preview)
                    logger.info(f"Job {job_id} skipped_not_mapped: venda_a_id={venda_id}")
                    return
                target_id = str(mapping["venda_c_id"])
                target_source = "B"
                target_token = await ensure_access_token("B")
            elif source == "B":
                mapping = await get_order_mapping_by_c(str(venda_id))
                if not mapping:
                    # TODO: futuramente, chamar GET /pedidos/{venda_id} no Tiny B
                    # para verificar se vendedor == Rejuderme (ID do vendedor em V365).
                    # Por ora, marca como not_mapped (pedido próprio da V365).
                    action_preview = {
                        "would": "sync_status",
                        "skipped": True,
                        "reason": "not_mapped",
                        "note": f"venda_c_id={venda_id} não existe na orders_map (pedido não replicado)",
                    }
                    await update_job_skipped_not_mapped(job_id, action_preview)
                    logger.info(f"Job {job_id} skipped_not_mapped: venda_c_id={venda_id}")
                    return
                target_id = str(mapping["venda_a_id"])
                target_source = "A"
                target_token = await ensure_access_token("A")
            else:
                raise Exception(f"sync_status: unknown source '{source}'")
            
            logger.info(f"sync_status: {source} venda {venda_id} -> {target_source} venda {target_id}, situacao={codigo_situacao} ({situacao_int})")
            
            if not target_token:
                action_preview = {"would": "sync_status", "skipped": True, "reason": f"token for {target_source} not available"}
                await update_job_done(job_id, action_preview)
                logger.warning(f"sync_status: token for {target_source} not available")
                return
            
            client_target = TinyClient(target_token)
            
            await call_tiny(target_source, client_target, "update_order_status", target_id, situacao_int)
            if target_source == "B":
                await refresh_order_c_snapshot(client_target, target_id)
            elif source == "B":
                source_token = await ensure_access_token("B")
                if source_token:
                    await refresh_order_c_snapshot(TinyClient(source_token), str(venda_id))
            
            if source == "A":
                await update_orders_map_sync(str(venda_id), target_id, codigo_situacao)
            else:
                await update_orders_map_sync(target_id, str(venda_id), codigo_situacao)
            
            action_preview = {
                "would": "sync_status",
                "done": True,
                "source": source,
                "venda_id": str(venda_id),
                "target_source": target_source,
                "target_venda_id": target_id,
                "situacao": codigo_situacao,
                "situacao_code": situacao_int,
            }
            
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: sync_status {source}:{venda_id} -> {target_source}:{target_id} = {codigo_situacao}")
        
        elif job_type == 'sync_nf_link':
            if not await get_feature_flag("sync_nf_link"):
                action_preview = {"would": "sync_nf_link", "skipped": True, "reason": "sync_nf_link flag disabled"}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: sync_nf_link flag disabled")
                return

            if not id_nota_fiscal:
                await update_job_failed(job_id, "missing id_nota_fiscal", attempts)
                return

            nf_numero = str(payload.get("nf_numero") or "")
            nf_serie = str(payload.get("nf_serie") or "")
            nf_chave_acesso = payload.get("nf_chave_acesso") or ""
            nf_data_emissao = payload.get("nf_data_emissao") or ""
            nf_protocolo = ""
            nf_data_autorizacao = ""
            nf_source = "webhook"

            if not nf_chave_acesso:
                logger.info(f"Job {job_id}: NF data not in payload, trying event fallback")
                from app.db import get_nf_event_payload
                ev_payload = await get_nf_event_payload(id_nota_fiscal)
                if ev_payload:
                    ev_dados = ev_payload.get("dados") or {}
                    nf_numero = str(ev_dados.get("numero") or "")
                    nf_serie = str(ev_dados.get("serie") or "")
                    nf_chave_acesso = ev_dados.get("chaveAcesso") or ev_dados.get("chave_acesso") or ""
                    nf_source = "event_fallback"
                if not nf_chave_acesso:
                    await update_job_failed(job_id, f"No NF data in payload or events for id_nota_fiscal={id_nota_fiscal}", attempts)
                    return

            venda_c_id_nf = await get_venda_c_by_nota_fiscal(id_nota_fiscal)

            if not venda_c_id_nf:
                action_preview = {"would": "sync_nf_link", "skipped": True, "reason": "no_vendas_event_for_nf", "id_nota_fiscal": id_nota_fiscal}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id}: no B/vendas event with id_nota_fiscal={id_nota_fiscal}, skipping")
                return

            mapping = await get_order_mapping_by_c(venda_c_id_nf)
            if not mapping:
                action_preview = {"would": "sync_nf_link", "skipped": True, "reason": "no_orders_map", "venda_c_id": venda_c_id_nf, "id_nota_fiscal": id_nota_fiscal}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id}: no orders_map for venda_b {venda_c_id_nf}, skipping")
                return

            venda_a_id = mapping.get("venda_a_id")
            if not venda_a_id:
                await update_job_failed(job_id, f"orders_map for B:{venda_c_id_nf} has no venda_a_id", attempts)
                return

            token_a = await ensure_access_token("A")
            if not token_a:
                await update_job_failed(job_id, "No valid OAuth token for A", attempts)
                return

            client_a = TinyClient(token_a)
            order_a = await call_tiny("A", client_a, "get_order_details", venda_a_id)
            obs_atual = order_a.get("observacoes") or ""

            nf_block_lines = [
                f"NF {nf_numero} - {nf_serie} | CHAVE DE ACESSO",
                nf_chave_acesso,
            ]
            if nf_protocolo or nf_data_autorizacao:
                nf_block_lines.append("PROTOCOLO DE AUTORIZAÇÃO DE USO")
                nf_block_lines.append(f"{nf_protocolo} - {nf_data_autorizacao}")
            nf_block = "\n".join(nf_block_lines)

            if nf_chave_acesso and nf_chave_acesso in obs_atual:
                action_preview = {"would": "sync_nf_link", "skipped": True, "reason": "nf_already_in_obs", "venda_a_id": venda_a_id, "nf_numero": nf_numero}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id}: NF {nf_numero} already in observations of A:{venda_a_id}, skipping")
                return

            if obs_atual.strip():
                nova_obs = obs_atual.rstrip() + "\n\n" + nf_block
            else:
                nova_obs = nf_block

            await call_tiny("A", client_a, "update_order", venda_a_id, {"observacoes": nova_obs})

            action_preview = {
                "would": "sync_nf_link",
                "done": True,
                "id_nota_fiscal": id_nota_fiscal,
                "venda_c_id": venda_c_id_nf,
                "venda_a_id": venda_a_id,
                "nf_numero": nf_numero,
                "nf_serie": nf_serie,
                "nf_chave_acesso": nf_chave_acesso[:20] + "..." if len(nf_chave_acesso) > 20 else nf_chave_acesso,
                "nf_source": nf_source,
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: sync_nf_link NF {nf_numero} for A:{venda_a_id} (from B:{venda_c_id_nf}, source={nf_source})")
        
        elif job_type == 'add_tag_c':
            TAG_BACKOFF_MINUTES = [1, 3, 5]
            TAG_MAX_RETRIES = len(TAG_BACKOFF_MINUTES)

            tag_venda_c_id = payload.get('venda_c_id')
            tag_text = payload.get('tag', 'API Rejuderme')

            if not tag_venda_c_id:
                await update_job_failed(job_id, "missing venda_c_id", attempts)
                return

            token_c = await ensure_access_token("B")
            if not token_c:
                if attempts <= TAG_MAX_RETRIES:
                    delay = TAG_BACKOFF_MINUTES[attempts - 1]
                    await reschedule_job_with_backoff(job_id, attempts, delay, "No valid OAuth token for B")
                    logger.info(f"Job {job_id} add_tag_c rescheduled (attempt {attempts}, retry in {delay}min)")
                else:
                    await update_job_failed(job_id, "No valid OAuth token for B after retries", attempts)
                return

            client_c = TinyClient(token_c)
            tag_ok = False
            tag_error = ""
            try:
                tag_ok = await call_tiny("B", client_c, "add_order_tags", tag_venda_c_id, [tag_text])
            except Exception as e:
                tag_error = str(e)
                logger.warning(f"Job {job_id} add_tag_c failed: {e}")

            if tag_ok:
                action_preview = {"would": "add_tag_c", "venda_c_id": tag_venda_c_id, "tag": tag_text, "tag_added": True, "attempts": attempts}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} completed: add_tag_c for order {tag_venda_c_id} (attempt {attempts})")
            elif attempts <= TAG_MAX_RETRIES:
                delay = TAG_BACKOFF_MINUTES[attempts - 1]
                await reschedule_job_with_backoff(job_id, attempts, delay, tag_error or "tag request failed")
                logger.info(f"Job {job_id} add_tag_c rescheduled (attempt {attempts}/{TAG_MAX_RETRIES}, retry in {delay}min)")
            else:
                await update_job_failed(job_id, f"add_tag_b failed after {attempts} attempts: {tag_error}", attempts)
                logger.warning(f"Job {job_id} add_tag_c gave up after {attempts} attempts for order {tag_venda_c_id}")

        elif job_type == 'add_tag_a':
            TAG_BACKOFF_MINUTES = [1, 3, 5]
            TAG_MAX_RETRIES = len(TAG_BACKOFF_MINUTES)

            tag_venda_a_id = payload.get('venda_a_id')
            tag_text = payload.get('tag', 'V365')

            if not tag_venda_a_id:
                await update_job_failed(job_id, "missing venda_a_id", attempts)
                return

            token_a = await ensure_access_token("A")
            if not token_a:
                if attempts <= TAG_MAX_RETRIES:
                    delay = TAG_BACKOFF_MINUTES[attempts - 1]
                    await reschedule_job_with_backoff(job_id, attempts, delay, "No valid OAuth token for A")
                    logger.info(f"Job {job_id} add_tag_a rescheduled (attempt {attempts}, retry in {delay}min)")
                else:
                    await update_job_failed(job_id, "No valid OAuth token for A after retries", attempts)
                return

            client_a = TinyClient(token_a)
            tag_ok = False
            tag_error = ""
            try:
                tag_ok = await call_tiny("A", client_a, "add_order_tags", tag_venda_a_id, [tag_text])
            except Exception as e:
                tag_error = str(e)
                logger.warning(f"Job {job_id} add_tag_a failed: {e}")

            if tag_ok:
                action_preview = {"would": "add_tag_a", "venda_a_id": tag_venda_a_id, "tag": tag_text, "tag_added": True, "attempts": attempts}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} completed: add_tag_a for order {tag_venda_a_id} (attempt {attempts})")
            elif attempts <= TAG_MAX_RETRIES:
                delay = TAG_BACKOFF_MINUTES[attempts - 1]
                await reschedule_job_with_backoff(job_id, attempts, delay, tag_error or "tag request failed")
                logger.info(f"Job {job_id} add_tag_a rescheduled (attempt {attempts}/{TAG_MAX_RETRIES}, retry in {delay}min)")
            else:
                await update_job_failed(job_id, f"add_tag_a failed after {attempts} attempts: {tag_error}", attempts)
                logger.warning(f"Job {job_id} add_tag_a gave up after {attempts} attempts for order {tag_venda_a_id}")

        elif job_type == 'sync_tracking_c_to_a':
            # Sincroniza codigo/url de rastreio de C para A quando C entra em pronto_envio.
            # Se A estiver num status anterior, avança A para pronto_envio (7).
            # Se A já estiver à frente (entregue etc), só grava rastreio e mantém status.
            if not await get_feature_flag("sync_tracking_pronto_envio"):
                action_preview = {"would": "sync_tracking", "skipped": True, "reason": "sync_tracking_pronto_envio flag disabled"}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: sync_tracking_pronto_envio flag disabled")
                return

            if not venda_id:
                await update_job_failed(job_id, "missing venda_id (venda_c_id)", attempts)
                return

            venda_c_id_str = str(venda_id)
            mapping = await get_order_mapping_by_c(venda_c_id_str)
            if not mapping or not mapping.get("venda_a_id"):
                action_preview = {
                    "would": "sync_tracking",
                    "skipped": True,
                    "reason": "not_mapped",
                    "venda_c_id": venda_c_id_str,
                    "note": "Pedido criado direto em C, sem par em A",
                }
                await update_job_skipped_not_mapped(job_id, action_preview)
                logger.info(f"Job {job_id} sync_tracking skipped_not_mapped: venda_c_id={venda_c_id_str}")
                return

            venda_a_id_str = str(mapping["venda_a_id"])
            external_key = mapping.get("external_key")

            # 1) Lê detalhes do pedido em C pra pegar codigoRastreamento/urlRastreamento
            token_c = await ensure_access_token("B")
            if not token_c:
                await update_job_failed(job_id, "No valid OAuth token for B", attempts)
                return
            client_c = TinyClient(token_c)
            c_details = await refresh_order_c_snapshot(client_c, venda_c_id_str) or {}
            transportador_c = (c_details.get("transportador") or {}) if isinstance(c_details, dict) else {}
            codigo_rastreio = (transportador_c.get("codigoRastreamento") or "").strip()
            url_rastreio = (transportador_c.get("urlRastreamento") or "").strip()

            if not codigo_rastreio:
                # Delay progressivo: tentativas 0→1min, 1→1min, 2→1min, 3→2min, 4→3min
                TRACKING_DELAYS = [1, 1, 1, 2, 3]
                if attempts < len(TRACKING_DELAYS):
                    delay = TRACKING_DELAYS[attempts]
                    logger.info(f"Job {job_id} sync_tracking: C:{venda_c_id_str} sem código rastreio (tentativa {attempts+1}/{len(TRACKING_DELAYS)+1}), reagendando em {delay}min")
                    await reschedule_job_with_backoff(job_id, attempts + 1, delay, "no_tracking_code_yet")
                    return
                # Esgotou tentativas — marca como done/skipped
                action_preview = {
                    "would": "sync_tracking",
                    "skipped": True,
                    "reason": "no_tracking_code_after_retries",
                    "venda_c_id": venda_c_id_str,
                    "venda_a_id": venda_a_id_str,
                    "attempts": attempts,
                    "url_rastreio": url_rastreio or None,
                    "note": "Código de rastreio vazio após todas as tentativas",
                }
                await update_job_done(job_id, action_preview)
                logger.warning(f"Job {job_id} sync_tracking: C:{venda_c_id_str} sem código rastreio após {attempts} tentativas")
                return

            # 2) Atualiza rastreio em A via PUT /pedidos/{id}/despacho
            token_a = await ensure_access_token("A")
            if not token_a:
                await update_job_failed(job_id, "No valid OAuth token for A", attempts)
                return
            client_a = TinyClient(token_a)
            try:
                await call_tiny("A", client_a, "update_order_despacho", venda_a_id_str, codigo_rastreio, url_rastreio)
            except TinyApiError as e:
                # 400 típico: "Não é possível alterar a forma de envio de um pedido que já possui uma expedição criada"
                if e.status_code == 400 and "expedi" in (e.body or "").lower():
                    action_preview = {
                        "would": "sync_tracking",
                        "skipped": True,
                        "reason": "a_has_expedicao",
                        "venda_a_id": venda_a_id_str,
                        "error_body": e.body[:200],
                    }
                    await update_job_done(job_id, action_preview)
                    logger.warning(f"Job {job_id} sync_tracking: A:{venda_a_id_str} já tem expedição, pulando update despacho")
                    return
                raise

            # 3) Só avança status se A estiver atrás de pronto_envio (7). Não rebaixa.
            # Status numéricos oficiais do Tiny v3:
            # 0=em_aberto, 1=faturado, 2=cancelado, 3=aprovado, 4=preparando_envio,
            # 5=enviado, 6=entregue, 7=pronto_envio, 8=dados_incompletos, 9=nao_entregue
            STATUS_RANK = {
                "em_aberto": 1, "dados_incompletos": 1,
                "aprovado": 2,
                "preparando_envio": 3,
                "pronto_envio": 4,
                "enviado": 5,
                "entregue": 6,
                "faturado": 7,
                "nao_entregue": 8,
                "cancelado": 99,
            }
            PRONTO_ENVIO_CODE = 7
            PRONTO_ENVIO_RANK = STATUS_RANK["pronto_envio"]

            a_details = await call_tiny("A", client_a, "get_order_details", venda_a_id_str)
            a_situacao_raw = (a_details or {}).get("situacao")
            from app.utils import normalize_status
            a_status_name = normalize_status(a_situacao_raw)
            a_rank = STATUS_RANK.get(a_status_name or "", 0)

            status_updated = False
            if a_rank < PRONTO_ENVIO_RANK:
                await call_tiny("A", client_a, "update_order_status", venda_a_id_str, PRONTO_ENVIO_CODE)
                status_updated = True
                logger.info(f"Job {job_id} sync_tracking: A:{venda_a_id_str} avançado para pronto_envio (de {a_status_name})")
            else:
                logger.info(f"Job {job_id} sync_tracking: A:{venda_a_id_str} já está em '{a_status_name}' (rank {a_rank}), mantém status")

            # 4) Marca no orders_map pra echo prevention (janela de 5min)
            await update_orders_map_sync(venda_a_id_str, venda_c_id_str, "pronto_envio")

            action_preview = {
                "would": "sync_tracking",
                "done": True,
                "venda_c_id": venda_c_id_str,
                "venda_a_id": venda_a_id_str,
                "external_key": external_key,
                "codigo_rastreamento": codigo_rastreio,
                "url_rastreamento": url_rastreio,
                "a_previous_status": a_status_name,
                "status_updated": status_updated,
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: sync_tracking C:{venda_c_id_str} -> A:{venda_a_id_str} (codigo={codigo_rastreio}, status_updated={status_updated})")

        elif job_type == 'update_numero_compra':
            # Atualiza apenas o campo numeroOrdemCompra de um pedido existente em C
            venda_c_id_str = payload.get('venda_c_id')
            venda_a_id_str = payload.get('venda_a_id')
            numero_pedido_ecommerce = payload.get('numero_pedido_ecommerce')

            if not venda_c_id_str or not numero_pedido_ecommerce:
                action_preview = {
                    "would": "update_numero_compra",
                    "skipped": True,
                    "reason": "missing_venda_c_id_or_numero_ecommerce",
                    "venda_c_id": venda_c_id_str,
                    "venda_a_id": venda_a_id_str,
                }
                await update_job_done(job_id, action_preview)
                logger.warning(f"Job {job_id} update_numero_compra skipped: missing data")
                return

            token_c = await ensure_access_token("B")
            if not token_c:
                await update_job_failed(job_id, "No valid OAuth token for B", attempts)
                return

            client_c = TinyClient(token_c)
            try:
                await call_tiny("B", client_c, "update_order", venda_c_id_str, {
                    "numeroOrdemCompra": str(numero_pedido_ecommerce)
                })
            except TinyApiError as e:
                # 404 = pedido não existe mais em C (foi deletado)
                if e.status_code == 404:
                    action_preview = {
                        "would": "update_numero_compra",
                        "skipped": True,
                        "reason": "order_not_found_in_c",
                        "venda_c_id": venda_c_id_str,
                        "venda_a_id": venda_a_id_str,
                    }
                    await update_job_done(job_id, action_preview)
                    logger.warning(f"Job {job_id} update_numero_compra: C:{venda_c_id_str} 404 not found")
                    return
                raise

            action_preview = {
                "would": "update_numero_compra",
                "updated": True,
                "venda_c_id": venda_c_id_str,
                "venda_a_id": venda_a_id_str,
                "numero_pedido_ecommerce": numero_pedido_ecommerce,
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} update_numero_compra: C:{venda_c_id_str} ← {numero_pedido_ecommerce}")

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
    
    except RateLimitError as e:
        delay_minutes = max(1, (e.retry_after + 59) // 60)  # round up to next minute
        logger.warning(f"Job {job_id} rate limited, rescheduling in {delay_minutes}min (reset in {e.retry_after}s)")
        safe_attempts = max(0, attempts - 1)
        await reschedule_job_with_backoff(job_id, safe_attempts, delay_minutes, str(e))
        raise  # propagate so the batch loop can abort remaining jobs

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Job {job_id} failed: {error_msg}")
        await update_job_failed(job_id, error_msg, attempts)


async def _requeue_remaining_jobs(jobs: list[dict], delay_minutes: int = 2):
    """Requeue jobs that weren't processed due to rate limiting."""
    for job in jobs:
        job_id = job['id']
        attempts = job.get('attempts') or 0
        await reschedule_job_with_backoff(job_id, attempts, delay_minutes, "rate_limit_batch_abort")
    if jobs:
        logger.info(f"Rate limit: requeued {len(jobs)} remaining jobs with {delay_minutes}min delay")


async def run_worker_once(limit: int = 25) -> int:
    await reset_stale_locks()

    jobs = await fetch_and_lock_jobs(limit=limit)

    if not jobs:
        return 0

    for i, job in enumerate(jobs):
        try:
            await process_job(job)
        except RateLimitError:
            # Rate limit hit — requeue remaining unprocessed jobs and stop batch
            await _requeue_remaining_jobs(jobs[i+1:])
            break

    return len(jobs)


async def run_worker_once_detailed(limit: int = 50) -> dict:
    await reset_stale_locks()

    jobs = await fetch_and_lock_jobs(limit=limit)
    locked = len(jobs)
    done = 0
    rate_limited = 0

    for i, job in enumerate(jobs):
        try:
            await process_job(job)
            done += 1
        except RateLimitError:
            rate_limited = len(jobs) - i
            await _requeue_remaining_jobs(jobs[i+1:])
            break

    return {"locked": locked, "done": done, "rate_limited_requeued": rate_limited}


TOKEN_REFRESH_MARGIN_MINUTES = 30
TOKEN_CHECK_INTERVAL_SECONDS = 1800

async def maybe_refresh_tokens():
    """Renova tokens proativamente. Faz refresh se expirado, expirando em breve (<30min), ou updated_at > 3h."""
    try:
        from app.tiny_oauth import get_tokens_from_db, ensure_access_token
        for account in ("A", "B"):
            tokens = await get_tokens_from_db(account)
            if not tokens or not tokens.get("refresh_token"):
                continue
            expires_at = tokens.get("expires_at")
            updated_at = tokens.get("updated_at")
            now = datetime.now(timezone.utc)
            needs_refresh = False
            reason = ""
            if expires_at:
                if hasattr(expires_at, 'tzinfo') and expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                if expires_at <= now:
                    needs_refresh = True
                    reason = "expired"
                elif (expires_at - now).total_seconds() < TOKEN_REFRESH_MARGIN_MINUTES * 60:
                    needs_refresh = True
                    remaining_min = int((expires_at - now).total_seconds() / 60)
                    reason = f"expiring soon ({remaining_min}min left)"
            if not needs_refresh and updated_at:
                if hasattr(updated_at, 'tzinfo') and updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                age_hours = (now - updated_at).total_seconds() / 3600
                if age_hours > 3:
                    needs_refresh = True
                    reason = f"stale (updated {age_hours:.1f}h ago)"
            if needs_refresh:
                logger.info(f"Token {account}: {reason}, refreshing...")
                result = await ensure_access_token(account)
                if result:
                    logger.info(f"Token {account} refreshed successfully")
                else:
                    logger.warning(f"Token {account} refresh failed")
    except Exception as e:
        logger.error(f"Token refresh check failed: {e}")


_last_token_check = None

async def worker_loop():
    global worker_running, _last_token_check
    worker_running = True
    await refresh_products_map()
    logger.info("Worker started")
    
    _last_token_check = datetime.now(timezone.utc)
    await maybe_refresh_tokens()
    
    while worker_running:
        # Heartbeat: sinaliza ao /health que o worker está vivo
        try:
            await update_worker_heartbeat()
        except Exception:
            pass  # heartbeat nunca deve derrubar o loop

        import time as _time
        max_cooldown = 0
        for acct, until in _rate_limit_cooldown_until.items():
            remaining = until - _time.time()
            if remaining > 0:
                max_cooldown = max(max_cooldown, remaining)
        if max_cooldown > 0:
            logger.info(f"Worker pausing {max_cooldown:.0f}s for rate limit cooldown")
            await asyncio.sleep(min(max_cooldown, 30))
        else:
            try:
                processed = await run_worker_once(limit=10)
                if processed > 0:
                    logger.info(f"Worker processed {processed} jobs")
            except Exception as e:
                logger.error(f"Worker error: {e}")
        
        now = datetime.now(timezone.utc)
        if _last_token_check is None or (now - _last_token_check).total_seconds() > TOKEN_CHECK_INTERVAL_SECONDS:
            _last_token_check = now
            await maybe_refresh_tokens()
        
        await asyncio.sleep(5)
    
    logger.info("Worker stopped")


def stop_worker():
    global worker_running
    worker_running = False
