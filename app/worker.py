import asyncio
import json
import logging
import os
import sys
from urllib.parse import quote
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.db import (
    MAX_ATTEMPTS,
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
    upsert_cancelled_order_review,
    get_pool,
    job_exists,
    get_contact_mapping,
    save_contact_mapping,
    remove_contact_mapping,
)
from app.contact_sync import (
    build_contact_payload,
    choose_exact_contact,
    contact_fingerprint,
    extract_source_order_date,
    normalize_tax_id,
    numeric_id,
    source_is_older,
)
from app.settings import (
    ENABLE_FETCH_A, EXECUTE_TINY_C, 
    ALLOW_VENDA_IDS, FETCH_CACHE_MINUTES,
)
from app.tiny_client import TinyClient, TinyApiError
from app.tiny_oauth import ensure_access_token, force_refresh_token
from app.utils import normalize_status

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

WORKER_BUILD = "2026-07-16-contact-map-001"
APP_TZ = ZoneInfo("America/Sao_Paulo")
REJUDERME_TRACKING_PAGE = os.getenv(
    "REJUDERME_TRACKING_PAGE",
    "https://rejuderme.com.br/pages/rastreio",
).rstrip("?")


def rejuderme_tracking_url(codigo_rastreio: str | None) -> str:
    code = str(codigo_rastreio or "").strip()
    return f"{REJUDERME_TRACKING_PAGE}?code={quote(code, safe='')}" if code else REJUDERME_TRACKING_PAGE

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


async def refresh_order_a_snapshot(client_a: TinyClient, venda_a_id: str, account: str = "A") -> dict | None:
    try:
        details = await call_tiny(account, client_a, "get_order_details", str(venda_a_id))
        await upsert_orders_a_fetched(str(venda_a_id), details)
        return details
    except TinyApiError as exc:
        await upsert_orders_a_fetch_error(str(venda_a_id), exc.status_code, exc.body)
        logger.warning(f"Failed to refresh A snapshot for order {venda_a_id}: {exc.body[:160]}")
    except RateLimitError as exc:
        await upsert_orders_a_fetch_error(str(venda_a_id), exc.status_code, str(exc))
        logger.warning(f"Skipped A snapshot refresh for order {venda_a_id}: rate limit")
    except Exception as exc:
        await upsert_orders_a_fetch_error(str(venda_a_id), None, str(exc))
        logger.warning(f"Failed to refresh A snapshot for order {venda_a_id}: {exc}")
    return None


def _json_lookup(data, *keys):
    if not isinstance(data, dict):
        return None
    candidates = [data]
    if isinstance(data.get("dados"), dict):
        candidates.append(data["dados"])
    if isinstance(data.get("notaFiscal"), dict):
        candidates.append(data["notaFiscal"])
    if isinstance(data.get("nota_fiscal"), dict):
        candidates.append(data["nota_fiscal"])
    for candidate in candidates:
        for key in keys:
            value = candidate.get(key)
            if value not in (None, ""):
                return value
    return None


async def sync_nf_observations_from_c(client_a: TinyClient, client_c: TinyClient, venda_a_id: str, venda_c_id: str, c_details: dict | None = None, id_nota_fiscal: str | None = None) -> dict:
    c_details = c_details or await refresh_order_c_snapshot(client_c, venda_c_id) or {}
    nf_id = id_nota_fiscal or _json_lookup(c_details, "idNotaFiscal", "id_nota_fiscal", "idNotaFiscalTiny", "id_nota_fiscal_tiny")

    nf_payload = {}
    if nf_id:
        try:
            nf_payload = await call_tiny("B", client_c, "get_nota_fiscal", str(nf_id))
        except Exception as exc:
            logger.warning(f"Could not fetch NF {nf_id} from C:{venda_c_id}: {exc}")

    nf_numero = str(_json_lookup(nf_payload, "numero") or _json_lookup(c_details, "numeroNotaFiscal", "numero_nota_fiscal", "nfNumero") or "")
    nf_serie = str(_json_lookup(nf_payload, "serie") or _json_lookup(c_details, "serieNotaFiscal", "serie_nota_fiscal") or "")
    nf_chave_acesso = _json_lookup(nf_payload, "chaveAcesso", "chave_acesso", "chave") or _json_lookup(c_details, "chaveAcesso", "chave_acesso")
    nf_protocolo = _json_lookup(nf_payload, "protocolo", "protocoloAutorizacao", "protocolo_autorizacao") or ""
    nf_data_autorizacao = _json_lookup(nf_payload, "dataAutorizacao", "data_autorizacao") or ""

    if not nf_numero and not nf_chave_acesso:
        return {"updated": False, "reason": "nf_data_not_found", "id_nota_fiscal": str(nf_id or "")}

    order_a = await call_tiny("A", client_a, "get_order_details", venda_a_id)
    obs_atual = order_a.get("observacoes") or ""
    nf_block_lines = [f"NF {nf_numero} - {nf_serie} | CHAVE DE ACESSO", nf_chave_acesso or ""]
    if nf_protocolo or nf_data_autorizacao:
        nf_block_lines.append("PROTOCOLO DE AUTORIZACAO DE USO")
        nf_block_lines.append(f"{nf_protocolo} - {nf_data_autorizacao}".strip(" -"))
    nf_block = "\n".join([line for line in nf_block_lines if line is not None])

    if nf_chave_acesso and nf_chave_acesso in obs_atual:
        await upsert_orders_a_fetched(str(venda_a_id), order_a)
        return {"updated": False, "reason": "nf_already_in_obs", "nf_numero": nf_numero}

    nova_obs = obs_atual.rstrip() + "\n\n" + nf_block if obs_atual.strip() else nf_block
    await call_tiny("A", client_a, "update_order", venda_a_id, {"observacoes": nova_obs})
    refreshed_a = await call_tiny("A", client_a, "get_order_details", venda_a_id)
    await upsert_orders_a_fetched(str(venda_a_id), refreshed_a)
    return {
        "updated": True,
        "id_nota_fiscal": str(nf_id or ""),
        "nf_numero": nf_numero,
        "nf_serie": nf_serie,
        "nf_chave_acesso": (nf_chave_acesso[:20] + "...") if nf_chave_acesso and len(nf_chave_acesso) > 20 else nf_chave_acesso,
    }


async def sync_tracking_from_c_to_a(client_a: TinyClient, client_c: TinyClient, venda_a_id: str, venda_c_id: str, c_details: dict | None = None) -> dict:
    c_details = c_details or await refresh_order_c_snapshot(client_c, venda_c_id) or {}
    transportador_c = (c_details.get("transportador") or {}) if isinstance(c_details, dict) else {}
    codigo_rastreio = (transportador_c.get("codigoRastreamento") or "").strip()
    url_rastreio_c = (transportador_c.get("urlRastreamento") or "").strip()
    url_rastreio = rejuderme_tracking_url(codigo_rastreio) if codigo_rastreio else url_rastreio_c

    if not codigo_rastreio and not url_rastreio:
        return {
            "updated": False,
            "reason": "tracking_not_found",
            "venda_c_id": str(venda_c_id),
            "venda_a_id": str(venda_a_id),
        }

    try:
        await call_tiny("A", client_a, "update_order_despacho", str(venda_a_id), codigo_rastreio, url_rastreio)
    except TinyApiError as exc:
        if exc.status_code == 400 and "expedi" in (exc.body or "").lower():
            return {
                "updated": False,
                "reason": "a_has_expedicao",
                "venda_c_id": str(venda_c_id),
                "venda_a_id": str(venda_a_id),
                "error_body": exc.body[:200],
            }
        raise

    return {
        "updated": True,
        "venda_c_id": str(venda_c_id),
        "venda_a_id": str(venda_a_id),
        "codigo_rastreamento": codigo_rastreio or None,
        "url_rastreamento": url_rastreio or None,
    }


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


def _parse_dt(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _payload_modified_at(payload: dict):
    if not isinstance(payload, dict):
        return None
    for key in (
        "dataAlteracao",
        "dataAtualizacao",
        "ultimaAlteracao",
        "updatedAt",
        "updated_at",
        "alteradoEm",
        "modificadoEm",
    ):
        parsed = _parse_dt(payload.get(key))
        if parsed:
            return parsed
    return None


async def _get_snapshot_times(venda_a_id: str, venda_c_id: str) -> tuple[datetime | None, datetime | None]:
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                CASE
                    WHEN oas.fetched_at IS NULL THEN oas.updated_at
                    WHEN oas.updated_at IS NULL THEN oas.fetched_at
                    ELSE GREATEST(oas.fetched_at, oas.updated_at)
                END AS a_seen_at,
                CASE
                    WHEN ocs.fetched_at IS NULL THEN ocs.updated_at
                    WHEN ocs.updated_at IS NULL THEN ocs.fetched_at
                    ELSE GREATEST(ocs.fetched_at, ocs.updated_at)
                END AS c_seen_at
            FROM public.orders_map om
            LEFT JOIN public.orders_a_snapshot oas ON oas.venda_a_id = om.venda_a_id::text
            LEFT JOIN public.orders_c_snapshot ocs ON ocs.venda_c_id = om.venda_c_id::text
            WHERE om.venda_a_id::text = $1 AND om.venda_c_id::text = $2
            LIMIT 1
        """, str(venda_a_id), str(venda_c_id))
    if not row:
        return None, None
    return row["a_seen_at"], row["c_seen_at"]


def _clean_dict(value: dict) -> dict:
    return {k: v for k, v in value.items() if v not in (None, "")}


def _first_present(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _source_address(payload: dict) -> dict:
    cliente = payload.get("cliente") if isinstance(payload.get("cliente"), dict) else {}
    address = payload.get("enderecoEntrega") or payload.get("endereco") or cliente.get("endereco") or {}
    return address if isinstance(address, dict) else {}


async def _resolve_contact_for_order(client_c: TinyClient, order_data: dict, venda_a_id: str) -> dict:
    """Resolve the C contact with a local map and only call Tiny when data changed."""
    cliente = order_data.get("cliente") if isinstance(order_data.get("cliente"), dict) else {}
    endereco = _source_address(order_data)
    contact_payload = build_contact_payload(cliente, endereco)
    cpf_normalized = normalize_tax_id(contact_payload.get("cpfCnpj"))
    if not cpf_normalized:
        raise RuntimeError("contact_missing_cpf_cnpj")

    contato_a_id = numeric_id(cliente.get("id"))
    source_order_a_id = numeric_id(venda_a_id)
    source_order_date = extract_source_order_date(order_data)
    fingerprint = contact_fingerprint(contact_payload)
    mapping = await get_contact_mapping(cpf_normalized, contato_a_id)
    recovered_mapping = False

    if mapping:
        contato_c_id = numeric_id(mapping.get("contato_c_id"))
        if contato_c_id is None:
            await remove_contact_mapping(int(mapping["id"]))
            mapping = None
            recovered_mapping = True
        elif source_is_older(mapping, source_order_date, source_order_a_id):
            logger.info(
                f"Contact C:{contato_c_id} reused for A order {venda_a_id}; "
                "older order cannot overwrite newer contact data"
            )
            return {
                "id": str(contato_c_id),
                "created": False,
                "updated": False,
                "resolution": "mapped_older_order_reused",
                "lookup_skipped": True,
                "update_skipped": True,
                "duplicate_matches": 0,
            }
        elif mapping.get("source_fingerprint") == fingerprint:
            await save_contact_mapping(
                mapping_id=int(mapping["id"]),
                cpf_cnpj_normalized=cpf_normalized,
                contato_a_id=contato_a_id,
                contato_c_id=contato_c_id,
                source_fingerprint=fingerprint,
                source_order_a_id=source_order_a_id,
                source_order_date=source_order_date,
                confirmed=False,
            )
            logger.info(f"Contact C:{contato_c_id} reused without Tiny contact calls for A order {venda_a_id}")
            return {
                "id": str(contato_c_id),
                "created": False,
                "updated": False,
                "resolution": "mapped_unchanged",
                "lookup_skipped": True,
                "update_skipped": True,
                "duplicate_matches": 0,
            }
        else:
            try:
                await call_tiny("B", client_c, "update_contact", str(contato_c_id), contact_payload)
            except TinyApiError as exc:
                if exc.status_code != 404:
                    raise
                await remove_contact_mapping(int(mapping["id"]))
                mapping = None
                recovered_mapping = True
                logger.warning(f"Contact mapping pointed to missing C:{contato_c_id}; rebuilding mapping")
            else:
                await save_contact_mapping(
                    mapping_id=int(mapping["id"]),
                    cpf_cnpj_normalized=cpf_normalized,
                    contato_a_id=contato_a_id,
                    contato_c_id=contato_c_id,
                    source_fingerprint=fingerprint,
                    source_order_a_id=source_order_a_id,
                    source_order_date=source_order_date,
                    confirmed=True,
                )
                logger.info(f"Contact C:{contato_c_id} updated because source data changed")
                return {
                    "id": str(contato_c_id),
                    "created": False,
                    "updated": True,
                    "resolution": "mapped_updated",
                    "lookup_skipped": True,
                    "update_skipped": False,
                    "duplicate_matches": 0,
                }

    contacts = await call_tiny("B", client_c, "search_contacts", contact_payload["cpfCnpj"])
    matched_contact, duplicate_matches = choose_exact_contact(contacts, cpf_normalized)
    if duplicate_matches > 1:
        duplicate_ids = [
            numeric_id(contact.get("id"))
            for contact in contacts
            if normalize_tax_id(contact.get("cpfCnpj") or contact.get("cpf_cnpj")) == cpf_normalized
        ]
        logger.warning(f"Multiple contacts in C share the same CPF/CNPJ; using oldest id from {duplicate_ids}")

    contact_created = False
    contact_updated = False
    update_skipped = False
    if matched_contact:
        contato_c_id = numeric_id(matched_contact.get("id"))
        remote_payload = build_contact_payload(matched_contact, matched_contact.get("endereco") or {})
        if contact_fingerprint(remote_payload) == fingerprint:
            update_skipped = True
            resolution = "searched_unchanged"
        else:
            await call_tiny("B", client_c, "update_contact", str(contato_c_id), contact_payload)
            contact_updated = True
            resolution = "searched_updated"
    else:
        created_contact = await call_tiny("B", client_c, "create_contact", contact_payload)
        contato_c_id = numeric_id(created_contact.get("id"))
        contact_created = True
        resolution = "searched_created"

    if contato_c_id is None:
        raise RuntimeError("contact_c_id_missing_after_resolution")

    await save_contact_mapping(
        mapping_id=None,
        cpf_cnpj_normalized=cpf_normalized,
        contato_a_id=contato_a_id,
        contato_c_id=contato_c_id,
        source_fingerprint=fingerprint,
        source_order_a_id=source_order_a_id,
        source_order_date=source_order_date,
        confirmed=True,
    )
    logger.info(f"Contact C:{contato_c_id} resolved via Tiny lookup for A order {venda_a_id}")
    return {
        "id": str(contato_c_id),
        "created": contact_created,
        "updated": contact_updated,
        "resolution": f"recovered_{resolution}" if recovered_mapping else resolution,
        "lookup_skipped": False,
        "update_skipped": update_skipped,
        "duplicate_matches": duplicate_matches,
    }


def _source_transportador(payload: dict) -> dict:
    transportador = payload.get("transportador") if isinstance(payload.get("transportador"), dict) else {}
    return transportador if isinstance(transportador, dict) else {}


def _source_ecommerce_number(payload: dict) -> str | None:
    ecommerce = payload.get("ecommerce") if isinstance(payload.get("ecommerce"), dict) else {}
    return _first_present(
        ecommerce.get("numeroPedidoEcommerce"),
        ecommerce.get("numeroPedido"),
        ecommerce.get("pedido"),
        payload.get("numeroPedidoEcommerce"),
        payload.get("numeroOrdemCompra"),
    )


def _build_field_update_payload(source_payload: dict, field_keys: list[str], target_account: str) -> tuple[dict, list[str], list[str]]:
    update_payload: dict = {}
    applied: list[str] = []
    unsupported: list[str] = []
    cliente = source_payload.get("cliente") if isinstance(source_payload.get("cliente"), dict) else {}

    cliente_payload = {}
    if "nome" in field_keys:
        cliente_payload["nome"] = cliente.get("nome")
    if "cpf" in field_keys:
        cliente_payload["cpfCnpj"] = cliente.get("cpfCnpj") or cliente.get("cpf_cnpj")
    cliente_payload = _clean_dict(cliente_payload)
    if cliente_payload:
        update_payload["cliente"] = cliente_payload
        applied.extend([key for key in ("nome", "cpf") if key in field_keys])

    address_keys = {"cep_entrega", "cidade_entrega", "uf_entrega", "numero_endereco", "complemento_endereco"}
    if address_keys.intersection(field_keys):
        address = _source_address(source_payload)
        update_payload["enderecoEntrega"] = _clean_dict({
            "endereco": address.get("endereco") or address.get("logradouro"),
            "enderecoNro": address.get("enderecoNro") or address.get("numero"),
            "numero": address.get("enderecoNro") or address.get("numero"),
            "complemento": address.get("complemento"),
            "bairro": address.get("bairro"),
            "municipio": address.get("municipio") or address.get("cidade"),
            "cep": address.get("cep"),
            "uf": address.get("uf"),
        })
        applied.extend([key for key in field_keys if key in address_keys])

    if "numero_ecommerce" in field_keys:
        ecommerce_number = _source_ecommerce_number(source_payload)
        if target_account == "B" and ecommerce_number:
            update_payload["numeroOrdemCompra"] = str(ecommerce_number)
            applied.append("numero_ecommerce")
        else:
            unsupported.append("numero_ecommerce")

    if {"forma_envio", "forma_frete"}.intersection(field_keys):
        if target_account == "B":
            transportador = _source_transportador(source_payload)
            forma_envio = transportador.get("formaEnvio") or {}
            forma_frete = transportador.get("formaFrete") or {}
            forma_envio_nome = forma_envio.get("nome") if isinstance(forma_envio, dict) else forma_envio
            forma_frete_nome = forma_frete.get("nome") if isinstance(forma_frete, dict) else forma_frete
            update_payload["transportador"] = build_transportador_v3(
                forma_envio_origem=forma_envio_nome,
                forma_frete_origem=forma_frete_nome,
                codigo_rastreio=transportador.get("codigoRastreamento"),
                url_rastreio=transportador.get("urlRastreamento"),
                volumes=transportador.get("volumes") or 1,
            )
            applied.extend([key for key in ("forma_envio", "forma_frete") if key in field_keys])
        else:
            unsupported.extend([key for key in ("forma_envio", "forma_frete") if key in field_keys])

    if "itens" in field_keys:
        unsupported.append("itens")

    return update_payload, applied, unsupported


def _extract_list_items(data: dict) -> list[dict]:
    if not isinstance(data, dict):
        return []
    items = data.get("itens")
    return items if isinstance(items, list) else []


def _order_has_marker(item: dict, marker: str) -> bool:
    marker_norm = str(marker or "").strip().lower()
    values: list[str] = []
    for key in ("marcadores", "tags"):
        raw = item.get(key)
        if isinstance(raw, list):
            for entry in raw:
                if isinstance(entry, dict):
                    values.extend(str(entry.get(k, "")) for k in ("descricao", "nome", "tag") if entry.get(k))
                elif entry:
                    values.append(str(entry))
        elif raw:
            values.append(str(raw))
    return any(marker_norm == value.strip().lower() for value in values)


def _order_date_from_item(item: dict) -> datetime | None:
    for key in ("data", "dataPedido", "dataCriacao", "createdAt"):
        raw = item.get(key)
        if not raw:
            continue
        text = str(raw).strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text[:19], fmt)
                return parsed.replace(tzinfo=APP_TZ)
            except ValueError:
                pass
    return None


def _approval_target_for_local(local_base: datetime) -> datetime:
    target_date = local_base.date()
    business_days = 0
    while business_days < 2:
        target_date = target_date + timedelta(days=1)
        if target_date.weekday() != 6:
            business_days += 1
    return datetime(target_date.year, target_date.month, target_date.day, 1, 0, tzinfo=APP_TZ)


async def run_sweep_origin_exports_job(job_id: str, attempts: int, payload: dict) -> None:
    days = int(payload.get("days") or 7)
    today = datetime.now(APP_TZ).date()
    data_inicial = (today - timedelta(days=days)).isoformat()
    data_final = today.isoformat()
    token_a = await ensure_access_token("A")
    if not token_a:
        raise RuntimeError("No valid OAuth token for A")

    client_a = TinyClient(token_a)
    p = await get_pool()
    queued_create = 0
    queued_approve = 0
    skipped_mapped = 0
    skipped_tagged = 0
    scanned = 0
    now_local = datetime.now(APP_TZ)

    async with p.acquire() as conn:
        for status_name, status_code in (("em_aberto", 0), ("aprovado", 3)):
            offset = 0
            while True:
                data = await call_tiny("A", client_a, "list_orders", data_inicial, data_final, 100, offset, status_code)
                items = _extract_list_items(data)
                if not items:
                    break
                ids = [str(item.get("id")) for item in items if isinstance(item, dict) and item.get("id")]
                mapped_rows = await conn.fetch("""
                    SELECT venda_a_id::text AS venda_a_id
                    FROM public.orders_map
                    WHERE venda_a_id::text = ANY($1::text[])
                """, ids) if ids else []
                mapped_ids = {str(row["venda_a_id"]) for row in mapped_rows}

                for item in items:
                    if not isinstance(item, dict) or not item.get("id"):
                        continue
                    scanned += 1
                    venda_a_id = str(item.get("id"))
                    if venda_a_id in mapped_ids:
                        skipped_mapped += 1
                        continue
                    if _order_has_marker(item, "v365"):
                        skipped_tagged += 1
                        continue

                    webhook_payload = dict(item)
                    webhook_payload["__source"] = "daily_sweep_origin_exports"
                    webhook_payload["__synced_at"] = datetime.now(timezone.utc).isoformat()
                    await upsert_orders_a_snapshot(venda_a_id, webhook_payload)

                    if status_name == "aprovado":
                        created = await insert_job(
                            job_type="create_order_c",
                            dedupe_key=f"A:vendas:{venda_a_id}:create_order_c",
                            event_id=None,
                            payload={"source": "A", "topic": "vendas", "venda_id": venda_a_id, "codigo_situacao": "aprovado", "__source": "daily_sweep_origin_exports"},
                        )
                        queued_create += 1 if created else 0
                    else:
                        order_dt = _order_date_from_item(item)
                        if order_dt and _approval_target_for_local(order_dt.astimezone(APP_TZ)) <= now_local:
                            created = await insert_job(
                                job_type="approve_order_a",
                                dedupe_key=f"A:vendas:{venda_a_id}:approve_order_a",
                                event_id=None,
                                payload={"source": "A", "topic": "vendas", "venda_id": venda_a_id, "codigo_situacao": "em_aberto", "__source": "daily_sweep_origin_exports"},
                            )
                            queued_approve += 1 if created else 0

                offset += len(items)
                if len(items) < 100:
                    break

    await update_job_done(job_id, {
        "would": "sweep_origin_exports",
        "done": True,
        "days": days,
        "data_inicial": data_inicial,
        "data_final": data_final,
        "scanned": scanned,
        "skipped_mapped": skipped_mapped,
        "skipped_tagged_v365": skipped_tagged,
        "queued_create_order_c": queued_create,
        "queued_approve_order_a": queued_approve,
    })


async def run_reconcile_recent_statuses_job(job_id: str, attempts: int, payload: dict) -> None:
    days = int(payload.get("days") or 5)
    today = datetime.now(APP_TZ).date()
    data_inicial = (today - timedelta(days=days)).isoformat()
    data_final = today.isoformat()
    status_codes = {"cancelado": 2, "enviado": 5, "entregue": 6, "nao_entregue": 9}
    tokens = {"A": await ensure_access_token("A"), "B": await ensure_access_token("B")}
    if not tokens["A"] or not tokens["B"]:
        raise RuntimeError("Missing Tiny token for A or B")

    clients = {"A": TinyClient(tokens["A"]), "B": TinyClient(tokens["B"])}
    remote_statuses: dict[str, dict[str, str]] = {"A": {}, "B": {}}
    queued = 0
    reviews = 0
    scanned = 0
    confirmed_aligned = 0
    unmapped = 0

    for account in ("A", "B"):
        for status_name, status_code in status_codes.items():
            offset = 0
            while True:
                data = await call_tiny(account, clients[account], "list_orders", data_inicial, data_final, 100, offset, status_code)
                items = _extract_list_items(data)
                if not items:
                    break
                for item in items:
                    if not isinstance(item, dict) or not item.get("id"):
                        continue
                    scanned += 1
                    remote_statuses[account][str(item.get("id"))] = status_name
                offset += len(items)
                if len(items) < 100:
                    break

    a_ids = list(remote_statuses["A"].keys())
    c_ids = list(remote_statuses["B"].keys())
    p = await get_pool()
    async with p.acquire() as conn:
        mappings = await conn.fetch("""
            SELECT venda_a_id::text AS venda_a_id,
                   venda_c_id::text AS venda_c_id
            FROM public.orders_map
            WHERE venda_c_id IS NOT NULL
              AND (
                  venda_a_id::text = ANY($1::text[])
                  OR venda_c_id::text = ANY($2::text[])
              )
        """, a_ids, c_ids)

    mapped_a_ids = {str(row["venda_a_id"]) for row in mappings}
    mapped_c_ids = {str(row["venda_c_id"]) for row in mappings}
    unmapped = len(set(a_ids) - mapped_a_ids) + len(set(c_ids) - mapped_c_ids)

    for mapping in mappings:
        venda_a_id = str(mapping["venda_a_id"])
        venda_c_id = str(mapping["venda_c_id"])
        status_a = remote_statuses["A"].get(venda_a_id)
        status_c = remote_statuses["B"].get(venda_c_id)

        # Cancelamento manual em A nunca altera C. Se C também estiver cancelado,
        # a situação já está alinhada e não caracteriza pendência para revisão.
        if status_a == "cancelado":
            if status_c != "cancelado":
                await upsert_cancelled_order_review(venda_a_id)
                reviews += 1
            if status_c == "cancelado":
                await update_orders_map_sync(venda_a_id, venda_c_id, "cancelado")
                confirmed_aligned += 1
            continue

        # Para estes estados, C é a origem da verdade e somente C pode atualizar A.
        if status_c in status_codes:
            if status_a == status_c:
                await update_orders_map_sync(venda_a_id, venda_c_id, status_c)
                confirmed_aligned += 1
                continue

            created = await insert_job(
                job_type="sync_status",
                dedupe_key=f"system:reconcile:{today.isoformat()}:B:{venda_c_id}:sync_status:{status_c}",
                event_id=None,
                payload={
                    "source": "B",
                    "topic": "vendas",
                    "venda_id": venda_c_id,
                    "codigo_situacao": status_c,
                    "__source": "daily_reconcile_statuses",
                },
            )
            queued += 1 if created else 0

    await update_job_done(job_id, {
        "would": "reconcile_recent_statuses",
        "done": True,
        "days": days,
        "data_inicial": data_inicial,
        "data_final": data_final,
        "scanned": scanned,
        "queued_sync_status": queued,
        "cancel_reviews_added": reviews,
        "confirmed_aligned": confirmed_aligned,
        "unmapped_relevant_orders": unmapped,
    })


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
        if job_type == 'sweep_origin_exports':
            await run_sweep_origin_exports_job(job_id, attempts, payload)
            return

        if job_type == 'reconcile_recent_statuses':
            await run_reconcile_recent_statuses_job(job_id, attempts, payload)
            return

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
            if payload.get("origin"):
                create_order_payload["origin"] = payload.get("origin")
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
            flag_key = "replicate_orders"
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
                    "has_fetched_payload": bool(order_data),
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

            status_a_normalized = normalize_status(order_data.get('situacao'))
            if status_a_normalized != "aprovado":
                action_preview = {
                    "would": "create_order_in_C",
                    "skipped": True,
                    "reason": "status_not_approved",
                    "venda_a_id": venda_id,
                    "current_status": status_a_normalized,
                }
                await update_job_done(job_id, action_preview)
                logger.info(
                    f"Job {job_id} skipped: A:{venda_id} is {status_a_normalized or 'unknown'}, not aprovado"
                )
                return

            # Resolve product mappings before consuming C API calls on contacts.
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

            token_c = await ensure_access_token("B")
            if not token_c:
                await update_job_failed(job_id, "No valid OAuth token for account B", attempts)
                logger.warning(f"Job {job_id} failed: No OAuth token for B")
                return

            client_c = TinyClient(token_c)

            nome_raw = cliente.get('nome') or ''
            if len(nome_raw) > 50:
                logger.warning(f"Job {job_id}: contact name truncated from {len(nome_raw)} to 50 characters")

            contact_resolution = await _resolve_contact_for_order(client_c, order_data, str(venda_id))
            id_contato_c = contact_resolution["id"]
            contact_created = contact_resolution["created"]
            contact_updated = contact_resolution["updated"]
            
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
                "situacao": 3 if status_a_normalized == "aprovado" else None,
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

            force_status_c = payload.get('force_status_c')
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
                "contact_resolution": contact_resolution["resolution"],
                "contact_lookup_skipped": contact_resolution["lookup_skipped"],
                "contact_update_skipped": contact_resolution["update_skipped"],
                "contact_duplicate_matches": contact_resolution["duplicate_matches"],
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
            
            refresh_only = bool(payload.get('refresh_only'))
            force_refresh = bool(payload.get('force_refresh'))
            webhook_payload_raw = payload.get('webhook_payload')
            if webhook_payload_raw:
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
            
            fetched_at = None if force_refresh else await get_snapshot_fetched_at(str(venda_id))
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
                    if refresh_only:
                        return
                    create_order_dedupe_key = f"A:vendas:{venda_id}:create_order_c"
                    create_order_payload = {
                        "source": "A", "topic": "vendas", "venda_id": str(venda_id),
                        "codigo_situacao": codigo_situacao, "id_nota_fiscal": id_nota_fiscal,
                        "from_fetch_order_a": True
                    }
                    if payload.get('force_status_c'):
                        create_order_payload["force_status_c"] = payload["force_status_c"]
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

            if refresh_only:
                return
            
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
            await insert_job(job_type="create_order_c", dedupe_key=create_order_dedupe_key, event_id=None, payload=create_order_payload)
            logger.info(f"Chained create_order_c job for venda {venda_id}")
        
        elif job_type == 'sync_status':
            if source == "A" and codigo_situacao == "cancelado":
                await upsert_cancelled_order_review(str(venda_id))
                action_preview = {
                    "would": "cancel_review",
                    "skipped": True,
                    "reason": "origin_cancellation_requires_review",
                    "venda_a_id": str(venda_id),
                }
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} did not cancel C; A:{venda_id} sent to cancellation review")
                return

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
            nf_sync_result = None
            tracking_sync_result = None
            if target_source == "B":
                await refresh_order_c_snapshot(client_target, target_id)
            elif source == "B":
                source_token = await ensure_access_token("B")
                if source_token:
                    client_source = TinyClient(source_token)
                    c_details = await refresh_order_c_snapshot(client_source, str(venda_id))
                    if codigo_situacao == "enviado":
                        nf_sync_result = await sync_nf_observations_from_c(
                            client_a=client_target,
                            client_c=client_source,
                            venda_a_id=target_id,
                            venda_c_id=str(venda_id),
                            c_details=c_details,
                            id_nota_fiscal=id_nota_fiscal,
                        )
                        tracking_sync_result = await sync_tracking_from_c_to_a(
                            client_a=client_target,
                            client_c=client_source,
                            venda_a_id=target_id,
                            venda_c_id=str(venda_id),
                            c_details=c_details,
                        )
                    await refresh_order_a_snapshot(client_target, target_id)
            
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
                "nf_sync": nf_sync_result,
                "tracking_sync": tracking_sync_result,
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
            url_rastreio_c = (transportador_c.get("urlRastreamento") or "").strip()
            url_rastreio = rejuderme_tracking_url(codigo_rastreio) if codigo_rastreio else url_rastreio_c

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

        elif job_type == 'sync_order_fields':
            venda_a_id_str = str(payload.get("venda_a_id") or "")
            venda_c_id_str = str(payload.get("venda_c_id") or "")
            field_keys = [str(key) for key in (payload.get("field_keys") or []) if key]
            if not venda_a_id_str or not venda_c_id_str or not field_keys:
                await update_job_failed(job_id, "missing venda_a_id, venda_c_id or field_keys", attempts)
                return

            a_seen_at, c_seen_at = await _get_snapshot_times(venda_a_id_str, venda_c_id_str)
            min_dt = datetime.min.replace(tzinfo=timezone.utc)
            source_account = "A" if (a_seen_at or min_dt) >= (c_seen_at or min_dt) else "B"
            target_account = "B" if source_account == "A" else "A"

            token_a = await ensure_access_token("A")
            token_c = await ensure_access_token("B")
            if not token_a or not token_c:
                await update_job_failed(job_id, "No valid OAuth token for A or B", attempts)
                return

            client_a = TinyClient(token_a)
            client_c = TinyClient(token_c)
            order_a = await call_tiny("A", client_a, "get_order_details", venda_a_id_str)
            await upsert_orders_a_fetched(venda_a_id=venda_a_id_str, fetched_payload=order_a)
            order_c = await refresh_order_c_snapshot(client_c, venda_c_id_str) or {}

            modified_a = _payload_modified_at(order_a) or a_seen_at
            modified_c = _payload_modified_at(order_c) or c_seen_at
            if modified_c and modified_a and modified_c > modified_a:
                source_account = "B"
                target_account = "A"
            elif modified_a and modified_c and modified_a >= modified_c:
                source_account = "A"
                target_account = "B"

            source_payload = order_a if source_account == "A" else order_c
            target_client = client_c if target_account == "B" else client_a
            target_id = venda_c_id_str if target_account == "B" else venda_a_id_str
            applied_fields = []
            unsupported_fields = []

            status_code_by_name = {
                "faturado": 1,
                "cancelado": 2,
                "aprovado": 3,
                "preparando_envio": 4,
                "enviado": 5,
                "entregue": 6,
                "pronto_envio": 7,
                "dados_incompletos": 8,
                "nao_entregue": 9,
            }

            if "situacao" in field_keys:
                status_name = normalize_status(source_payload.get("situacao"))
                status_code = status_code_by_name.get(status_name or "")
                if source_account == "A" and target_account == "B" and status_name == "cancelado":
                    unsupported_fields.append("situacao")
                elif status_code:
                    await call_tiny(target_account, target_client, "update_order_status", target_id, status_code)
                    applied_fields.append("situacao")
                    await update_orders_map_sync(venda_a_id_str, venda_c_id_str, status_name)
                else:
                    unsupported_fields.append("situacao")

            if "codigo_rastreamento" in field_keys:
                transportador = _source_transportador(source_payload)
                codigo_rastreamento = (transportador.get("codigoRastreamento") or source_payload.get("codigoRastreamento") or "").strip()
                url_rastreamento = (transportador.get("urlRastreamento") or source_payload.get("urlRastreamento") or "").strip()
                if codigo_rastreamento:
                    if target_account == "A":
                        url_rastreamento = rejuderme_tracking_url(codigo_rastreamento)
                    await call_tiny(target_account, target_client, "update_order_despacho", target_id, codigo_rastreamento, url_rastreamento)
                    applied_fields.append("codigo_rastreamento")
                else:
                    unsupported_fields.append("codigo_rastreamento")

            update_payload, partial_applied, partial_unsupported = _build_field_update_payload(source_payload, field_keys, target_account)
            if update_payload:
                await call_tiny(target_account, target_client, "update_order", target_id, update_payload)
                applied_fields.extend(partial_applied)
            unsupported_fields.extend(partial_unsupported)

            if target_account == "A":
                refreshed_a = await call_tiny("A", client_a, "get_order_details", venda_a_id_str)
                await upsert_orders_a_fetched(venda_a_id=venda_a_id_str, fetched_payload=refreshed_a)
            else:
                await refresh_order_c_snapshot(client_c, venda_c_id_str)

            applied_fields = sorted(set(applied_fields))
            unsupported_fields = sorted(set(unsupported_fields) - set(applied_fields))
            action_preview = {
                "would": "sync_order_fields",
                "done": bool(applied_fields),
                "venda_a_id": venda_a_id_str,
                "venda_c_id": venda_c_id_str,
                "source_account": source_account,
                "target_account": target_account,
                "requested_fields": field_keys,
                "applied_fields": applied_fields,
                "unsupported_fields": unsupported_fields,
                "a_seen_at": a_seen_at.isoformat() if a_seen_at else None,
                "c_seen_at": c_seen_at.isoformat() if c_seen_at else None,
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} sync_order_fields {source_account}->{target_account} A:{venda_a_id_str} C:{venda_c_id_str} fields={applied_fields} unsupported={unsupported_fields}")

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
        if job_type in {"sweep_origin_exports", "reconcile_recent_statuses"} and attempts < MAX_ATTEMPTS:
            delay_minutes = min(60, 5 * (2 ** max(0, attempts - 1)))
            await reschedule_job_with_backoff(job_id, attempts, delay_minutes, error_msg)
            logger.info(
                f"System job {job_id} scheduled for retry {attempts + 1}/{MAX_ATTEMPTS} "
                f"in {delay_minutes}min"
            )
        else:
            await update_job_failed(job_id, error_msg, attempts)


async def _requeue_remaining_jobs(jobs: list[dict], delay_minutes: int = 2):
    """Requeue jobs that weren't processed due to rate limiting."""
    for job in jobs:
        job_id = job['id']
        attempts = job.get('attempts') or 0
        await reschedule_job_with_backoff(job_id, attempts, delay_minutes, "rate_limit_batch_abort")
    if jobs:
        logger.info(f"Rate limit: requeued {len(jobs)} remaining jobs with {delay_minutes}min delay")


async def run_worker_once(limit: int = 25, reset_locks: bool = False) -> int:
    if reset_locks:
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


async def run_worker_once_detailed(limit: int = 50, reset_locks: bool = True) -> dict:
    if reset_locks:
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
PROACTIVE_TOKEN_REFRESH_ENABLED = os.getenv("PROACTIVE_TOKEN_REFRESH_ENABLED", "0").lower() in ("1", "true", "yes")
WORKER_POLL_SECONDS = int(os.getenv("WORKER_POLL_SECONDS", "30"))
WORKER_BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "3"))
WORKER_HEARTBEAT_SECONDS = int(os.getenv("WORKER_HEARTBEAT_SECONDS", "120"))
WORKER_STALE_LOCK_RESET_SECONDS = int(os.getenv("WORKER_STALE_LOCK_RESET_SECONDS", "300"))

async def maybe_refresh_tokens():
    """Renova tokens proativamente. Faz refresh se expirado, expirando em breve (<30min), ou updated_at > 3h."""
    if not PROACTIVE_TOKEN_REFRESH_ENABLED:
        return
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
_last_heartbeat = None
_last_stale_lock_reset = None
_last_origin_sweep_date = None
_last_status_reconcile_date = None


async def maybe_schedule_daily_system_jobs() -> None:
    global _last_origin_sweep_date, _last_status_reconcile_date
    local_now = datetime.now(APP_TZ)
    today_key = local_now.date().isoformat()

    if local_now.weekday() <= 5 and local_now.hour >= 6 and _last_origin_sweep_date != today_key:
        dedupe_key = f"system:sweep_origin_exports:{today_key}"
        created = await insert_job(
            job_type="sweep_origin_exports",
            dedupe_key=dedupe_key,
            event_id=None,
            payload={"source": "system", "topic": "daily_sweep", "days": 7},
        )
        if created or await job_exists(dedupe_key):
            _last_origin_sweep_date = today_key
        if created:
            logger.info(f"Scheduled daily origin export sweep for {today_key}")

    if local_now.hour >= 23 and _last_status_reconcile_date != today_key:
        dedupe_key = f"system:reconcile_recent_statuses:{today_key}"
        created = await insert_job(
            job_type="reconcile_recent_statuses",
            dedupe_key=dedupe_key,
            event_id=None,
            payload={"source": "system", "topic": "daily_reconcile", "days": 5},
        )
        if created or await job_exists(dedupe_key):
            _last_status_reconcile_date = today_key
        if created:
            logger.info(f"Scheduled daily recent status reconcile for {today_key}")

async def worker_loop():
    global worker_running, _last_token_check, _last_heartbeat, _last_stale_lock_reset
    worker_running = True
    await refresh_products_map()
    logger.info("Worker started")

    _last_token_check = datetime.now(timezone.utc)
    _last_heartbeat = None
    _last_stale_lock_reset = None
    await maybe_refresh_tokens()

    while worker_running:
        now = datetime.now(timezone.utc)
        if _last_heartbeat is None or (now - _last_heartbeat).total_seconds() >= WORKER_HEARTBEAT_SECONDS:
            try:
                await update_worker_heartbeat()
                _last_heartbeat = now
            except Exception:
                pass

        reset_locks = False
        if _last_stale_lock_reset is None or (now - _last_stale_lock_reset).total_seconds() >= WORKER_STALE_LOCK_RESET_SECONDS:
            reset_locks = True
            _last_stale_lock_reset = now

        try:
            await maybe_schedule_daily_system_jobs()
        except Exception as e:
            logger.error(f"Daily system job scheduler failed: {e}")

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
                processed = await run_worker_once(limit=WORKER_BATCH_SIZE, reset_locks=reset_locks)
                if processed > 0:
                    logger.info(f"Worker processed {processed} jobs")
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(max(WORKER_POLL_SECONDS, 60))

        now = datetime.now(timezone.utc)
        if _last_token_check is None or (now - _last_token_check).total_seconds() > TOKEN_CHECK_INTERVAL_SECONDS:
            _last_token_check = now
            await maybe_refresh_tokens()

        await asyncio.sleep(WORKER_POLL_SECONDS)

    logger.info("Worker stopped")

def stop_worker():
    global worker_running
    worker_running = False
