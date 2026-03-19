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
    upsert_orders_map_with_b,
    get_order_a_snapshot,
    get_snapshot_fetched_at,
    get_order_mapping_by_a,
    get_order_mapping_by_b,
    get_venda_b_by_nota_fiscal,
    insert_job,
    reset_stale_locks,
    count_orders_replicated_to_b,
    load_products_map,
    get_feature_flag,
    update_orders_map_sync
)
from app.settings import (
    TINY_A_TOKEN, TINY_B_TOKEN, 
    ENABLE_FETCH_A, EXECUTE_TINY_B, 
    ALLOW_VENDA_IDS, FETCH_CACHE_MINUTES,
    MAX_ORDERS_TO_REPLICATE
)
from app.tiny_client import TinyClient, TinyApiError
from app.tiny_oauth import ensure_access_token, force_refresh_token

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

WORKER_BUILD = "2026-03-19-001"

async def call_tiny(account: str, client: TinyClient, method: str, *args, **kwargs):
    """Call a TinyClient method with automatic 401 retry (force-refresh + retry once)."""
    try:
        return await getattr(client, method)(*args, **kwargs)
    except TinyApiError as e:
        if e.status_code != 401:
            raise
        logger.warning(f"Got 401 on {method} for account {account}, force-refreshing token...")
        new_token = await force_refresh_token(account)
        if not new_token:
            raise
        new_client = TinyClient(new_token)
        return await getattr(new_client, method)(*args, **kwargs)


PRODUTO_ID_MAP: dict[int, int] = {}
_products_map_loaded = False

async def refresh_products_map():
    """Recarrega o mapeamento de produtos do banco de dados."""
    global PRODUTO_ID_MAP, _products_map_loaded
    try:
        PRODUTO_ID_MAP = await load_products_map()
        _products_map_loaded = True
        logger.info(f"Products map reloaded: {len(PRODUTO_ID_MAP)} mappings")
    except Exception as e:
        logger.error(f"Failed to load products map: {e}")

SKU_PRICE = {
    "Rosto-5": 24.45,
    "Te": 21.20,
    "Pescoco": 9.65,
    "Rosto-1t": 12.25,
    "Rosto-2o": 12.25,
    "Rosto-5too": 24.45,
}

SKU_ALIAS = {
    "Rosto-5": "Rosto-5too",
}

DROPSHIPPING_DEPOSIT_ID = 0  # TODO: ID do depósito de dropshipping em A para V365

DEST1_FE_SEDEX_ID = 0         # TODO: ID forma envio Sedex em V365
DEST1_FE_FM_ID = 0            # TODO: ID forma envio FM em V365
DEST1_FE_PAC_ID = 0           # TODO: ID forma envio PAC em V365
DEST1_FE_ME_ID = 0
DEST1_PRICE_LIST_ID = 0       # TODO: ID da lista de preço em V365

DEST1_FF_FM_STANDARD_ID = 0   # TODO: ID forma frete FM Standard em V365
DEST1_FF_SEDEX_ID = 0         # TODO: ID forma frete Sedex em V365
DEST1_FF_PAC_ID = 0           # TODO: ID forma frete PAC em V365

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


def map_sku(codigo: str | None) -> str | None:
    if not codigo:
        return None
    return SKU_ALIAS.get(codigo, codigo)


def price_for(codigo: str | None, fallback: float) -> float:
    if codigo and codigo in SKU_PRICE:
        return float(SKU_PRICE[codigo])
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


async def build_itens_dest_v3(itens_src: list, retry_on_miss: bool = True) -> list:
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
        codigo_destino = map_sku(sku)
        valor_final = price_for(codigo_destino, valor_unitario)
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
    
    logger.info(f"build_itens_dest_v3: mapeados={len(out)} de {len(itens_src)}")
    return out


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
        if job_type == 'create_order_b':
            if not await get_feature_flag("replicate_orders"):
                action_preview = {"would": "create_order_in_B", "skipped": True, "reason": "replicate_orders flag disabled"}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: replicate_orders flag disabled")
                return

            if not venda_id:
                await update_job_failed(job_id, "missing_venda_id", attempts)
                logger.warning(f"Job {job_id} failed: missing_venda_id")
                return
            
            max_orders = int(os.getenv("MAX_ORDERS_TO_REPLICATE", "0"))
            if max_orders > 0:
                current_count = await count_orders_replicated_to_b()
                if current_count >= max_orders:
                    action_preview = {
                        "would": "create_order_in_B",
                        "skipped": True,
                        "reason": f"MAX_ORDERS_TO_REPLICATE limit reached ({current_count}/{max_orders})",
                        "venda_a_id": venda_id
                    }
                    await update_job_done(job_id, action_preview)
                    logger.info(f"Job {job_id} skipped: MAX_ORDERS_TO_REPLICATE limit reached ({current_count}/{max_orders})")
                    return
            
            external_key = f"A:{venda_id}"
            snapshot = await get_order_a_snapshot(str(venda_id))
            fetched_payload = snapshot.get('fetched_payload') if snapshot else None
            
            if fetched_payload and isinstance(fetched_payload, str):
                fetched_payload = json.loads(fetched_payload)
            
            order_data = fetched_payload or {}

            if not order_data:
                await update_job_failed(job_id, "fetched_payload missing, cannot verify deposit", attempts)
                logger.warning(f"Job {job_id} failed: no fetched_payload for venda {venda_id}")
                return

            deposito = order_data.get('deposito') or {}
            deposito_id = deposito.get('id')
            deposito_nome = deposito.get('nome', '')
            if deposito_id != DROPSHIPPING_DEPOSIT_ID:
                action_preview = {
                    "would": "create_order_in_B",
                    "skipped": True,
                    "reason": "deposit_not_allowed",
                    "venda_a_id": venda_id,
                    "deposito_id": deposito_id,
                    "deposito_nome": deposito_nome,
                    "expected_deposit_id": DROPSHIPPING_DEPOSIT_ID,
                    "note": f"Depósito '{deposito_nome}' não é Dropshipping (V365)"
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
            
            if not EXECUTE_TINY_B:
                await upsert_orders_map(external_key=external_key, venda_a_id=str(venda_id))
                action_preview = {
                    "would": "create_order_in_B",
                    "situacao_target": "em_aberto",
                    "venda_a_id": venda_id,
                    "external_key": external_key,
                    "dry_run": True,
                    "has_fetched_payload": fetched_payload is not None,
                    "cliente_nome": cliente.get('nome'),
                    "cpf_cnpj": cpf_cnpj,
                    "itens_count": len(itens_a),
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
            
            token_b = await ensure_access_token("B")
            if not token_b:
                await update_job_failed(job_id, "No valid OAuth token for account B", attempts)
                logger.warning(f"Job {job_id} failed: No OAuth token for B")
                return
            
            client_b = TinyClient(token_b)
            
            id_contato_b = None
            contact_created = False
            try:
                contacts = await call_tiny("B", client_b, "search_contacts", cpf_cnpj)
                if contacts:
                    id_contato_b = contacts[0].get('id')
                    logger.info(f"Found existing contact in B: {id_contato_b}")
            except TinyApiError as e:
                logger.warning(f"Error searching contacts: {e}")
            
            if not id_contato_b:
                contact_payload = {
                    "nome": cliente.get('nome'),
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
                    contact_result = await call_tiny("B", client_b, "create_contact", contact_payload)
                    id_contato_b = contact_result.get('id')
                    contact_created = True
                    logger.info(f"Created contact in B: {id_contato_b}")
                except TinyApiError as e:
                    await update_job_failed(job_id, f"Failed to create contact: {e.status_code} {e.body}", attempts)
                    logger.error(f"Job {job_id} failed to create contact: {e}")
                    return
            
            itens_b = await build_itens_dest_v3(itens_a)
            
            if not itens_b:
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
            
            order_payload_b = {
                "data": order_data.get('data'),
                "idContato": id_contato_b,
                "numeroOrdemCompra": str(order_data.get('numeroPedido') or ""),
                "itens": itens_b,
                "enderecoEntrega": endereco_entrega,
                "listaPreco": {"id": DEST1_PRICE_LIST_ID},
                "vendedor": {"id": 0},  # TODO: ID do vendedor em V365
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
                    "formaPagamento": {"id": 0, "nome": "Conta V365"},  # TODO: ID forma pagamento em V365
                    "formaRecebimento": {"id": 0, "nome": "Conta V365"},  # TODO: ID forma recebimento em V365
                    "meioPagamento": None,
                    "condicaoPagamento": "0",
                    "parcelas": [
                        {
                            "dias": 0,
                            "observacoes": "API REJUDERME",
                            "formaPagamento": {"id": 0, "nome": "Conta V365"},  # TODO: ID forma pagamento em V365
                            "formaRecebimento": {"id": 0, "nome": "Conta V365"},  # TODO: ID forma recebimento em V365
                            "meioPagamento": None
                        }
                    ]
                }
            }
            if numero_pedido_ecommerce:
                order_payload_b["ecommerce"] = {"id": 0, "numeroPedidoEcommerce": numero_pedido_ecommerce}
            order_payload_b = {k: v for k, v in order_payload_b.items() if v is not None}
            
            logger.info(f"create_order_b payload for venda {venda_id}: {json.dumps(order_payload_b, default=str)}")
            result = await call_tiny("B", client_b, "create_order", order_payload_b)
            venda_b_id = str(result.get('id') or result.get('numeroPedido') or '')
            
            await upsert_orders_map_with_b(external_key=external_key, venda_a_id=str(venda_id), venda_b_id=venda_b_id)
            
            tag_added = False
            tag_job_created = False
            try:
                tag_added = await call_tiny("B", client_b, "add_order_tags", venda_b_id, ["API Rejuderme"])
            except Exception as e:
                logger.warning(f"Job {job_id}: failed to add tag to order {venda_b_id}: {e}")
            if not tag_added:
                tag_dedupe = f"B:tag:{venda_b_id}:add_tag_b"
                tag_payload = {"venda_b_id": venda_b_id, "tag": "API Rejuderme"}
                tag_job_created = await insert_job(job_type="add_tag_b", dedupe_key=tag_dedupe, event_id=None, payload=tag_payload, delay_minutes=1)
                if tag_job_created:
                    logger.info(f"Job {job_id}: tag failed, created add_tag_b job for order {venda_b_id}")
            
            action_preview = {
                "would": "create_order_in_B",
                "situacao_target": "em_aberto",
                "venda_a_id": venda_id,
                "venda_b_id": venda_b_id,
                "external_key": external_key,
                "id_contato_b": id_contato_b,
                "contact_created": contact_created,
                "itens_mapped": len(itens_b),
                "forma_envio_origem": forma_envio_src,
                "forma_frete_origem": forma_frete_src,
                "volumes": volumes_src,
                "created": True,
                "tag_added": tag_added,
                "tag_job_created": tag_job_created if not tag_added else None
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: created order in B with id {venda_b_id} (envio={forma_envio_src}, frete={forma_frete_src}, tag={tag_added})")
        
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
            flag_key = f"sync_status_{codigo_situacao}" if codigo_situacao else None
            if flag_key and not await get_feature_flag(flag_key):
                action_preview = {"would": "sync_status", "skipped": True, "reason": f"{flag_key} flag disabled", "situacao": codigo_situacao}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} skipped: {flag_key} flag disabled")
                return

            SITUACAO_CODE = {
                "faturado": 1,
                "cancelado": 2,
                "enviado": 5,
                "entregue": 6,
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
                target_id = str(mapping["venda_b_id"])
                target_source = "B"
                target_token = await ensure_access_token("B")
            elif source == "B":
                mapping = await get_order_mapping_by_b(str(venda_id))
                if not mapping:
                    # TODO: futuramente, chamar GET /pedidos/{venda_id} no Tiny B
                    # para verificar se vendedor == Rejuderme (ID do vendedor em V365).
                    # Por ora, marca como not_mapped (pedido próprio da V365).
                    action_preview = {
                        "would": "sync_status",
                        "skipped": True,
                        "reason": "not_mapped",
                        "note": f"venda_b_id={venda_id} não existe na orders_map (pedido não replicado)",
                    }
                    await update_job_skipped_not_mapped(job_id, action_preview)
                    logger.info(f"Job {job_id} skipped_not_mapped: venda_b_id={venda_id}")
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

            venda_b_id_nf = await get_venda_b_by_nota_fiscal(id_nota_fiscal)

            if not venda_b_id_nf:
                action_preview = {"would": "sync_nf_link", "skipped": True, "reason": "no_vendas_event_for_nf", "id_nota_fiscal": id_nota_fiscal}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id}: no B/vendas event with id_nota_fiscal={id_nota_fiscal}, skipping")
                return

            mapping = await get_order_mapping_by_b(venda_b_id_nf)
            if not mapping:
                action_preview = {"would": "sync_nf_link", "skipped": True, "reason": "no_orders_map", "venda_b_id": venda_b_id_nf, "id_nota_fiscal": id_nota_fiscal}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id}: no orders_map for venda_b {venda_b_id_nf}, skipping")
                return

            venda_a_id = mapping.get("venda_a_id")
            if not venda_a_id:
                await update_job_failed(job_id, f"orders_map for B:{venda_b_id_nf} has no venda_a_id", attempts)
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
                "venda_b_id": venda_b_id_nf,
                "venda_a_id": venda_a_id,
                "nf_numero": nf_numero,
                "nf_serie": nf_serie,
                "nf_chave_acesso": nf_chave_acesso[:20] + "..." if len(nf_chave_acesso) > 20 else nf_chave_acesso,
                "nf_source": nf_source,
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: sync_nf_link NF {nf_numero} for A:{venda_a_id} (from B:{venda_b_id_nf}, source={nf_source})")
        
        elif job_type == 'add_tag_b':
            TAG_BACKOFF_MINUTES = [1, 3, 5]
            TAG_MAX_RETRIES = len(TAG_BACKOFF_MINUTES)

            tag_venda_b_id = payload.get('venda_b_id')
            tag_text = payload.get('tag', 'API Rejuderme')

            if not tag_venda_b_id:
                await update_job_failed(job_id, "missing venda_b_id", attempts)
                return

            token_b = await ensure_access_token("B")
            if not token_b:
                if attempts <= TAG_MAX_RETRIES:
                    delay = TAG_BACKOFF_MINUTES[attempts - 1]
                    await reschedule_job_with_backoff(job_id, attempts, delay, "No valid OAuth token for B")
                    logger.info(f"Job {job_id} add_tag_b rescheduled (attempt {attempts}, retry in {delay}min)")
                else:
                    await update_job_failed(job_id, "No valid OAuth token for B after retries", attempts)
                return

            client_b = TinyClient(token_b)
            tag_ok = False
            tag_error = ""
            try:
                tag_ok = await call_tiny("B", client_b, "add_order_tags", tag_venda_b_id, [tag_text])
            except Exception as e:
                tag_error = str(e)
                logger.warning(f"Job {job_id} add_tag_b failed: {e}")

            if tag_ok:
                action_preview = {"would": "add_tag_b", "venda_b_id": tag_venda_b_id, "tag": tag_text, "tag_added": True, "attempts": attempts}
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} completed: add_tag_b for order {tag_venda_b_id} (attempt {attempts})")
            elif attempts <= TAG_MAX_RETRIES:
                delay = TAG_BACKOFF_MINUTES[attempts - 1]
                await reschedule_job_with_backoff(job_id, attempts, delay, tag_error or "tag request failed")
                logger.info(f"Job {job_id} add_tag_b rescheduled (attempt {attempts}/{TAG_MAX_RETRIES}, retry in {delay}min)")
            else:
                await update_job_failed(job_id, f"add_tag_b failed after {attempts} attempts: {tag_error}", attempts)
                logger.warning(f"Job {job_id} add_tag_b gave up after {attempts} attempts for order {tag_venda_b_id}")

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
        try:
            processed = await run_worker_once(limit=25)
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
