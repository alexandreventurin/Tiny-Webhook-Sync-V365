import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from app.db import (
    fetch_and_lock_jobs,
    update_job_done,
    update_job_failed,
    upsert_orders_map,
    upsert_orders_a_snapshot,
    upsert_orders_a_fetched,
    upsert_orders_a_fetch_error,
    upsert_orders_map_with_b,
    get_order_a_snapshot,
    get_snapshot_fetched_at,
    insert_job,
    reset_stale_locks,
    count_orders_replicated_to_b,
    load_products_map
)
from app.settings import (
    TINY_A_TOKEN, TINY_B_TOKEN, 
    ENABLE_FETCH_A, EXECUTE_TINY_B, 
    ALLOW_VENDA_IDS, FETCH_CACHE_MINUTES,
    MAX_ORDERS_TO_REPLICATE
)
from app.tiny_client import TinyClient, TinyApiError
from app.tiny_oauth import ensure_access_token

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

WORKER_BUILD = "2026-02-04-005"

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

DEST1_FE_SEDEX_ID = 846978945
DEST1_FE_FM_ID = 895824123
DEST1_FE_PAC_ID = 971399662
DEST1_FE_ME_ID = 0
DEST1_PRICE_LIST_ID = 915701964

FORMA_ENVIO_MAP = {
    "FM Transportes": {
        "formaEnvioId": DEST1_FE_FM_ID,
        "fretePorConta": "R",
        "formaFreteMap": {
            "Standard": "Standard",
            "FMSTD": "Standard",
            "EXPRESSO": "EXPRESSO",
            "FMEXP": "EXPRESSO",
        },
        "defaultFormaFrete": "Standard",
    },
    "Correios (Sedex)": {
        "formaEnvioId": DEST1_FE_SEDEX_ID,
        "fretePorConta": "R",
        "formaFreteMap": {
            "SEDEX CONTRATO AG (03220)": "SEDEX CONTRATO AG (03220)",
            "03220": "SEDEX CONTRATO AG (03220)",
            "SEDEX 12 CONTRATO AG (03140)": "SEDEX 12 CONTRATO AG (03140)",
            "03140": "SEDEX 12 CONTRATO AG (03140)",
            "SEDEX 10 CONTRATO AG (03158)": "SEDEX 10 CONTRATO AG (03158)",
            "03158": "SEDEX 10 CONTRATO AG (03158)",
            "SEDEX HOJE CONTRATO AG (03204)": "SEDEX HOJE CONTRATO AG (03204)",
            "03204": "SEDEX HOJE CONTRATO AG (03204)",
        },
        "defaultFormaFrete": "SEDEX CONTRATO AG (03220)",
    },
    "Correios (PAC)": {
        "formaEnvioId": DEST1_FE_PAC_ID,
        "fretePorConta": "R",
        "formaFreteMap": {
            "PAC CONTRATO AG (03298)": "PAC CONTRATO AG (03298)",
            "03298": "PAC CONTRATO AG (03298)",
            "CORREIOS MINI ENVIOS CTR AG (04227)": "CORREIOS MINI ENVIOS CTR AG (04227)",
            "04227": "CORREIOS MINI ENVIOS CTR AG (04227)",
        },
        "defaultFormaFrete": "PAC CONTRATO AG (03298)",
    },
    "Mercado Envios": {
        "formaEnvioId": DEST1_FE_ME_ID if DEST1_FE_ME_ID else None,
        "fretePorConta": "R",
        "formaFreteMap": {
            "PAC": "PAC",
            "21": "PAC",
            "Sedex": "Sedex",
            "22": "Sedex",
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


def map_forma_frete(forma_envio_nome: str | None, forma_frete_origem: str | None) -> str | None:
    """Mapeia a forma de frete de A para B."""
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
        "volumes": volumes,
    }
    
    forma_envio_id = config.get("formaEnvioId")
    if forma_envio_id:
        forma_envio_obj = {"id": forma_envio_id}
        if forma_frete_dest:
            forma_envio_obj["formaFrete"] = forma_frete_dest
        transportador["formaEnvio"] = forma_envio_obj
    
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
            if not venda_id:
                await update_job_failed(job_id, "missing_venda_id", attempts)
                logger.warning(f"Job {job_id} failed: missing_venda_id")
                return
            
            if MAX_ORDERS_TO_REPLICATE > 0:
                current_count = await count_orders_replicated_to_b()
                if current_count >= MAX_ORDERS_TO_REPLICATE:
                    action_preview = {
                        "would": "create_order_in_B",
                        "skipped": True,
                        "reason": f"MAX_ORDERS_TO_REPLICATE limit reached ({current_count}/{MAX_ORDERS_TO_REPLICATE})",
                        "venda_a_id": venda_id
                    }
                    await update_job_done(job_id, action_preview)
                    logger.info(f"Job {job_id} skipped: MAX_ORDERS_TO_REPLICATE limit reached ({current_count}/{MAX_ORDERS_TO_REPLICATE})")
                    return
            
            external_key = f"A:{venda_id}"
            snapshot = await get_order_a_snapshot(str(venda_id))
            fetched_payload = snapshot.get('fetched_payload') if snapshot else None
            
            if fetched_payload and isinstance(fetched_payload, str):
                fetched_payload = json.loads(fetched_payload)
            
            order_data = fetched_payload or {}
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
                    "situacao_target": 8,
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
                contacts = await client_b.search_contacts(cpf_cnpj)
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
                    contact_result = await client_b.create_contact(contact_payload)
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
                "situacao": 8,
                "numeroOrdemCompra": str(order_data.get('numeroPedido') or ""),
                "itens": itens_b,
                "enderecoEntrega": endereco_entrega,
                "listaPreco": {"id": DEST1_PRICE_LIST_ID},
                "vendedor": {"id": 963241122},
                "transportador": build_transportador_v3(
                    forma_envio_origem=forma_envio_src,
                    forma_frete_origem=forma_frete_src,
                    codigo_rastreio=codigo_rastreio,
                    url_rastreio=url_rastreio,
                    volumes=volumes_src
                ),
                "observacoes": f"Repasse Tiny - origem id {order_data.get('id')} nº {order_data.get('numeroPedido')} {obs_extra}",
                "valorFrete": float(str(order_data.get('valorFrete') or 0).replace(',', '.')),
                "valorDesconto": float(str(order_data.get('valorDesconto') or 0).replace(',', '.'))
            }
            if numero_pedido_ecommerce:
                order_payload_b["ecommerce"] = {"id": 0, "numeroPedidoEcommerce": numero_pedido_ecommerce}
            order_payload_b = {k: v for k, v in order_payload_b.items() if v is not None}
            
            result = await client_b.create_order(order_payload_b)
            venda_b_id = str(result.get('id') or result.get('numeroPedido') or '')
            
            await upsert_orders_map_with_b(external_key=external_key, venda_a_id=str(venda_id), venda_b_id=venda_b_id)
            
            action_preview = {
                "would": "create_order_in_B",
                "situacao_target": 8,
                "venda_a_id": venda_id,
                "venda_b_id": venda_b_id,
                "external_key": external_key,
                "id_contato_b": id_contato_b,
                "contact_created": contact_created,
                "itens_mapped": len(itens_b),
                "forma_envio_origem": forma_envio_src,
                "forma_frete_origem": forma_frete_src,
                "volumes": volumes_src,
                "created": True
            }
            await update_job_done(job_id, action_preview)
            logger.info(f"Job {job_id} completed: created order in B with id {venda_b_id} (envio={forma_envio_src}, frete={forma_frete_src})")
        
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
                action_preview = {
                    "would": "fetch_order_a",
                    "venda_a_id": venda_id,
                    "skipped": True,
                    "reason": "No valid OAuth token for account A"
                }
                await update_job_done(job_id, action_preview)
                logger.warning(f"Job {job_id} skipped: No OAuth token for A")
                create_order_dedupe_key = f"A:vendas:{venda_id}:create_order_b"
                create_order_payload = {
                    "source": "A", "topic": "vendas", "venda_id": str(venda_id),
                    "codigo_situacao": codigo_situacao, "id_nota_fiscal": id_nota_fiscal,
                    "from_fetch_order_a": True
                }
                await insert_job(job_type="create_order_b", dedupe_key=create_order_dedupe_key, event_id=None, payload=create_order_payload)
                return
            
            client_a = TinyClient(token_a)
            try:
                fetched_data = await client_a.get_order_details(str(venda_id))
                await upsert_orders_a_fetched(venda_a_id=str(venda_id), fetched_payload=fetched_data)
                
                action_preview = {
                    "would": "fetch_order_a",
                    "venda_a_id": venda_id,
                    "fetched": True,
                    "note": "fetched from Tiny A"
                }
                await update_job_done(job_id, action_preview)
                logger.info(f"Job {job_id} completed: fetch_order_a for venda {venda_id}")
            except TinyApiError as e:
                await upsert_orders_a_fetch_error(venda_a_id=str(venda_id), status_code=e.status_code, error_body=e.body)
                logger.error(f"Job {job_id} fetch_order_a failed: {e.status_code} {e.body[:100]}")
                raise
            
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
    await refresh_products_map()
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
