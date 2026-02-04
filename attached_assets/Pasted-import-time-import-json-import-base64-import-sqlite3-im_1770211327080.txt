import time
import json
import base64
import sqlite3
import urllib.parse
import asyncio
from typing import Optional, Tuple, Dict, List
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from decouple import config

app = FastAPI()

TINY_V3_BASE = "https://api.tiny.com.br/public-api/v3"
OAUTH_BASE = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect"

ORIG_CLIENT_ID = config("ORIG_CLIENT_ID")
ORIG_CLIENT_SECRET = config("ORIG_CLIENT_SECRET")
ORIG_REDIRECT_URI = config("ORIG_REDIRECT_URI")

DEST1_CLIENT_ID = config("DEST1_CLIENT_ID")
DEST1_CLIENT_SECRET = config("DEST1_CLIENT_SECRET")
DEST1_REDIRECT_URI = config("DEST1_REDIRECT_URI")

DEST1_PRICE_LIST_ID = int(config("DEST1_PRICE_LIST_ID", default="915701964"))

DEST1_FE_SEDEX_ID = int(config("DEST1_FE_SEDEX_ID", default="846978945"))
DEST1_FE_FM_ID = int(config("DEST1_FE_FM_ID", default="895824123"))
DEST1_FE_PAC_ID = int(config("DEST1_FE_PAC_ID", default="971399662"))
DEST1_FE_ME_ID = int(config("DEST1_FE_ME_ID", default="0"))

PROCESSING_DELAY = int(config("PROCESSING_DELAY_SECONDS", default="300"))

PRODUTO_ID_MAP = {
    335959393: 853501914,  
    335959369: 969386704, 
    335959374: 853501837,  
    335959379: 853501882,  
    335959384: 961060387,
}

_VENDEDOR_REJUDERME_ID: Optional[int] = None

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

DB = sqlite3.connect("repasse_v3.db", isolation_level=None, check_same_thread=False)

DB.execute(
    """
CREATE TABLE IF NOT EXISTS oauth_tokens (
  account TEXT PRIMARY KEY,
  access_token TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  expires_at INTEGER NOT NULL
)
"""
)

DB.execute(
    """
CREATE TABLE IF NOT EXISTS processed (
  origin_order_id TEXT PRIMARY KEY,
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
)
"""
)

DB.execute(
    """
CREATE TABLE IF NOT EXISTS contato_cache (
  cpf_cnpj TEXT PRIMARY KEY,
  contato_id INTEGER NOT NULL,
  account TEXT NOT NULL,
  updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
)
"""
)

# ⏱️ Tabela de pedidos pendentes (com delay)
DB.execute(
    """
CREATE TABLE IF NOT EXISTS pending_orders (
  origin_order_id TEXT PRIMARY KEY,
  webhook_data TEXT NOT NULL,
  scheduled_at INTEGER NOT NULL,
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
)
"""
)


def oauth_client(account: str) -> Tuple[str, str, str]:
    if account == "orig":
        return ORIG_CLIENT_ID, ORIG_CLIENT_SECRET, ORIG_REDIRECT_URI
    if account == "dest1":
        return DEST1_CLIENT_ID, DEST1_CLIENT_SECRET, DEST1_REDIRECT_URI
    raise RuntimeError("account_invalida")


def basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}"
    return base64.b64encode(raw.encode()).decode()


def save_tokens(account: str, access: str, refresh: str, expires_in: int) -> None:
    exp = int(time.time()) + int(expires_in) - 30
    DB.execute(
        """
        INSERT OR REPLACE INTO oauth_tokens(account, access_token, refresh_token, expires_at)
        VALUES(?,?,?,?)
        """,
        (account, access, refresh, exp),
    )
    print(f"[TOKENS][{account}] saved exp={exp}")


def get_tokens(account: str) -> Optional[Tuple[str, str, int]]:
    row = DB.execute(
        "SELECT access_token, refresh_token, expires_at FROM oauth_tokens WHERE account=?",
        (account,),
    ).fetchone()
    if not row:
        return None
    return row[0], row[1], int(row[2])


async def ensure_access_token(account: str) -> str:
    print(f"[TOKEN] check {account}")
    rec = get_tokens(account)
    if not rec:
        print(f"[TOKEN] {account} nao_autorizado")
        raise HTTPException(status_code=428, detail=f"conta_{account}_nao_autorizada")

    access, refresh, exp = rec
    now = int(time.time())
    
    print(f"[TOKEN][DEBUG] {account} - now={now}, exp={exp}, diff={exp-now}s")

    if now < exp and access:
        print(f"[TOKEN] {account} valido (expires in {exp-now}s)")
        return access

    if not refresh:
        print(f"[TOKEN] {account} sem refresh_token")
        raise HTTPException(status_code=428, detail=f"conta_{account}_sem_refresh_token")

    print(f"[TOKEN] {account} expirado, renovando...")
    cid, csec, _ = oauth_client(account)
    auth = basic_auth_header(cid, csec)

    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, read=30.0)) as c:
        r = await c.post(
            f"{OAUTH_BASE}/token",
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh,
            },
        )

    print(f"[{account.upper()} REFRESH][STATUS]", r.status_code, r.text[:200])

    if r.status_code != 200:
        raise HTTPException(status_code=428, detail=f"falha_renovar_token_{account}")

    data = r.json()
    new_access = data.get("access_token")
    new_refresh = data.get("refresh_token", refresh)
    expires_in = data.get("expires_in")

    if not new_access or not expires_in:
        raise HTTPException(status_code=428, detail="refresh_response_invalido")

    save_tokens(account, new_access, new_refresh, int(expires_in))
    print(f"[TOKEN] {account} renovado")
    return new_access


def h_v3(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def already_done(order_id: str) -> bool:
    return (
        DB.execute(
            "SELECT 1 FROM processed WHERE origin_order_id=?",
            (order_id,),
        ).fetchone()
        is not None
    )


def mark_done(order_id: str) -> None:
    DB.execute(
        "INSERT OR IGNORE INTO processed(origin_order_id) VALUES(?)",
        (order_id,),
    )


def get_cached_contato(cpf_cnpj: str, account: str) -> Optional[int]:
    row = DB.execute(
        "SELECT contato_id FROM contato_cache WHERE cpf_cnpj=? AND account=?",
        (cpf_cnpj, account),
    ).fetchone()
    if row:
        return int(row[0])
    return None


def cache_contato(cpf_cnpj: str, contato_id: int, account: str) -> None:
    DB.execute(
        """
        INSERT OR REPLACE INTO contato_cache(cpf_cnpj, contato_id, account)
        VALUES(?,?,?)
        """,
        (cpf_cnpj, contato_id, account),
    )


# ⏱️ Funções de delay
def save_pending_order(order_id: str, webhook_data: dict) -> None:
    """Salva pedido para processar com delay"""
    scheduled_at = int(time.time()) + PROCESSING_DELAY
    
    DB.execute(
        """
        INSERT OR REPLACE INTO pending_orders(origin_order_id, webhook_data, scheduled_at)
        VALUES(?,?,?)
        """,
        (order_id, json.dumps(webhook_data), scheduled_at),
    )
    
    scheduled_time = datetime.fromtimestamp(scheduled_at).strftime('%H:%M:%S')
    print(f"[DELAY] Pedido {order_id} agendado para {scheduled_time} ({PROCESSING_DELAY}s)")


def get_ready_orders() -> List[Tuple[str, dict]]:
    """Retorna pedidos prontos para processar"""
    now = int(time.time())
    
    rows = DB.execute(
        "SELECT origin_order_id, webhook_data FROM pending_orders WHERE scheduled_at <= ?",
        (now,),
    ).fetchall()
    
    return [(row[0], json.loads(row[1])) for row in rows]


def remove_pending_order(order_id: str) -> None:
    """Remove pedido da fila de pendentes"""
    DB.execute(
        "DELETE FROM pending_orders WHERE origin_order_id=?",
        (order_id,),
    )


def map_sku(codigo: Optional[str]) -> Optional[str]:
    if not codigo:
        return None
    return SKU_ALIAS.get(codigo, codigo)


def price_for(codigo: Optional[str], fallback: float) -> float:
    if codigo and codigo in SKU_PRICE:
        return float(SKU_PRICE[codigo])
    return float(fallback or 0)


def total_for(pedido_src: dict) -> float:
    for k in ("valorTotalPedido", "totalPedido", "valorTotal"):
        v = pedido_src.get(k)
        if v not in (None, "", 0):
            return float(str(v).replace(",", "."))
    return 0.0


def transport_map_for_dest1(forma_envio_origem: Optional[str]) -> Dict[str, any]:
    if forma_envio_origem == "FM Transportes":
        return {"formaEnvioId": DEST1_FE_FM_ID, "fretePorConta": "R"}
    if forma_envio_origem == "Correios (Sedex)":
        return {"formaEnvioId": DEST1_FE_SEDEX_ID, "fretePorConta": "R"}
    if forma_envio_origem == "Correios (PAC)":
        return {"formaEnvioId": DEST1_FE_PAC_ID, "fretePorConta": "R"}
    if forma_envio_origem == "Mercado Envios" and DEST1_FE_ME_ID:
        return {"formaEnvioId": DEST1_FE_ME_ID, "fretePorConta": "R"}
    return {"formaEnvioId": None, "fretePorConta": "R"}


def extract_tracking_v3_src(pedido_src: dict) -> Tuple[Optional[str], Optional[str]]:
    t = pedido_src.get("transportador") or {}
    codigo = t.get("codigoRastreamento") or ""
    url = t.get("urlRastreamento") or ""
    return (codigo or None, url or None)


def extract_forma_envio_v3(pedido_src: dict) -> Optional[str]:
    """Extrai nome da forma de envio do pedido origem"""
    transp = pedido_src.get("transportador") or {}
    forma = transp.get("formaEnvio") or {}
    return forma.get("nome")


def build_itens_dest_v3(pedido_src: dict) -> List[dict]:
    itens_src = pedido_src.get("itens") or []
    out = []

    for src in itens_src:
        if not isinstance(src, dict):
            continue

        produto = src.get("produto") or {}
        produto_id_origem = produto.get("id")
        
        if not produto_id_origem:
            print(f"[WARN] Item sem produto.id, pulando")
            continue

        produto_id_destino = PRODUTO_ID_MAP.get(produto_id_origem)
        
        if not produto_id_destino:
            sku = produto.get("sku") or "?"
            print(f"[WARN] Produto ID {produto_id_origem} (SKU: {sku}) não mapeado, pulando")
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

    print(f"[BUILD_ITENS_DEST_V3] mapeados={len(out)} de {len(itens_src)}")
    return out


def build_transportador_v3(
    forma_envio_origem: Optional[str],
    codigo: Optional[str],
    url: Optional[str],
) -> dict:
    conf = transport_map_for_dest1(forma_envio_origem)
    
    transportador = {
        "id": 0,
        "fretePorConta": conf.get("fretePorConta", "R"),
        "codigoRastreamento": codigo or "",
        "urlRastreamento": url or "",
    }
    
    if conf.get("formaEnvioId"):
        transportador["formaEnvio"] = {"id": conf["formaEnvioId"]}
    
    return transportador


def build_pagamento_v3(pedido_src: dict, total: float) -> dict:
    data_pedido = pedido_src.get("data")
    
    return {
        "formaPagamento": {"id": 0},
        "meioPagamento": {"id": 0},
        "parcelas": [
            {
                "dias": 0,
                "data": data_pedido,
                "valor": total,
                "observacoes": "Repasse automático"
            }
        ]
    }


def build_despacho_payload_v3(
    forma_envio_origem: Optional[str],
    codigo: Optional[str],
    url: Optional[str],
) -> dict:
    conf = transport_map_for_dest1(forma_envio_origem)
    
    body = {
        "codigoRastreamento": codigo or "",
        "urlRastreamento": url or "",
        "volumes": 1,
        "pesoBruto": 0,
        "pesoLiquido": 0,
    }
    
    if conf.get("formaEnvioId"):
        body["formaEnvio"] = {"id": conf["formaEnvioId"]}
    
    return body


def map_order_payload_v3_dest1(
    pedido_src: dict,
    contato_id: int,
    forma_envio_origem: Optional[str],
    codigo_rastreio: Optional[str],
    url_rastreio: Optional[str],
) -> dict:
    valor_frete = float(str(pedido_src.get("valorFrete") or 0).replace(",", "."))
    valor_desconto = float(str(pedido_src.get("valorDesconto") or 0).replace(",", "."))
    valor_total = total_for(pedido_src)
    
    itens = build_itens_dest_v3(pedido_src)
    if not itens:
        raise ValueError("Nenhum item válido para mapear")
    
    body = {
        "data": pedido_src.get("data"),
        "situacao": 8,
        "numeroOrdemCompra": str(pedido_src.get("numeroPedido") or ""),
        "valorDesconto": valor_desconto,
        "valorFrete": valor_frete,
        "valorOutrasDespesas": 0,
        "observacoes": f"Repasse Tiny - origem id {pedido_src.get('id')} nº {pedido_src.get('numeroPedido')}",
        
        "idContato": contato_id,
        
        "listaPreco": {"id": DEST1_PRICE_LIST_ID},
        
        "ecommerce": {
            "id": 0,
            "numeroPedidoEcommerce": str(pedido_src.get("ecommerce")["numeroPedidoEcommerce"] or "")
        },
        
        "transportador": build_transportador_v3(
            forma_envio_origem,
            codigo_rastreio,
            url_rastreio,
        ),
        
        "itens": itens,
        
        "pagamento": build_pagamento_v3(pedido_src, valor_total),
    }
    
    print("\n[MAP DEST1 POST BODY V3]")
    print(json.dumps(body, indent=2, ensure_ascii=False)[:2000])
    return body


async def get_vendedor_rejuderme_id() -> int:
    global _VENDEDOR_REJUDERME_ID
    
    if _VENDEDOR_REJUDERME_ID:
        return _VENDEDOR_REJUDERME_ID
    
    print("[VENDEDOR] Buscando ID do vendedor 'Rejuderme'...")
    
    token = await ensure_access_token("dest1")
    url = f"{TINY_V3_BASE}/vendedores"
    
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.get(url, headers=h_v3(token), params={"limit": 100})
    
    print(f"[VENDEDOR][STATUS]", r.status_code)
    
    if r.status_code != 200:
        print(f"[VENDEDOR][WARN] Falha ao buscar vendedores, usando ID 0")
        return 0
    
    data = r.json()
    vendedores = data.get("itens") or []
    
    print(f"[VENDEDOR] Encontrados {len(vendedores)} vendedores")
    
    for v in vendedores:
        contato = v.get("contato") or {}
        nome = (contato.get("nome") or "").lower()
        if "rejuderme" in nome or "rejurdeme" in nome:
            vendedor_id = v.get("id")
            print(f"[VENDEDOR] Encontrado '{contato.get('nome')}' com ID={vendedor_id}")
            _VENDEDOR_REJUDERME_ID = vendedor_id
            return vendedor_id
    
    print(f"[VENDEDOR][WARN] 'Rejuderme' não encontrado, usando primeiro vendedor")
    
    if vendedores:
        primeiro = vendedores[0]
        vendedor_id = primeiro.get("id")
        print(f"[VENDEDOR] Usando '{primeiro.get('nome')}' com ID={vendedor_id}")
        _VENDEDOR_REJUDERME_ID = vendedor_id
        return vendedor_id
    
    print(f"[VENDEDOR][WARN] Nenhum vendedor encontrado, usando ID 0")
    return 0


async def tiny_get_order_v3(account: str, order_id: str) -> dict:
    token = await ensure_access_token(account)
    url = f"{TINY_V3_BASE}/pedidos/{order_id}"
    print(f"\n[GET {account.upper()} v3] URL:", url)

    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, read=30.0)) as c:
        r = await c.get(url, headers=h_v3(token))

    print(f"[GET {account.upper()} v3][STATUS]", r.status_code, r.text[:400])

    if r.status_code == 401:
        raise HTTPException(status_code=428, detail=f"token_invalido_{account}")
    
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail=f"pedido_{order_id}_nao_encontrado")

    r.raise_for_status()
    return r.json()


async def ensure_contato_dest1(cliente_data: dict) -> int:
    cpf_cnpj = cliente_data.get("cpfCnpj") or cliente_data.get("cpf_cnpj") or ""
    cpf_cnpj = cpf_cnpj.replace(".", "").replace("-", "").replace("/", "") 
    
    if not cpf_cnpj:
        raise ValueError("CPF/CNPJ obrigatório para buscar/criar contato")
    
    cached_id = get_cached_contato(cpf_cnpj, "dest1")
    if cached_id:
        print(f"[CONTATO] Cache hit: {cached_id}")
        return cached_id
    
    token = await ensure_access_token("dest1")
    search_url = f"{TINY_V3_BASE}/contatos"
    params = {"cpfCnpj": cpf_cnpj}
    
    print(f"[CONTATO] Buscando CPF/CNPJ: {cpf_cnpj}")
    
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.get(search_url, headers=h_v3(token), params=params)
    
    print(f"[CONTATO][BUSCA][STATUS] {r.status_code}")
    
    if r.status_code == 200:
        data = r.json()
        contatos = data.get("itens") or []
        
        print(f"[CONTATO][BUSCA] Encontrados {len(contatos)} contatos")
        
        if contatos and len(contatos) > 0:
            contato_id = contatos[0].get("id")
            print(f"[CONTATO] Encontrado ID={contato_id}")
            cache_contato(cpf_cnpj, contato_id, "dest1")
            return contato_id
    
    print("[CONTATO] Não encontrado, criando novo...")
    
    endereco_data = cliente_data.get("endereco") or {}
    
    payload = {
        "nome": cliente_data.get("nome") or "Cliente Sem Nome",
        "cpfCnpj": cpf_cnpj,  # ✅ Envia sem formatação
        "tipoPessoa": "F" if len(cpf_cnpj) == 11 else "J",
        "email": cliente_data.get("email"),
        "telefone": cliente_data.get("telefone") or cliente_data.get("fone"),
        "endereco": {
            "endereco": endereco_data.get("endereco"),
            "numero": endereco_data.get("numero"),
            "complemento": endereco_data.get("complemento"),
            "bairro": endereco_data.get("bairro"),
            "municipio": endereco_data.get("municipio"),
            "cep": endereco_data.get("cep"),
            "uf": endereco_data.get("uf"),
        }
    }
    
    print("[CONTATO] Payload:", json.dumps(payload, indent=2, ensure_ascii=False)[:500])
    
    token_fresh = await ensure_access_token("dest1")
    
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.post(
            f"{TINY_V3_BASE}/contatos",
            headers=h_v3(token_fresh),
            json=payload
        )
    
    print(f"[CONTATO][CREATE][STATUS]", r.status_code, r.text[:400])
    
    # ✅ Se der 400 com "já existe", buscar novamente
    if r.status_code == 400:
        try:
            data = r.json()
            mensagem = data.get("mensagem", "")
            detalhes = data.get("detalhes", [])
            
            ja_existe = "já existe" in mensagem.lower() or "already exists" in mensagem.lower()
            
            if not ja_existe and detalhes:
                for detalhe in detalhes:
                    msg_detalhe = detalhe.get("mensagem", "")
                    if "já existe" in msg_detalhe.lower() or "already exists" in msg_detalhe.lower():
                        ja_existe = True
                        break
            
            if ja_existe:
                print(f"[CONTATO] Contato já existe (erro 400), buscando novamente...")
                
                async with httpx.AsyncClient(timeout=15.0) as c:
                    r2 = await c.get(search_url, headers=h_v3(token_fresh), params=params)
                
                print(f"[CONTATO][RETRY][STATUS] {r2.status_code}")
                
                if r2.status_code == 200:
                    data2 = r2.json()
                    contatos2 = data2.get("itens") or []
                    
                    print(f"[CONTATO][RETRY] Encontrados {len(contatos2)} contatos")
                    
                    if contatos2 and len(contatos2) > 0:
                        contato_id = contatos2[0].get("id")
                        print(f"[CONTATO] Encontrado após erro: ID={contato_id}")
                        cache_contato(cpf_cnpj, contato_id, "dest1")
                        return contato_id
                    else:
                        print(f"[CONTATO][RETRY][ERRO] Nenhum contato encontrado")
                        raise HTTPException(502, f"contato_existe_mas_nao_encontrado")
                else:
                    print(f"[CONTATO][RETRY][ERRO] Falha na busca: {r2.status_code}")
                    raise HTTPException(502, f"falha_buscar_contato_existente")
        except HTTPException:
            raise
        except Exception as e:
            print(f"[CONTATO][WARN] Erro ao processar 400: {e}")
    
    r.raise_for_status()
    created = r.json()
    contato_id = created.get("id")
    
    if not contato_id:
        raise HTTPException(502, "contato_criado_sem_id")
    
    print(f"[CONTATO] Criado ID={contato_id}")
    cache_contato(cpf_cnpj, contato_id, "dest1")
    return contato_id


async def dest1_create_order_v3(pedido_obj: dict) -> dict:
    token = await ensure_access_token("dest1")
    url = f"{TINY_V3_BASE}/pedidos"

    print("\n[DEST1 POST /pedidos] URL:", url)

    async with httpx.AsyncClient(timeout=httpx.Timeout(25.0, read=60.0)) as c:
        r = await c.post(url, headers=h_v3(token), json=pedido_obj)

    print("[DEST1 POST /pedidos][STATUS]", r.status_code, r.text[:500])

    if r.status_code == 401:
        raise HTTPException(status_code=428, detail="token_invalido_dest1")

    r.raise_for_status()
    return r.json()


async def dest1_update_dispatch_v3(
    pedido_id: str,
    despacho_body: dict,
) -> dict:
    token = await ensure_access_token("dest1")
    url = f"{TINY_V3_BASE}/pedidos/{pedido_id}/despacho"

    print("\n[DEST1 PUT /despacho] URL:", url)

    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, read=30.0)) as c:
        r = await c.put(url, headers=h_v3(token), json=despacho_body)

    print("[DEST1 PUT /despacho][STATUS]", r.status_code)

    if r.status_code == 401:
        raise HTTPException(status_code=428, detail="token_invalido_dest1")

    if r.status_code == 204:
        return {"status": "ok", "message": "Despacho atualizado"}

    r.raise_for_status()
    
    try:
        return r.json()
    except:
        return {"status": "ok"}


async def safe_read_payload(request: Request) -> dict:
    try:
        return await request.json()
    except Exception:
        raw = (await request.body()).decode(errors="ignore")
        if not raw.strip():
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {}


# ============================================
# PROCESSAMENTO DE PEDIDO (core logic)
# ============================================

async def process_order(origin_id: str, payload: dict = None):
    """
    Processa um pedido (chamado após o delay)
    """
    print(f"\n⏱️  [PROCESSING] Iniciando processamento do pedido {origin_id}")
    
    try:
        # 1. Buscar pedido origem
        try:
            orig = await tiny_get_order_v3("orig", origin_id)
        except HTTPException as e:
            if e.status_code == 428:
                print("[ERRO] Conta orig não autorizada")
                return
            raise

        pedido_src = orig

        if not pedido_src or not pedido_src.get("cliente") or not pedido_src.get("itens"):
            print("[ERRO] origem_sem_dados")
            return

        # 2. Extrair dados de rastreio e forma de envio
        codigo, urlrast = extract_tracking_v3_src(pedido_src)
        forma_envio = extract_forma_envio_v3(pedido_src)
        
        print(
            "[ORIG RASTREIO]",
            {"codigo": codigo, "url": urlrast, "forma_envio": forma_envio},
        )

        # 3. Obter ID do contato
        try:
            cliente = pedido_src.get("cliente") or {}
            contato_id = await ensure_contato_dest1(cliente)
        except HTTPException as e:
            if e.status_code == 428:
                print("[ERRO] Conta dest1 não autorizada")
                return
            raise
        except Exception as e:
            print(f"[ERRO] Falha ao buscar/criar contato: {e}")
            return

        # 4. Buscar ID do vendedor
        vendedor_id = await get_vendedor_rejuderme_id()
        
        # 5. Montar payload
        body_dest1 = map_order_payload_v3_dest1(
            pedido_src,
            contato_id,
            forma_envio,
            codigo,
            urlrast,
        )
        
        if vendedor_id and vendedor_id > 0:
            body_dest1["vendedor"] = {"id": vendedor_id}
            print(f"[VENDEDOR] Adicionado ao payload: ID={vendedor_id}")

        # 6. Criar pedido
        try:
            created = await dest1_create_order_v3(body_dest1)
        except HTTPException as e:
            if e.status_code == 428:
                print("[ERRO] dest1 não autorizado")
                return
            raise

        created_id = created.get("id") or created.get("idPedido")
        created_num = created.get("numeroPedido") or created.get("numero")

        print("[DEST1 CREATED]", {"id": created_id, "numero": created_num})

        # 7. Atualizar despacho
        if created_id and (codigo or forma_envio):
            despacho_body = build_despacho_payload_v3(
                forma_envio,
                codigo,
                urlrast,
            )
            try:
                await dest1_update_dispatch_v3(str(created_id), despacho_body)
            except Exception as e:
                print("[WARN] erro no despacho:", str(e))

        # 8. Marcar como processado
        mark_done(origin_id)
        remove_pending_order(origin_id)

        print(f"\n✅ [SUCCESS] Pedido {origin_id} processado!")
        print("   origin:", origin_id)
        print("   dest1:", {"id": created_id, "numero": created_num})
        
    except Exception as e:
        print(f"\n❌ [ERROR] Falha ao processar pedido {origin_id}: {e}")
        import traceback
        traceback.print_exc()


# ⏱️ Background task para processar pedidos pendentes
async def process_pending_orders():
    """Processa pedidos que já passaram do delay"""
    while True:
        try:
            ready_orders = get_ready_orders()
            
            for order_id, webhook_data in ready_orders:
                print(f"\n⏰ [SCHEDULER] Processando pedido agendado: {order_id}")
                await process_order(order_id, webhook_data)
        
        except Exception as e:
            print(f"[SCHEDULER ERROR] {e}")
        
        # Verificar a cada 30 segundos
        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    """Inicia o scheduler de pedidos pendentes"""
    print("🚀 Iniciando scheduler de pedidos pendentes...")
    asyncio.create_task(process_pending_orders())


# ============================================
# ROUTES - OAUTH
# ============================================

@app.get("/tiny/oauth/start")
async def oauth_start(account: str):
    if account not in ("orig", "dest1"):
        raise HTTPException(400, "account_invalida")

    cid, _, redir = oauth_client(account)
    params = {
        "client_id": cid,
        "redirect_uri": redir,
        "response_type": "code",
        "scope": "openid offline_access",
        "state": f"account={account}",
    }
    url = f"{OAUTH_BASE}/auth?{urllib.parse.urlencode(params)}"
    print(f"[{account.upper()} AUTH URL] {url}")
    return {"authorize_url": url}


@app.get("/tiny/oauth/callback")
async def oauth_callback(code: str, state: str = ""):
    if not state.startswith("account="):
        raise HTTPException(400, "state_invalido")

    account = state.split("=", 1)[1]
    cid, csec, redir = oauth_client(account)
    auth = basic_auth_header(cid, csec)

    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, read=30.0)) as c:
        r = await c.post(
            f"{OAUTH_BASE}/token",
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redir,
            },
        )

    print(f"[{account.upper()} TOKEN][STATUS]", r.status_code)

    if r.status_code != 200:
        raise HTTPException(502, "token_request_falhou")

    data = r.json()
    access = data.get("access_token")
    refresh = data.get("refresh_token")
    expires_in = data.get("expires_in")

    if not access or not refresh or not expires_in:
        raise HTTPException(502, "token_response_invalida")

    save_tokens(account, access, refresh, int(expires_in))
    return {"ok": True, "account": account}


@app.get("/redirect/orig/")
async def redirect_orig(code: str, state: str = "account=orig"):
    return await oauth_callback(code=code, state=state)


@app.get("/redirect/dest1/")
async def redirect_dest1(code: str, state: str = "account=dest1"):
    return await oauth_callback(code=code, state=state)


# ============================================
# ROUTES - WEBHOOK
# ============================================

@app.post("/webhooks/tiny")
@app.post("/webhooks/tiny/")
async def tiny_webhook(req: Request):
    """
    ✅ WEBHOOK COM DELAY DE 5 MINUTOS
    """
    payload = await safe_read_payload(req)

    print("\n=== [WEBHOOK] ===")
    print(json.dumps(payload, indent=2, ensure_ascii=False)[:500])

    origin_id = (
        payload.get("idPedido") or 
        payload.get("id") or 
        (payload.get("dados") or {}).get("id")
    )

    if not origin_id:
        print("[IGNORADO] missing_order_id")
        return {"status": "ignored", "reason": "missing_order_id"}

    origin_id = str(origin_id)

    if already_done(origin_id):
        print("[IGNORADO] duplicado", origin_id)
        return {
            "status": "ignored",
            "reason": "duplicate",
            "origin_id": origin_id,
        }

    # ⏱️ AGENDAR para processar com delay
    save_pending_order(origin_id, payload)
    
    scheduled_time = datetime.fromtimestamp(
        int(time.time()) + PROCESSING_DELAY
    ).strftime('%H:%M:%S')
    
    return JSONResponse(
        status_code=202,
        content={
            "status": "scheduled",
            "origin_id": origin_id,
            "delay_seconds": PROCESSING_DELAY,
            "scheduled_for": scheduled_time,
            "message": f"Pedido agendado para processamento em {PROCESSING_DELAY//60} minutos"
        }
    )


@app.get("/admin/pending")
async def list_pending():
    """Lista pedidos pendentes"""
    now = int(time.time())
    
    rows = DB.execute(
        "SELECT origin_order_id, scheduled_at FROM pending_orders ORDER BY scheduled_at"
    ).fetchall()
    
    pending = []
    for order_id, scheduled_at in rows:
        remaining = scheduled_at - now
        pending.append({
            "order_id": order_id,
            "scheduled_at": datetime.fromtimestamp(scheduled_at).strftime('%Y-%m-%d %H:%M:%S'),
            "remaining_seconds": max(0, remaining),
            "remaining_minutes": max(0, remaining // 60)
        })
    
    return {
        "total": len(pending),
        "pending_orders": pending
    }


@app.post("/admin/process-now/{order_id}")
async def process_now(order_id: str):
    """Força processamento imediato de um pedido pendente"""
    row = DB.execute(
        "SELECT webhook_data FROM pending_orders WHERE origin_order_id=?",
        (order_id,),
    ).fetchone()
    
    if not row:
        raise HTTPException(404, "pedido_nao_encontrado_na_fila")
    
    webhook_data = json.loads(row[0])
    
    print(f"\n🚀 [ADMIN] Processamento forçado do pedido {order_id}")
    await process_order(order_id, webhook_data)
    
    return {"status": "processed", "order_id": order_id}

@app.get("/")
@app.get("/health")
async def health():
    pending_count = DB.execute("SELECT COUNT(*) FROM pending_orders").fetchone()[0]
    processed_count = DB.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
    
    return {
        "status": "ok",
        "api_version": "v3",
        "delay_seconds": PROCESSING_DELAY,
        "pending_orders": pending_count,
        "processed_orders": processed_count
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)