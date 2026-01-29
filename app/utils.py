import hashlib
import json


def to_int_or_none(x):
    if x is None:
        return None
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if s.isdigit():
        return int(s)
    return None


def compute_payload_hash(payload: dict) -> str:
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload_str.encode()).hexdigest()[:16]


def generate_event_key(
    source: str,
    topic: str,
    venda_id: int | None,
    codigo_situacao: str | None,
    id_nota_fiscal: int | None,
    payload: dict
) -> str:
    payload_hash = compute_payload_hash(payload)
    parts = [
        source,
        topic,
        str(venda_id or ""),
        str(codigo_situacao or ""),
        str(id_nota_fiscal or ""),
        payload_hash
    ]
    return ":".join(parts)


def generate_dedupe_key(
    source: str,
    topic: str,
    venda_id: int | None,
    job_type: str
) -> str:
    parts = [source, topic, str(venda_id or ""), job_type]
    return ":".join(parts)


def normalize_status(codigo_situacao) -> str | None:
    if codigo_situacao is None:
        return None
    if isinstance(codigo_situacao, int):
        status_map = {
            3: "aprovado",
            4: "faturado",
            5: "enviado",
            6: "pronto_envio",
            7: "entregue",
            9: "cancelado"
        }
        return status_map.get(codigo_situacao)
    s = str(codigo_situacao).strip().lower()
    return s if s else None


def determine_job_type(source: str, topic: str, codigo_situacao) -> str:
    status = normalize_status(codigo_situacao)
    
    if source == "A" and topic == "vendas":
        if status == "aprovado":
            return "create_order_b"
        if status in ("pronto_envio", "entregue"):
            return "sync_status"
        if status == "cancelado":
            return "sync_status"
    
    if source == "B" and topic == "notas":
        if status == "faturado":
            return "sync_status"
    
    if source == "B" and topic == "enviados":
        if status == "enviado":
            return "sync_status"
    
    if source == "B" and topic == "notas_fiscais":
        return "sync_nf_link"
    
    return "noop"
