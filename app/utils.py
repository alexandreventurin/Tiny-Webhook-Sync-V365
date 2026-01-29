import hashlib
import json


def compute_payload_hash(payload: dict) -> str:
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload_str.encode()).hexdigest()[:16]


def generate_event_key(
    source: str,
    topic: str,
    venda_id: int | None,
    codigo_situacao: int | None,
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


def determine_job_type(source: str, topic: str, codigo_situacao: int | None) -> str:
    status_map = {
        3: "aprovado",
        4: "faturado",
        5: "enviado"
    }
    status = status_map.get(codigo_situacao) if codigo_situacao else None
    
    if source == "A" and topic == "vendas" and status == "aprovado":
        return "create_order_b"
    if source == "B" and topic == "notas" and status == "faturado":
        return "sync_status"
    if source == "B" and topic == "enviados" and status == "enviado":
        return "sync_status"
    
    return "noop"
