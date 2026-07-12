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
    job_type: str,
    codigo_situacao: str | None = None
) -> str:
    parts = [source, topic, str(venda_id or ""), job_type]
    if job_type == "sync_status" and codigo_situacao:
        parts.append(codigo_situacao)
    # sync_tracking_c_to_a: dedupe por venda_c_id — se vier 2x em seguida, segundo vira no-op
    return ":".join(parts)


def normalize_status(codigo_situacao) -> str | None:
    if codigo_situacao is None:
        return None
    # Fonte oficial (Tiny API v3):
    # 0=aberta, 1=faturada, 2=cancelada, 3=aprovada, 4=preparando_envio,
    # 5=enviada, 6=entregue, 7=pronto_envio, 8=dados_incompletos, 9=nao_entregue
    num_map = {
        0: "em_aberto",
        1: "faturado",
        2: "cancelado",
        3: "aprovado",
        4: "preparando_envio",
        5: "enviado",
        6: "entregue",
        7: "pronto_envio",
        8: "dados_incompletos",
        9: "nao_entregue",
    }
    if isinstance(codigo_situacao, int):
        return num_map.get(codigo_situacao)
    s = str(codigo_situacao).strip().lower().replace(" ", "_")
    try:
        return num_map.get(int(s))
    except (ValueError, TypeError):
        pass
    return s if s else None


def determine_job_type(source: str, topic: str, codigo_situacao) -> str:
    status = normalize_status(codigo_situacao)
    
    if source == "A" and topic == "vendas":
        if status == "em_aberto":
            return "approve_order_a"
        if status == "aprovado":
            return "fetch_order_a"
        if status in ("cancelado",):
            return "sync_status"

    if source == "B" and topic == "vendas":
        if status == "pronto_envio":
            return "sync_tracking_c_to_a"
        if status in ("faturado", "cancelado", "enviado", "entregue", "nao_entregue"):
            return "sync_status"
    
    if source == "B" and topic == "notas":
        if status == "faturado":
            return "sync_status"
    
    if source == "B" and topic == "notas_fiscais":
        return "sync_nf_link"
    
    return "noop"
