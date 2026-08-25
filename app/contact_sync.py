import hashlib
import json
import re
from datetime import date, datetime


def normalize_tax_id(value) -> str:
    return re.sub(r"\D", "", str(value or ""))


def numeric_id(value) -> int | None:
    text = str(value or "").strip()
    return int(text) if text.isdigit() else None


def _present(value) -> bool:
    return value not in (None, "")


def build_contact_payload(cliente: dict, endereco: dict) -> dict:
    cliente = cliente if isinstance(cliente, dict) else {}
    endereco = endereco if isinstance(endereco, dict) else {}
    cpf_cnpj = cliente.get("cpfCnpj") or cliente.get("cpf_cnpj") or ""
    cpf_digits = normalize_tax_id(cpf_cnpj)
    tipo_pessoa = cliente.get("tipoPessoa") or ("J" if len(cpf_digits) > 11 else "F")
    nome = str(cliente.get("nome") or "")[:50]

    address_payload = {
        "endereco": endereco.get("endereco") or endereco.get("logradouro"),
        "numero": endereco.get("enderecoNro") or endereco.get("numero"),
        "complemento": endereco.get("complemento"),
        "bairro": endereco.get("bairro"),
        "municipio": endereco.get("municipio") or endereco.get("cidade"),
        "cep": endereco.get("cep"),
        "uf": endereco.get("uf"),
    }
    address_payload = {key: value for key, value in address_payload.items() if _present(value)}

    payload = {
        "nome": nome,
        "cpfCnpj": cpf_cnpj,
        "tipoPessoa": tipo_pessoa,
        "email": cliente.get("email"),
        "telefone": cliente.get("telefone") or cliente.get("fone"),
        "celular": cliente.get("celular"),
        "endereco": address_payload,
    }
    return {key: value for key, value in payload.items() if _present(value)}


def _canonicalize(value, key: str = ""):
    if isinstance(value, dict):
        return {
            item_key: _canonicalize(item_value, item_key)
            for item_key, item_value in sorted(value.items())
            if _present(item_value)
        }
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    if isinstance(value, (int, float, bool)):
        return value

    text = " ".join(str(value or "").strip().split())
    if key in {"cpfCnpj", "cep", "telefone", "celular"}:
        return normalize_tax_id(text)
    if key == "email":
        return text.casefold()
    if key == "uf":
        return text.upper()
    return text.casefold()


def contact_fingerprint(contact_payload: dict) -> str:
    canonical = _canonicalize(contact_payload)
    encoded = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def extract_source_order_date(order_payload: dict) -> date | None:
    if not isinstance(order_payload, dict):
        return None
    for key in ("data", "dataPedido", "dataCriacao", "createdAt"):
        value = order_payload.get(key)
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value or "").strip()
        if not text:
            continue
        for fmt, length in (("%Y-%m-%d", 10), ("%d/%m/%Y", 10)):
            try:
                return datetime.strptime(text[:length], fmt).date()
            except ValueError:
                pass
    return None


def source_is_older(mapping: dict, order_date: date | None, order_id: int | None) -> bool:
    existing_date = mapping.get("source_order_date")
    if isinstance(existing_date, datetime):
        existing_date = existing_date.date()
    elif isinstance(existing_date, str):
        try:
            existing_date = date.fromisoformat(existing_date[:10])
        except ValueError:
            existing_date = None

    existing_id = numeric_id(mapping.get("source_order_a_id"))
    if existing_date and order_date:
        if order_date != existing_date:
            return order_date < existing_date
    elif existing_date and not order_date:
        return existing_id is None or order_id is None or order_id < existing_id
    elif order_date and not existing_date:
        return False

    return bool(existing_id is not None and order_id is not None and order_id < existing_id)


def choose_exact_contact(contacts: list[dict], cpf_cnpj_normalized: str) -> tuple[dict | None, int]:
    matches = [
        contact for contact in (contacts or [])
        if isinstance(contact, dict)
        and normalize_tax_id(contact.get("cpfCnpj") or contact.get("cpf_cnpj")) == cpf_cnpj_normalized
        and numeric_id(contact.get("id")) is not None
    ]
    matches.sort(key=lambda contact: numeric_id(contact.get("id")) or 0)
    return (matches[0] if matches else None), len(matches)
