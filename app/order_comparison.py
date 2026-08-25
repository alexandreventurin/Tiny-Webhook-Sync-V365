import re


_ADDRESS_FIELDS = (
    "cep",
    "uf",
    "municipio",
    "cidade",
    "endereco",
    "logradouro",
    "numero",
    "enderecoNro",
    "bairro",
    "complemento",
)

_REJUDERME_PREFIX = re.compile(
    r"^rejuderme\s*(?:-|:|\||\u2013|\u2014)\s*",
    re.IGNORECASE,
)


def normalize_shipping_label(value) -> str:
    """Normalize equivalent shipping labels used by the origin and destination."""
    text = " ".join(str(value or "").strip().split()).casefold()
    return _REJUDERME_PREFIX.sub("", text, count=1).strip()


def _has_address_data(address) -> bool:
    if not isinstance(address, dict):
        return False
    return any(str(address.get(field) or "").strip() for field in _ADDRESS_FIELDS)


def select_delivery_address(payload: dict, *, fallback_to_customer: bool = False) -> dict:
    """Use delivery address first and customer address only when delivery is absent."""
    payload = payload if isinstance(payload, dict) else {}
    delivery = payload.get("enderecoEntrega")
    if _has_address_data(delivery):
        return delivery
    if not fallback_to_customer:
        return delivery if isinstance(delivery, dict) else {}

    customer = payload.get("cliente") if isinstance(payload.get("cliente"), dict) else {}
    customer_address = customer.get("endereco")
    if _has_address_data(customer_address):
        return customer_address

    root_address = payload.get("endereco")
    return root_address if isinstance(root_address, dict) else {}
