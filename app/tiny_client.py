import httpx
import logging
from dataclasses import dataclass
from typing import Optional

from app.settings import TINY_API_BASE

logger = logging.getLogger(__name__)


class TinyApiError(Exception):
    def __init__(self, status_code: int, body: str, url: str):
        self.status_code = status_code
        self.body = body[:500]
        self.url = url
        super().__init__(f"Tiny API error {status_code}: {self.body[:100]}")


@dataclass
class TinyPingResult:
    ok: bool
    status_code: int
    excerpt: Optional[dict] = None
    error: Optional[str] = None


class TinyClient:
    def __init__(self, token: str):
        self._token = token
        self._base_url = TINY_API_BASE
        self.last_ratelimit_remaining: Optional[int] = None
        self.last_ratelimit_reset: Optional[int] = None
        self.last_ratelimit_limit: Optional[int] = None

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }

    def _read_ratelimit_headers(self, response) -> None:
        try:
            self.last_ratelimit_remaining = int(response.headers.get("x-ratelimit-remaining", -1))
            self.last_ratelimit_reset = int(response.headers.get("x-ratelimit-reset", 0))
            self.last_ratelimit_limit = int(response.headers.get("x-ratelimit-limit", 0))
        except (ValueError, TypeError):
            pass
    
    async def ping_light(self) -> TinyPingResult:
        url = f"{self._base_url}/contatos"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url, headers=self._headers(), params={"limite": 1})
                if response.status_code == 200:
                    return TinyPingResult(ok=True, status_code=200)
                else:
                    return TinyPingResult(ok=False, status_code=response.status_code, error=response.text[:200])
        except httpx.TimeoutException:
            return TinyPingResult(ok=False, status_code=0, error="timeout")
        except Exception as e:
            return TinyPingResult(ok=False, status_code=0, error=str(e)[:200])

    async def ping_order(self, pedido_id: str) -> TinyPingResult:
        url = f"{self._base_url}/pedidos/{pedido_id}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=self._headers())
                if response.status_code == 200:
                    data = response.json()
                    excerpt = {
                        "id": data.get("id"),
                        "numero": data.get("numero"),
                        "situacao": data.get("situacao")
                    }
                    return TinyPingResult(ok=True, status_code=200, excerpt=excerpt)
                else:
                    return TinyPingResult(ok=False, status_code=response.status_code, error=response.text[:200])
        except httpx.TimeoutException:
            return TinyPingResult(ok=False, status_code=0, error="timeout")
        except Exception as e:
            return TinyPingResult(ok=False, status_code=0, error=str(e)[:200])
    
    async def get_order_details(self, pedido_id: str) -> dict:
        url = f"{self._base_url}/pedidos/{pedido_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers())
            self._read_ratelimit_headers(response)
            if response.status_code != 200:
                raise TinyApiError(response.status_code, response.text, url)
            return response.json()

    async def create_order(self, order_payload: dict) -> dict:
        url = f"{self._base_url}/pedidos"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=self._headers(), json=order_payload)
            self._read_ratelimit_headers(response)
            if response.status_code not in (200, 201):
                raise TinyApiError(response.status_code, response.text, url)
            return response.json()

    async def search_contacts(self, cpf_cnpj: str) -> list:
        url = f"{self._base_url}/contatos"
        params = {"cpfCnpj": cpf_cnpj}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers(), params=params)
            self._read_ratelimit_headers(response)
            if response.status_code != 200:
                raise TinyApiError(response.status_code, response.text, url)
            data = response.json()
            return data.get("itens", [])

    async def create_contact(self, contact_payload: dict) -> dict:
        url = f"{self._base_url}/contatos"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=self._headers(), json=contact_payload)
            self._read_ratelimit_headers(response)
            if response.status_code not in (200, 201):
                raise TinyApiError(response.status_code, response.text, url)
            return response.json()

    async def update_contact(self, contato_id: str, contact_payload: dict) -> dict:
        url = f"{self._base_url}/contatos/{contato_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.put(url, headers=self._headers(), json=contact_payload)
            self._read_ratelimit_headers(response)
            if response.status_code not in (200, 204):
                raise TinyApiError(response.status_code, response.text, url)
            if response.status_code == 204:
                return {}
            return response.json()

    async def search_products(self, codigo: str) -> list:
        url = f"{self._base_url}/produtos"
        params = {"codigo": codigo}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers(), params=params)
            self._read_ratelimit_headers(response)
            if response.status_code != 200:
                raise TinyApiError(response.status_code, response.text, url)
            data = response.json()
            return data.get("itens", [])

    async def update_order_status(self, pedido_id: str, situacao: int) -> None:
        url = f"{self._base_url}/pedidos/{pedido_id}/situacao"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.put(url, headers=self._headers(), json={"situacao": situacao})
            self._read_ratelimit_headers(response)
            if response.status_code not in (200, 204):
                raise TinyApiError(response.status_code, response.text, url)
        logger.info(f"Updated order {pedido_id} to situacao {situacao}")

    async def add_order_tags(self, pedido_id: str, tags: list[str]) -> bool:
        url = f"{self._base_url}/pedidos/{pedido_id}/marcadores"
        body = [{"descricao": t} for t in tags]
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=self._headers(), json=body)
            self._read_ratelimit_headers(response)
            if response.status_code == 204:
                logger.info(f"Added tags {tags} to order {pedido_id}")
                return True
            else:
                logger.warning(f"Failed to add tags to order {pedido_id}: {response.status_code} {response.text[:200]}")
                return False

    async def get_nota_fiscal(self, nota_id: str) -> dict:
        url = f"{self._base_url}/notas-fiscais/{nota_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers())
            self._read_ratelimit_headers(response)
            if response.status_code != 200:
                raise TinyApiError(response.status_code, response.text, url)
            return response.json()

    async def update_order(self, pedido_id: str, fields: dict) -> dict:
        url = f"{self._base_url}/pedidos/{pedido_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.put(url, headers=self._headers(), json=fields)
            self._read_ratelimit_headers(response)
            if response.status_code not in (200, 204):
                raise TinyApiError(response.status_code, response.text, url)
            if response.status_code == 204:
                return {}
            return response.json()

    async def update_order_despacho(self, pedido_id: str, codigo_rastreamento: str, url_rastreamento: str) -> None:
        """Atualiza info de despacho (rastreio) de um pedido. Usa PUT /pedidos/{id}/despacho.
        Nota: este endpoint falha (400) se o pedido ja tem expedicao criada."""
        url = f"{self._base_url}/pedidos/{pedido_id}/despacho"
        payload = {
            "codigoRastreamento": codigo_rastreamento or "",
            "urlRastreamento": url_rastreamento or "",
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.put(url, headers=self._headers(), json=payload)
            self._read_ratelimit_headers(response)
            if response.status_code not in (200, 204):
                raise TinyApiError(response.status_code, response.text, url)
        logger.info(f"Updated order {pedido_id} despacho: codigo={codigo_rastreamento}")

    async def list_orders(self, pagina: int = 1, data_inicial: str = None, data_final: str = None, limite: int = 100, sort: str = None) -> dict:
        url = f"{self._base_url}/pedidos"
        params = {"pagina": pagina, "limite": min(limite, 100)}
        if data_inicial:
            params["dataInicial"] = data_inicial
        if data_final:
            params["dataFinal"] = data_final
        if sort:
            params["sort"] = sort
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers(), params=params)
            if response.status_code != 200:
                raise TinyApiError(response.status_code, response.text, url)
            return response.json()

    async def list_all_products(self, limit: int = 100) -> list:
        url = f"{self._base_url}/produtos"
        all_products = []
        offset = 0
        async with httpx.AsyncClient(timeout=60.0) as client:
            while True:
                params = {"limite": min(limit, 100), "offset": offset}
                response = await client.get(url, headers=self._headers(), params=params)
                if response.status_code != 200:
                    raise TinyApiError(response.status_code, response.text, url)
                data = response.json()
                items = data.get("itens", [])
                if not items:
                    break
                all_products.extend(items)
                if len(items) < 100:
                    break
                offset += len(items)
                if len(all_products) >= 1000:
                    break
        return all_products
