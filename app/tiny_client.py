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
    
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }
    
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
            if response.status_code != 200:
                raise TinyApiError(response.status_code, response.text, url)
            return response.json()
    
    async def create_order(self, order_payload: dict) -> dict:
        url = f"{self._base_url}/pedidos"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=self._headers(), json=order_payload)
            if response.status_code not in (200, 201):
                raise TinyApiError(response.status_code, response.text, url)
            return response.json()
    
    async def search_contacts(self, cpf_cnpj: str) -> list:
        url = f"{self._base_url}/contatos"
        params = {"cpfCnpj": cpf_cnpj}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers(), params=params)
            if response.status_code != 200:
                raise TinyApiError(response.status_code, response.text, url)
            data = response.json()
            return data.get("itens", [])
    
    async def create_contact(self, contact_payload: dict) -> dict:
        url = f"{self._base_url}/contatos"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=self._headers(), json=contact_payload)
            if response.status_code not in (200, 201):
                raise TinyApiError(response.status_code, response.text, url)
            return response.json()
    
    async def search_products(self, codigo: str) -> list:
        url = f"{self._base_url}/produtos"
        params = {"codigo": codigo}
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers(), params=params)
            if response.status_code != 200:
                raise TinyApiError(response.status_code, response.text, url)
            data = response.json()
            return data.get("itens", [])
