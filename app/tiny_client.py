import httpx
import logging

from app.settings import TINY_API_BASE

logger = logging.getLogger(__name__)


class TinyClient:
    def __init__(self, token: str):
        self._token = token
        self._base_url = TINY_API_BASE
    
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }
    
    async def get_order_details(self, pedido_id: str) -> dict:
        url = f"{self._base_url}/pedidos/{pedido_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers())
            response.raise_for_status()
            return response.json()
    
    async def create_order(self, order_payload: dict) -> dict:
        url = f"{self._base_url}/pedidos"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=self._headers(), json=order_payload)
            response.raise_for_status()
            return response.json()
