import httpx
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from app.settings import (
    TINY_AUTH_BASE, TINY_API_BASE,
    TINY_A_CLIENT_ID, TINY_A_CLIENT_SECRET,
    TINY_B_CLIENT_ID, TINY_B_CLIENT_SECRET,
    APP_BASE_URL
)

logger = logging.getLogger(__name__)

SCOPES = "openid"


def get_credentials(account: str) -> tuple[str, str]:
    if account == "A":
        return TINY_A_CLIENT_ID or "", TINY_A_CLIENT_SECRET or ""
    else:
        return TINY_B_CLIENT_ID or "", TINY_B_CLIENT_SECRET or ""


def get_redirect_uri(account: str) -> str:
    base = APP_BASE_URL.rstrip("/") if APP_BASE_URL else ""
    return f"{base}/auth/{account.lower()}/callback"


def build_auth_url(account: str) -> str | None:
    client_id, _ = get_credentials(account)
    if not client_id:
        return None
    redirect_uri = get_redirect_uri(account)
    return (
        f"{TINY_AUTH_BASE}/realms/tiny/protocol/openid-connect/auth"
        f"?client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
        f"&scope={SCOPES.replace(' ', '%20')}"
    )


async def exchange_code_for_tokens(account: str, code: str) -> dict:
    client_id, client_secret = get_credentials(account)
    redirect_uri = get_redirect_uri(account)
    
    url = f"{TINY_AUTH_BASE}/realms/tiny/protocol/openid-connect/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "code": code
    }
    
    logger.info(f"Token exchange for {account}: client_id={client_id[:30]}..., secret_len={len(client_secret)}, redirect_uri={redirect_uri}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, data=data)
        if response.status_code != 200:
            logger.error(f"Token exchange failed: {response.status_code} - {response.text}")
        response.raise_for_status()
        return response.json()


async def refresh_access_token(account: str, refresh_token: str) -> dict:
    client_id, client_secret = get_credentials(account)
    
    url = f"{TINY_AUTH_BASE}/realms/tiny/protocol/openid-connect/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, data=data)
        response.raise_for_status()
        return response.json()


async def get_tokens_from_db(account: str) -> Optional[dict]:
    from app.db import get_pool
    p = await get_pool()
    async with p.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT access_token, refresh_token, expires_at FROM public.tiny_tokens WHERE account = $1",
            account
        )
        return dict(row) if row else None


async def save_tokens_to_db(account: str, access_token: str, refresh_token: str, expires_in: int) -> None:
    from app.db import get_pool
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    p = await get_pool()
    async with p.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.tiny_tokens (account, access_token, refresh_token, expires_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (account) DO UPDATE SET
                access_token = EXCLUDED.access_token,
                refresh_token = EXCLUDED.refresh_token,
                expires_at = EXCLUDED.expires_at,
                updated_at = NOW()
        """, account, access_token, refresh_token, expires_at)


async def ensure_access_token(account: str) -> Optional[str]:
    tokens = await get_tokens_from_db(account)
    if not tokens:
        logger.warning(f"No tokens found for account {account}")
        return None
    
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_at = tokens.get("expires_at")
    
    if not access_token or not refresh_token:
        logger.warning(f"Missing tokens for account {account}")
        return None
    
    now = datetime.now(timezone.utc)
    buffer = timedelta(minutes=5)
    
    if expires_at and expires_at > now + buffer:
        return access_token
    
    logger.info(f"Refreshing token for account {account}")
    try:
        new_tokens = await refresh_access_token(account, refresh_token)
        new_access = new_tokens.get("access_token")
        new_refresh = new_tokens.get("refresh_token") or refresh_token
        new_expires_in = new_tokens.get("expires_in", 3600)
        
        if not new_access:
            logger.error(f"No access_token in refresh response for {account}")
            return None
        
        await save_tokens_to_db(account, new_access, new_refresh, new_expires_in)
        logger.info(f"Token refreshed for account {account}")
        return new_access
    except Exception as e:
        logger.error(f"Failed to refresh token for account {account}: {e}")
        return None


async def list_token_status() -> list[dict]:
    from app.db import get_pool
    p = await get_pool()
    async with p.acquire() as conn:
        rows = await conn.fetch("""
            SELECT account, 
                   CASE WHEN refresh_token IS NOT NULL THEN true ELSE false END as has_refresh,
                   expires_at,
                   updated_at
            FROM public.tiny_tokens
            ORDER BY account
        """)
        return [dict(row) for row in rows]
