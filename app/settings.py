import os

DATABASE_URL = os.getenv("DATABASE_URL")

TINY_API_BASE = os.getenv("TINY_API_BASE", "https://api.tiny.com.br/public-api/v3")
TINY_A_TOKEN = os.getenv("TINY_A_TOKEN")
TINY_B_TOKEN = os.getenv("TINY_B_TOKEN")

ENABLE_FETCH_A = os.getenv("ENABLE_FETCH_A", "true").lower() == "true"
EXECUTE_TINY_B = os.getenv("EXECUTE_TINY_B", "false").lower() == "true"

_allow_ids_raw = os.getenv("ALLOW_VENDA_IDS", "")
ALLOW_VENDA_IDS = set([x.strip() for x in _allow_ids_raw.split(",") if x.strip()])

FETCH_CACHE_MINUTES = int(os.getenv("FETCH_CACHE_MINUTES", "10"))
