import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

TINY_API_BASE = os.getenv("TINY_API_BASE", "https://api.tiny.com.br/public-api/v3")
TINY_AUTH_BASE = os.getenv("TINY_AUTH_BASE", "https://accounts.tiny.com.br")

TINY_A_TOKEN = os.getenv("TINY_A_TOKEN")
TINY_C_TOKEN = os.getenv("TINY_C_TOKEN")

TINY_A_CLIENT_ID = os.getenv("TINY_A_CLIENT_ID")
TINY_A_CLIENT_SECRET = os.getenv("TINY_A_CLIENT_SECRET")
TINY_C_CLIENT_ID = os.getenv("TINY_C_CLIENT_ID")
TINY_C_CLIENT_SECRET = os.getenv("TINY_C_CLIENT_SECRET")

APP_BASE_URL = os.getenv("APP_BASE_URL", "")

ENABLE_FETCH_A = os.getenv("ENABLE_FETCH_A", "true").lower() == "true"
EXECUTE_TINY_C = os.getenv("EXECUTE_TINY_C", "false").lower() == "true"

_allow_ids_raw = os.getenv("ALLOW_VENDA_IDS", "")
ALLOW_VENDA_IDS = set([x.strip() for x in _allow_ids_raw.split(",") if x.strip()])

FETCH_CACHE_MINUTES = int(os.getenv("FETCH_CACHE_MINUTES", "10"))

MAX_ORDERS_TO_REPLICATE = int(os.getenv("MAX_ORDERS_TO_REPLICATE", "0"))

JOB_DELAY_MINUTES = int(os.getenv("JOB_DELAY_MINUTES", "0"))
