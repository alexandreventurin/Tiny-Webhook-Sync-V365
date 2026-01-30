# Tiny Webhooks Receiver

## Overview
FastAPI application to receive webhooks from Tiny ERP and store them in PostgreSQL (Supabase). Implements two-stage integration:
1. Fetch order details from Tiny A (Rejuderme)
2. Create order in Tiny B (Muy Bela) with situacao=8 (Dados Incompletos)

## Project Structure
```
app/
├── __init__.py
├── main.py         # FastAPI app, routes, background worker startup
├── db.py           # Database connection and queries (asyncpg)
├── settings.py     # Configuration from environment
├── schemas.py      # Pydantic models
├── utils.py        # Helper functions (hashing, key generation)
├── worker.py       # Job worker and processing logic
└── tiny_client.py  # HTTP client for Tiny API
```

## Environment Variables

### Required
- `DATABASE_URL`: PostgreSQL connection string (Supabase Transaction Pooler)

### Tiny API Integration
- `TINY_A_TOKEN`: Bearer token for Tiny A (GET only)
- `TINY_B_TOKEN`: Bearer token for Tiny B (POST create order)
- `TINY_API_BASE`: API base URL (default: https://api.tiny.com.br/public-api/v3)

### Feature Flags
- `ENABLE_FETCH_A`: Enable fetching from Tiny A (default: true)
- `EXECUTE_TINY_B`: Enable creating orders in Tiny B (default: false)
- `ALLOW_VENDA_IDS`: Comma-separated allowlist of venda_ids (e.g., "30012,30013")
- `FETCH_CACHE_MINUTES`: Cache duration for fetched orders (default: 10)

## Endpoints

### Webhooks
- `POST /webhooks/a/vendas` - Webhook for source A sales
- `POST /webhooks/b/vendas` - Webhook for source B sales
- `POST /webhooks/b/notas` - Webhook for source B invoices
- `POST /webhooks/b/notas_fiscais` - Webhook for source B fiscal notes
- `POST /webhooks/b/enviados` - Webhook for source B shipments

### Admin
- `GET /health` - Health check with metrics
- `GET /admin/runtime` - Returns APP_BUILD and WORKER_BUILD versions
- `GET /admin/jobs?status=queued&limit=50` - List jobs by status
- `POST /admin/jobs/run?limit=50` - Manual job processing round
- `POST /admin/worker/run_once?limit=50` - Debug: run worker once with detailed results
- `GET /admin/orders-a?limit=50` - List order snapshots
- `GET /admin/orders-a/{venda_a_id}` - Get specific order snapshot
- `GET /admin/orders-map?limit=50` - List order mappings (A -> B)

### Auth Diagnosis
- `GET /admin/tiny_a/ping?venda_id=XXXXX` - Test Tiny A auth (GET /pedidos/{id})
- `GET /admin/tiny_b/ping?venda_id=XXXXX` - Test Tiny B auth (GET /pedidos/{id})

## Database Tables
- `public.events` - Stores all webhook events with deduplication
- `public.jobs` - Job queue for processing (includes payload, action_preview)
- `public.orders_map` - Maps external keys to order IDs (venda_a_id -> venda_b_id)
- `public.orders_a_snapshot` - Stores webhook and fetched payloads from Tiny A (includes last_error, needs_fetch)

## Job Flow

### A/vendas aprovado
1. Creates `fetch_order_a` job
2. Worker processes `fetch_order_a`:
   - Saves webhook payload to orders_a_snapshot
   - If ENABLE_FETCH_A=true and TINY_A_TOKEN set: calls GET /pedidos/{id}
   - Saves fetched_payload and fetched_at
   - Creates chained `create_order_b` job
3. Worker processes `create_order_b`:
   - If EXECUTE_TINY_B=false: dry-run with action_preview
   - If EXECUTE_TINY_B=true: calls POST /pedidos with situacao=8
   - Saves venda_b_id to orders_map

## Job Types
- `fetch_order_a` - Fetch order details from Tiny A
- `create_order_b` - Create order in Tiny B (situacao=8)
- `sync_status` - Sync status changes (pronto_envio, entregue, cancelado, faturado, enviado)
- `sync_nf_link` - Sync fiscal note links
- `noop` - No operation

## Worker
- Background task runs every 5 seconds
- Resets stale locks (running > 2 minutes)
- Respects feature flags and allowlist
- Rate limit via FETCH_CACHE_MINUTES

## Running
```bash
uvicorn app.main:app --host 0.0.0.0 --port 5000 --reload
```

## Testing
```bash
# Create webhook
curl -X POST http://localhost:5000/webhooks/a/vendas \
  -H "Content-Type: application/json" \
  -d '{"dados": {"id": 50001, "codigoSituacao": "aprovado"}}'

# Run worker manually
curl -X POST "http://localhost:5000/admin/worker/run_once?limit=10"

# Check runtime builds
curl http://localhost:5000/admin/runtime

# Check orders map
curl http://localhost:5000/admin/orders-map
```
