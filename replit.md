# Tiny Webhooks Receiver

## Overview
FastAPI application to receive webhooks from Tiny ERP and store them in PostgreSQL (Supabase). Includes a background worker for job processing (dry-run mode).

## Project Structure
```
app/
├── __init__.py
├── main.py       # FastAPI app, routes, background worker startup
├── db.py         # Database connection and queries (asyncpg)
├── settings.py   # Configuration from environment
├── schemas.py    # Pydantic models
├── utils.py      # Helper functions (hashing, key generation)
└── worker.py     # Job worker and processing logic
```

## Environment Variables
- `DATABASE_URL`: PostgreSQL connection string (Supabase Transaction Pooler)

## Endpoints

### Webhooks
- `POST /webhooks/a/vendas` - Webhook for source A sales
- `POST /webhooks/b/vendas` - Webhook for source B sales
- `POST /webhooks/b/notas` - Webhook for source B invoices
- `POST /webhooks/b/enviados` - Webhook for source B shipments

### Admin
- `GET /health` - Health check with metrics:
  - events_total, jobs_queued, jobs_failed, jobs_dead
  - last_event_at, last_job_done_at
- `GET /admin/jobs?status=queued&limit=50` - List jobs by status
- `POST /admin/jobs/run?limit=50` - Manual job processing round

## Database Tables
- `public.events` - Stores all webhook events with deduplication
- `public.jobs` - Job queue for processing
- `public.orders_map` - Maps external keys to order IDs

## Job Types
- `create_order_b` - Triggered by A/vendas/aprovado (codigoSituacao=3)
- `sync_status` - Triggered by B/notas/faturado (4) or B/enviados/enviado (5)
- `noop` - All other combinations

## Worker
- Background task runs every 5 seconds
- Fetches and locks queued jobs
- Processes jobs and sets action_preview (dry-run, no API calls)
- Handles errors with retry logic (max 5 attempts)

## Running
```bash
uvicorn app.main:app --host 0.0.0.0 --port 5000
```
