# Tiny Webhooks Receiver

## Overview
FastAPI application to receive webhooks from Tiny ERP and store them in PostgreSQL (Supabase).

## Project Structure
```
app/
├── __init__.py
├── main.py       # FastAPI app, routes
├── db.py         # Database connection and queries (asyncpg)
├── settings.py   # Configuration from environment
├── schemas.py    # Pydantic models
└── utils.py      # Helper functions (hashing, key generation)
```

## Environment Variables
- `DATABASE_URL`: PostgreSQL connection string (Supabase)

## Endpoints
- `POST /webhooks/a/vendas` - Webhook for source A sales
- `POST /webhooks/b/vendas` - Webhook for source B sales
- `POST /webhooks/b/notas` - Webhook for source B invoices
- `POST /webhooks/b/enviados` - Webhook for source B shipments
- `GET /health` - Health check with event/job counts
- `GET /admin/jobs?status=queued&limit=50` - List jobs

## Database Tables
- `public.events` - Stores all webhook events with deduplication
- `public.jobs` - Job queue for processing

## Running
```bash
uvicorn app.main:app --host 0.0.0.0 --port 5000
```
