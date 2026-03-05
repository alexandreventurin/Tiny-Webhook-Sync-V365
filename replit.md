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
├── tiny_client.py  # HTTP client for Tiny API
└── static/
    └── dashboard.html  # Dashboard UI (controles + atividade)
```

## Environment Variables

### Required
- `DATABASE_URL`: PostgreSQL connection string (Supabase Transaction Pooler)

### Tiny API Integration (OAuth V3)
- `TINY_A_CLIENT_ID`: OAuth client ID for Tiny A
- `TINY_A_CLIENT_SECRET`: OAuth client secret for Tiny A
- `TINY_B_CLIENT_ID`: OAuth client ID for Tiny B
- `TINY_B_CLIENT_SECRET`: OAuth client secret for Tiny B
- `APP_BASE_URL`: Public URL for OAuth callbacks (e.g., https://your-app.replit.app)
- `TINY_API_BASE`: API base URL (default: https://api.tiny.com.br/public-api/v3)
- `TINY_AUTH_BASE`: Auth server URL (default: https://accounts.tiny.com.br)

### Feature Flags
- `ENABLE_FETCH_A`: Enable fetching from Tiny A (default: true)
- `EXECUTE_TINY_B`: Enable creating orders in Tiny B (default: false)
- `ALLOW_VENDA_IDS`: Comma-separated allowlist of venda_ids (e.g., "30012,30013")
- `FETCH_CACHE_MINUTES`: Cache duration for fetched orders (default: 10)
- `JOB_DELAY_MINUTES`: Delay in minutes before processing fetch_order_a jobs, allows tracking data to populate (default: 0, recommended: 5)

## Endpoints

### Webhooks
- `POST /webhooks/a/vendas` - Webhook for source A sales
- `POST /webhooks/b/vendas` - Webhook for source B sales
- `POST /webhooks/b/notas` - Webhook for source B invoices
- `POST /webhooks/b/notas_fiscais` - Webhook for source B fiscal notes
- `POST /webhooks/b/enviados` - Webhook for source B shipments

### Dashboard
- `GET /dashboard` - Painel visual com controles (liga/desliga) e atividade em tempo real

### Admin
- `GET /health` - Health check with metrics
- `GET /admin/runtime` - Returns APP_BUILD and WORKER_BUILD versions
- `GET /admin/flags` - Lista feature flags (controles do dashboard)
- `POST /admin/flags` - Liga/desliga uma feature flag `{"key":"...", "enabled":true/false}`
- `GET /admin/dashboard` - Dados do dashboard (stats, last event, recent jobs)
- `GET /admin/jobs?status=queued&limit=50` - List jobs by status
- `POST /admin/jobs/run?limit=50` - Manual job processing round
- `POST /admin/jobs/retry-failed?job_type=create_order_b` - Reprocessar jobs falhos (opcional: filtrar por tipo)
- `GET /admin/jobs/failed-count` - Conta jobs falhos por tipo
- `GET /admin/replication-status` - Status da replicação (contador, limite, se atingiu limite)
- `POST /admin/jobs/backfill?limit=100` - Cria jobs fetch_order_a para pedidos órfãos (eventos sem job)
- `POST /admin/worker/run_once?limit=50` - Debug: run worker once with detailed results
- `GET /admin/orders-a?limit=50` - List order snapshots
- `GET /admin/orders-a/{venda_a_id}` - Get specific order snapshot
- `GET /admin/orders-map?limit=50` - List order mappings (A -> B)

### Auth Diagnosis
- `GET /admin/tiny_a/ping?venda_id=XXXXX` - Test Tiny A auth (GET /pedidos/{id})
- `GET /admin/tiny_b/ping?venda_id=XXXXX` - Test Tiny B auth (GET /pedidos/{id})
- `GET /admin/tokens` - List OAuth token status (A/B) without exposing secrets
- `GET /admin/tokens/health` - Ping real das APIs A e B (GET /contatos?limite=1, timeout 5s) — dashboard usa para mostrar status real

### Product Mapping
- `GET /admin/tiny_a/produtos` - List all products from Tiny A with mapping status
- `POST /admin/tiny_a/produtos/sync` - Sync products from Tiny A to products_map table
- `GET /admin/products_map` - List all products from database table (for João to edit in Supabase)

### OAuth Endpoints
- `GET /auth/a/start` - Start OAuth flow for Tiny A (redirect to Tiny)
- `GET /auth/a/callback` - Callback from Tiny A OAuth, saves tokens
- `GET /auth/b/start` - Start OAuth flow for Tiny B
- `GET /auth/b/callback` - Callback from Tiny B OAuth

## Database Tables
- `public.events` - Stores all webhook events with deduplication
- `public.jobs` - Job queue for processing (includes payload, action_preview)
- `public.orders_map` - Maps external keys to order IDs (venda_a_id -> venda_b_id)
- `public.orders_a_snapshot` - Stores webhook and fetched payloads from Tiny A (includes last_error, needs_fetch)
- `public.tiny_tokens` - OAuth tokens for accounts A/B (access_token, refresh_token, expires_at)
- `public.products_map` - Mapeamento de produtos A -> B (editável pelo João no Supabase)
- `public.feature_flags` - Controles liga/desliga das funções (dashboard)

## Feature Flags (Dashboard Controls)
| Flag Key | Label | Descrição | Funcional |
|----------|-------|-----------|-----------|
| `replicate_orders` | Replicar Pedidos | Cria pedidos em B quando A é aprovado | Sim |
| `sync_status_enviado` | Sync Enviado | Espelha status enviado de A para B | Sim |
| `sync_status_entregue` | Sync Entregue | Espelha status entregue de A para B | Sim |
| `sync_status_cancelado` | Sync Cancelado | Espelha status cancelado entre A e B | Sim |
| `sync_status_faturado` | Sync Faturado | Espelha status faturado de B para A | Sim |
| `sync_nf_link` | Enviar NF | Envia dados da NF de B para observações de A | Sim |

## Job Flow

### sync_status gate (router)
- Antes de criar job `sync_status`, o router verifica se existe `orders_map` para o `venda_id`
- Source A → busca por `venda_a_id`; Source B → busca por `venda_b_id`
- Se não existir mapeamento → cria job `noop` com `action_result: "noop:no_orders_map"`
- Pedidos de B que não foram originados em A são ignorados silenciosamente com rastreabilidade

### A/vendas aprovado
1. Creates `fetch_order_a` job
2. Worker processes `fetch_order_a`:
   - Saves webhook payload to orders_a_snapshot
   - If ENABLE_FETCH_A=true and TINY_A_TOKEN set: calls GET /pedidos/{id}
   - Saves fetched_payload and fetched_at
   - Creates chained `create_order_b` job
3. Worker processes `create_order_b`:
   - **Gate Depósito**: Verifica `deposito.id` no fetched_payload
     - Se `deposito.id != 336403602` (Dropshipping Muy Bela) → skipped com reason `deposit_not_allowed`
     - Se `fetched_payload` ausente → failed (não pode verificar depósito)
   - If EXECUTE_TINY_B=false: dry-run with action_preview
   - If EXECUTE_TINY_B=true: calls POST /pedidos (Em Aberto)
   - Saves venda_b_id to orders_map

### B/notas_fiscais (sync_nf_link)
1. Webhook chega com `idNotaFiscalTiny` + dados da NF (numero, serie, chaveAcesso, dataEmissao, valorNota) → cria job `sync_nf_link` com dados da NF no payload
2. Worker processa `sync_nf_link`:
   - Verifica feature flag `sync_nf_link`
   - Extrai dados da NF do payload do job (numero, serie, chaveAcesso)
   - Fallback para jobs antigos: se `nf_chave_acesso` não está no payload, busca dados da NF na tabela `events` (evento original do webhook)
   - Busca `venda_b_id` na tabela `events` (webhook B/vendas com mesmo `id_nota_fiscal`)
   - Se não encontra evento de vendas → done/skipped com reason `no_vendas_event_for_nf`
   - Busca `orders_map` por `venda_b_id` → se não existe → done/skipped com reason `no_orders_map`
   - Busca pedido em A: `GET /pedidos/{venda_a_id}` para observações atuais
   - Concatena bloco NF após 2 linhas em branco:
     ```
     NF {numero} - {serie} | CHAVE DE ACESSO
     {chave_acesso}
     ```
   - Se protocolo disponível (via API fallback), adiciona bloco protocolo
   - Dedup: se chave de acesso já está nas observações → skipped
   - Atualiza pedido em A: `PATCH /pedidos/{venda_a_id}` com novas observações

## Job Types
- `fetch_order_a` - Fetch order details from Tiny A
- `create_order_b` - Create order in Tiny B (Em Aberto, somente depósito Dropshipping)
- `sync_status` - Sync status changes (pronto_envio, entregue, cancelado, faturado, enviado)
- `sync_nf_link` - Envia dados da NF de B para observações de A
- `add_tag_b` - Adiciona marcador "API Rejuderme" ao pedido em B (retry com backoff: 1min, 3min, 5min)
- `noop` - No operation

## Worker
- Background task runs every 5 seconds
- Resets stale locks (running > 2 minutes)
- Respects feature flags and allowlist
- Rate limit via FETCH_CACHE_MINUTES

## Product & Transport Mapping

### PRODUTO_ID_MAP
Direct product ID mapping from Tiny A to Tiny B (hardcoded in worker.py):
- Maps produto.id from source to destination
- Example: `335959369 → 969386704`

### SKU_PRICE / SKU_ALIAS
- SKU_PRICE: Override unit prices for specific SKUs
- SKU_ALIAS: Normalize SKU variations (e.g., "Rj Kit" → "RJ Kit")

### Transport Mapping (FORMA_ENVIO_MAP)
Mapeamento completo de formas de envio de A para B com suporte a formas de frete:

| Forma Envio A | ID Destino B | Formas de Frete |
|---------------|--------------|-----------------|
| FM Transportes | 895824123 | Standard (default), EXPRESSO |
| Correios (Sedex) | 846978945 | SEDEX CONTRATO AG (03220) (default), SEDEX 12, SEDEX 10, SEDEX HOJE |
| Correios (PAC) | 971399662 | PAC CONTRATO AG (03298) (default), MINI ENVIOS |
| Mercado Envios | (não configurado) | PAC, Sedex |

### Order Payload Fields (create_order_b)
- `listaPreco`: Links to price list in B (ID: 915701964)
- `transportador`: Maps shipping method via `build_transportador_v3()`
  - `formaEnvio.id`: ID da forma de envio mapeada
  - `formaEnvio.formaFrete`: Tipo de frete (Standard, SEDEX CONTRATO AG, etc)
  - `volumes`: Quantidade de volumes do pedido original
  - `codigoRastreamento`: Código de rastreio (se disponível)
  - `urlRastreamento`: URL de rastreio (se disponível)
- `numeroOrdemCompra`: Source order number
- `ecommerce.numeroPedidoEcommerce`: E-commerce reference
- `observacoes`: Inclui dados de origem (forma de envio/frete original)
- `pagamento`: Bloco fixo para todos os pedidos replicados
  - `formaPagamento.id`: 1 (Múltiplas)
  - `formaRecebimento.id`: 1 (Múltiplas)
  - `condicaoPagamento`: "0"
  - 1 parcela: dias=0, data=data da venda (aaaa-mm-dd 00:00:00), obs="API REJUDERME", formaPagamento.id=9, formaRecebimento.id=9
- **Marcador**: Após criar o pedido, adiciona marcador "API Rejuderme" via `POST /pedidos/{id}/marcadores` (falha no marcador não impede o job de concluir)

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
