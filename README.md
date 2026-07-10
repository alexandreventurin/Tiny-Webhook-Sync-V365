# Tiny Webhook Sync V365

Sistema de sincronização de pedidos entre duas contas Tiny ERP:

- **A (Rejuderme)**: conta de origem (pedidos criados via Shopify)
- **C (V365)**: conta de destino (fulfillment / dropshipping)

Quando um pedido é aprovado em A, o sistema clona automaticamente em C, mantém status e rastreio sincronizados nas duas pontas e propaga notas fiscais e códigos de rastreio.

Rodando em produção em [tiny-webhook-sync-v365.fly.dev](https://tiny-webhook-sync-v365.fly.dev).

---

## Sumário

- [Arquitetura](#arquitetura)
- [Fluxos principais](#fluxos-principais)
- [Job types](#job-types)
- [Dashboard](#dashboard)
- [Feature flags](#feature-flags)
- [Endpoints admin](#endpoints-admin)
- [Banco de dados](#banco-de-dados)
- [Deploy e infra](#deploy-e-infra)
- [Autenticação OAuth](#autenticação-oauth)
- [Observabilidade e auto-recovery](#observabilidade-e-auto-recovery)
- [Troubleshooting](#troubleshooting)
- [Estrutura do código](#estrutura-do-código)

---

## Arquitetura

```
        Webhooks A (Rejuderme)               Webhooks C (V365)
                │                                     │
                ▼                                     ▼
        ┌───────────────────────────────────────────────────┐
        │           FastAPI (Fly.io — região iad)           │
        │                                                   │
        │  ┌──────────────┐    ┌──────────────────────┐    │
        │  │ HTTP handlers│    │ Worker loop async    │    │
        │  │ (webhooks +  │    │ (poll a cada 5s +    │    │
        │  │ admin/dash)  │    │  heartbeat + jobs)   │    │
        │  └──────┬───────┘    └──────┬───────────────┘    │
        │         │                    │                    │
        └─────────┼────────────────────┼────────────────────┘
                  │                    │
                  ▼                    ▼
        ┌──────────────────────────────────────┐
        │       Supabase Postgres              │
        │  events / jobs / orders_map /        │
        │  orders_a_snapshot / products_map /  │
        │  tiny_tokens / worker_heartbeat …    │
        └──────────────────────────────────────┘
                  │
                  ▼
        ┌──────────────────────────────────────┐
        │       API Tiny v3 (A e C)            │
        │  fetch orders, create orders, sync   │
        │  status, update tracking, notas      │
        └──────────────────────────────────────┘
```

- **Sem servidor de fila externo**: fila fica em Postgres (`public.jobs`) com locking `FOR UPDATE SKIP LOCKED`.
- **Sem downtime esperado**: worker roda dentro do processo web (task asyncio).
- **Auto-recovery**: healthcheck `/health` + Fly restarta a máquina se worker travar.

---

## Fluxos principais

### 1. Novo pedido A → replicação em C

1. Shopify cria pedido em A → Tiny A dispara webhook `A/vendas` com `codigo_situacao=aprovado`
2. `POST /webhooks/a/vendas` grava em `events` e enfileira job **`fetch_order_a`**
3. Worker roda `fetch_order_a`: busca dados completos do pedido na API v3 do Tiny A → salva em `orders_a_snapshot`
4. Automaticamente encadeia job **`create_order_c`** para o mesmo `venda_id`
5. `create_order_c`:
   - **Sempre busca dados frescos** da API de A (evita snapshots velhos com CPF/endereço desatualizado)
   - Verifica se o depósito de A é o correto para dropshipping (`DROPSHIPPING_DEPOSIT_ID_A`)
   - Mapeia produtos via `products_map` (id_a → id_c). Se algum SKU não mapeado → status **`waiting_sku`** + pré-cria linha parcial em `products_map`
   - Busca/cria contato em C por CPF/CNPJ
   - Cria pedido em C via `POST /pedidos` com:
     - `numeroOrdemCompra` = `numeroPedidoEcommerce` (Shopify), fallback vazio
     - `observacoes` = `"Repasse Tiny - origem id <A> nº <numeroPedido> [Origem: <formaEnvio> | Frete: <formaFrete>]"`
     - `enderecoEntrega` com os dados frescos do pedido A
   - Encadeia jobs de `add_tag_a` e `add_tag_c` (tag "Rejuderme" em C, "V365" em A)

### 2. Sincronização de status

- `A → C`: quando A muda para `aprovado` (dispara o fluxo 1) ou `cancelado` (job `sync_status`)
- `C → A`: quando C muda para `faturado`, `cancelado`, `enviado`, `entregue` (job `sync_status`)

### 3. Sincronização de rastreio (C → A)

Quando pedido em C entra em `pronto_envio`, dispara webhook → job **`sync_tracking_c_to_a`**:

1. Job criado com **1 minuto de delay** (dá tempo pra transportadora popular o código em C)
2. Worker busca detalhes do pedido em C
3. Se `codigoRastreamento` está vazio → **retry progressivo** (1+1+1+2+3 min, até 5 tentativas)
4. Quando aparece o código, chama `PUT /pedidos/{id}/despacho` em A com `codigoRastreamento` + `urlRastreamento`
5. Avança status em A se estiver atrás de `pronto_envio` (nunca rebaixa)

Se esgotar as 5 tentativas sem código → job vira `done` com `reason=no_tracking_code_after_retries` (fica na fila do "Sem Rastreio" no dashboard para reprocessar manual)

### 4. Nota fiscal (C → A)

Quando C emite NF, webhook `C/notas_fiscais` → job **`sync_nf_link`** propaga chave/link da NF para observações em A.

---

## Job types

Tabela `public.jobs`, coluna `job_type` restrita por CHECK constraint:

| Job type | O que faz | Trigger |
|---|---|---|
| `fetch_order_a` | Busca detalhes do pedido em A e salva em `orders_a_snapshot`; encadeia `create_order_c` | webhook `A/vendas` status=aprovado |
| `create_order_c` | Cria pedido replicado em C (com dados frescos de A) | encadeado do `fetch_order_a` |
| `sync_status` | Espelha mudança de status entre A e C | webhooks de status em ambas as pontas |
| `sync_tracking_c_to_a` | Copia rastreio de C para A e avança status em A | webhook `C/vendas` status=pronto_envio |
| `sync_nf_link` | Copia dados da NF de C para observações em A | webhook `C/notas_fiscais` |
| `add_tag_a` / `add_tag_c` | Adiciona tag ("V365" em A, "Rejuderme" em C) | encadeado do `create_order_c` |
| `update_numero_compra` | Atualiza `numeroOrdemCompra` em pedidos antigos em C (batch de correção) | admin manual |
| `fetch_label` / `fetch_nf_link` | Reservados (não em uso ativo) | — |
| `noop` | Placeholder para webhooks sem ação necessária | ignorado |

### Status possíveis dos jobs

- `queued`: na fila esperando
- `running`: worker está processando
- `done`: terminou (pode ter `skipped: true` no `action_preview`)
- `failed`: falhou (atualiza `attempts`, reagendado com backoff se `attempts < 5`)
- `dead`: falhou após `MAX_ATTEMPTS` (5)
- `waiting_sku`: aguardando João preencher `id_c` no `products_map` — botão "Reprocessar" no dashboard
- `skipped_not_mapped`: skip porque `orders_map` não existe (pedido em C não veio pela nossa clonagem)

---

## Dashboard

Interface web em [tiny-webhook-sync-v365.fly.dev/dashboard](https://tiny-webhook-sync-v365.fly.dev/dashboard).

### Cards principais

- **Webhooks**: total de eventos recebidos
- **Hoje OK / Hoje Erro / Na Fila**: contadores diários e da fila
- **Replicados**: total de pedidos em C que vieram da nossa clonagem
- **Aguardando SKU** (só se > 0): pedidos parados por SKU não mapeado — botão "Reprocessar" + link para editar `products_map` no Supabase
- **Sem Rastreio (2d)** (só se > 0): rastreios que esgotaram retries nos últimos 2 dias — botão "Reprocessar"
- **⚠️ Worker instável** (só se > 5min sem pulsar): aviso de instabilidade + reinicialização automática em andamento

### Botão "Reprocessar" dos cards

- **Hoje Erro** → `POST /admin/jobs/retry-failed`
- **Aguardando SKU** → `POST /admin/jobs/retry-waiting-sku`
- **Sem Rastreio (2d)** → `POST /admin/jobs/retry-tracking-skipped?days=2`

### Lista de jobs recentes

Filtros: status, tipo (`fetch_order_a`, `create_order_c`, `sync_status`, `sync_nf_link`, `sync_tracking_c_to_a`, `update_numero_compra`, `noop`), exclusão de noop.

### Feature flags

Painel de switches que controla o que o sistema faz:

- `replicate_orders`: clonagem de pedidos aprovados (webhook)
- `replicate_imports`: clonagem via importação em massa
- `sync_status_enviado/entregue/cancelado/faturado`: espelhamento de cada status
- `sync_nf_link`: envio de dados da NF
- `sync_tracking_pronto_envio`: sync de rastreio C→A

Toda flag pode ser ligada/desligada individualmente sem deploy.

---

## Endpoints admin

Todos sob `/admin/*`. Sem autenticação (assumindo que o dashboard não é público). Categorias:

### Jobs / worker

| Método | Rota | O que faz |
|---|---|---|
| GET | `/admin/jobs` | Lista jobs por status/tipo |
| GET | `/admin/jobs-dashboard` | Dados formatados para o dashboard |
| GET | `/admin/jobs/failed-count` | Contagem de failed por tipo |
| GET | `/admin/dashboard` | Snapshot completo (usado pelo dashboard.html) |
| POST | `/admin/jobs/run` | Roda até N jobs manualmente |
| POST | `/admin/worker/run_once` | Alias para `/admin/jobs/run` |
| POST | `/admin/jobs/retry-failed?job_type=X` | Move failed → queued (attempts=0) |
| POST | `/admin/jobs/retry-waiting-sku` | Move waiting_sku → queued |
| POST | `/admin/jobs/retry-tracking-skipped?days=2` | Reprocessa tracking sem código dos últimos N dias |

### Batches de correção (one-shot)

| Método | Rota | O que faz |
|---|---|---|
| POST | `/admin/jobs/fix-empty-tracking` | Cria novos jobs para pedidos que escreveram tracking vazio (~2000 corrigidos em maio/2026) |
| GET | `/admin/jobs/tracking-fix-report` | Relatório desse batch |
| GET | `/admin/jobs/fix-numero-compra-count` | Quantos pedidos podem ter numeroOrdemCompra atualizado |
| POST | `/admin/jobs/fix-numero-compra?limit=N` | Cria N jobs para atualizar `numeroOrdemCompra` em pedidos antigos com o número da Shopify |
| GET | `/admin/jobs/fix-numero-compra-report` | Progresso do batch |

### Importação em massa (backfill)

| Método | Rota | O que faz |
|---|---|---|
| GET | `/import` | UI para importação por range de datas |
| POST | `/admin/import/start` | Inicia importação em A |
| GET | `/admin/import` | Lista runs |
| GET | `/admin/import/{run_id}` | Detalhes de um run |
| POST | `/admin/import/{run_id}/requeue` | Reencanta itens de um run |
| POST | `/admin/import/{run_id}/cancel` | Cancela run em execução |
| POST | `/admin/jobs/backfill` | Enfileira jobs para pedidos aprovados em A |

### Tokens OAuth

| Método | Rota | O que faz |
|---|---|---|
| GET | `/admin/tokens` | Preview + expires_at dos tokens A e B |
| GET | `/admin/tokens/health` | Testa API de A e B com `ping_light` |
| GET | `/auth/a/start` | Link para reautenticar A (Rejuderme) |
| GET | `/auth/c/start` | Link para reautenticar C (V365) |
| GET | `/auth/a/callback`, `/auth/c/callback` | Callbacks do OAuth |

### Dados brutos e debug

| Método | Rota | O que faz |
|---|---|---|
| GET | `/admin/orders-a` | Lista snapshots de A |
| GET | `/admin/orders-a/{venda_a_id}` | Snapshot completo de um pedido A |
| GET | `/admin/orders-map` | Lista mapeamentos A↔C |
| GET | `/admin/products_map` | Lista de produtos mapeados |
| GET | `/admin/tiny_a/ping?venda_id=X` | Testa API de A com um pedido |
| GET | `/admin/tiny_c/order?venda_id=X` | Detalhes de um pedido em C |
| GET | `/admin/tiny_c/orders?ids=A,B,C` | Batch de pedidos em C |
| GET | `/admin/tiny_a/produtos` | Lista produtos de A |
| POST | `/admin/tiny_a/produtos/sync` | Sincroniza catálogo de A → `products_map` |
| GET | `/admin/replication-status` | Estatísticas de replicação A→C |
| GET | `/admin/events` | Últimos eventos (webhooks) |
| GET | `/admin/flags` / POST | Ler/escrever feature flags |
| GET | `/admin/runtime` | Versão do app + worker |
| GET | `/admin/server-info` | Timestamp de start do processo |

### Health / dashboard

| Método | Rota | O que faz |
|---|---|---|
| GET | `/health` | Retorna 200 se worker pulsou nos últimos 30min; 500 se travou (usado pelo healthcheck do Fly) |
| GET | `/dashboard` | Serve `app/static/dashboard.html` |
| GET | `/` | Root, retorna `{status:ok}` |

### Webhooks (Tiny → nós)

| Rota | Fonte | Situações |
|---|---|---|
| `POST /webhooks/a/vendas` | A (Rejuderme) | aprovado → replica; cancelado → sync status |
| `POST /webhooks/rejuderme/vendas` | alias de `/webhooks/a/vendas` | idem |
| `POST /webhooks/c/vendas` | C (V365) | faturado, cancelado, enviado, entregue → sync; pronto_envio → sync rastreio |
| `POST /webhooks/c/notas` | C (V365) | faturado → sync status |
| `POST /webhooks/c/notas_fiscais` | C (V365) | qualquer → sync NF link em A |
| `POST /webhooks/c/enviados` | C (V365) | (registro) |

Todos os webhooks retornam sempre `{ok: true}` para nunca causar retry do Tiny, mesmo em caso de erro interno (erro é registrado nos logs e no `event_key`).

---

## Banco de dados

Postgres no Supabase (`aws-1-us-east-1.pooler.supabase.com`). Todas as tabelas em `public` com **RLS habilitado** (acesso só via role `postgres` super-admin via connection direta).

### Tabelas principais

| Tabela | O que guarda |
|---|---|
| `events` | Todo webhook recebido (`source`, `topic`, `venda_id`, `codigo_situacao`, `payload`, `action_result`) |
| `jobs` | Fila de trabalho (`job_type`, `status`, `payload`, `attempts`, `run_after`, `dedupe_key`, `action_preview`, `last_error`) |
| `orders_map` | Mapeamento venda A ↔ venda C (`venda_a_id`, `venda_c_id`, `external_key`, `last_sync_status`) |
| `orders_a_snapshot` | Cache do payload do pedido em A (`venda_a_id`, `webhook_payload`, `fetched_payload`, `fetched_at`) |
| `products_map` | Mapeamento produto A → produto C (`id_a`, `id_c`, `sku`, `preco`, `ativo`) |
| `tiny_tokens` | Tokens OAuth de A e B (access, refresh, expires_at) |
| `feature_flags` | Flags de controle (key, enabled, functional, label, description) |
| `import_runs` / `import_run_items` | Histórico de importações em massa |
| `worker_heartbeat` | 1 linha, `updated_at` toca cada ciclo do worker (usado por `/health`) |

### Dedupe key dos jobs

- Formato: `<source>:<topic>:<venda_id>:<job_type>[:<codigo_situacao>]`
- Batches especiais: `tracking_fix:<venda_c_id>`, `update_numero_compra:<venda_c_id>`

---

## Deploy e infra

### Fly.io

- **App**: `tiny-webhook-sync-v365`
- **Região**: `iad` (Virginia). Antes era `gru` mas foi migrado em jul/2026 por causa de challenge Cloudflare no OAuth da Tiny do IP GRU.
- **VM**: `shared-cpu-1x`, 256 MB RAM
- **Auto-restart**: healthcheck HTTP a cada 60s no `/health`; se falha 3× seguidas, Fly reinicia
- **Deploy**: manual, via `fly deploy` local usando `.fly-token`

```bash
FLY_API_TOKEN="$(cat .fly-token)" fly deploy
```

### Variáveis de ambiente (secrets no Fly)

- `DATABASE_URL`: connection string Postgres do Supabase (via pooler, `statement_cache_size=0` no asyncpg)
- `TINY_A_CLIENT_ID` / `TINY_A_CLIENT_SECRET`: OAuth de A
- `TINY_C_CLIENT_ID` / `TINY_C_CLIENT_SECRET`: OAuth de C
- `APP_BASE_URL`: URL pública para OAuth callback
- `TINY_AUTH_BASE`: default `https://accounts.tiny.com.br`
- `TINY_API_BASE`: default `https://api.tiny.com.br/public-api/v3`
- `DROPSHIPPING_DEPOSIT_ID_A`: ID do depósito em A que qualifica para clonagem
- `DEST1_PRICE_LIST_ID`: lista de preços a usar em C
- `MAX_ORDERS_TO_REPLICATE`: kill-switch (0 = ilimitado)
- `ALLOW_VENDA_IDS`: allowlist para testes (vazio = todos)
- `ENABLE_FETCH_A`: on/off do fetch em A (default true)
- `FETCH_CACHE_MINUTES`: cache do `fetch_order_a` (default 30)
- `RATE_LIMIT_RESERVE`: reserva de chamadas antes de pausar (default 8)

### GitHub

Repo: [alexandreventurin/Tiny-Webhook-Sync-V365](https://github.com/alexandreventurin/Tiny-Webhook-Sync-V365)

- Sem CI/CD (workflows de deploy foram removidos, deploy é manual via CLI)
- Convenção: 1 commit por bloco lógico de mudanças, empurrar direto na `main`

---

## Autenticação OAuth

### Fluxo

1. João acessa `/auth/a/start` (ou `/auth/c/start`) no dashboard
2. Redireciona pra `accounts.tiny.com.br` — João loga e autoriza
3. Tiny redireciona pra `/auth/{a|c}/callback?code=...`
4. Nosso backend troca o `code` por `access_token` + `refresh_token` e salva em `tiny_tokens`
5. Worker faz refresh automático a cada ~30min (via `ensure_access_token` no início de cada job)

### Headers de bypass do Cloudflare

`tiny_oauth.py` envia `User-Agent`, `Accept`, `Referer`, `Origin` de browser realista porque o `accounts.tiny.com.br` tem Cloudflare com bot detection.

### Rotação

Refresh tokens da Tiny expiram em ~30 dias sem uso. Se o sistema fica muito tempo parado (worker travado), o refresh token pode expirar — nesse caso, precisa reautenticar via `/auth/a/start` e `/auth/c/start`.

---

## Observabilidade e auto-recovery

### `/health` endpoint

- **Verde (200)**: worker pulsou há < 30min
- **Vermelho (500)**: worker travou ou banco inacessível

### Heartbeat

Worker atualiza `worker_heartbeat.updated_at = NOW()` a cada ciclo do loop (~5s).

### Auto-restart

- Fly healthcheck bate no `/health` a cada 60s (`fly.toml → [[http_service.checks]]`)
- Se 3 checks consecutivos falharem (~3 min de 500), Fly reinicia a máquina
- Após restart, `lifespan` do FastAPI cria novo `worker_task` em background

### Alerta visual no dashboard

Card **"⚠️ Worker instável"** aparece quando `heartbeat_age > 5 min`. Some sozinho quando o worker volta.

### Logs

- `fly logs -n` para logs recentes
- Nível INFO por default, ERROR/WARNING para problemas
- Cada job loga: início (`refreshed snapshot`), sucesso (`Job N completed`), erro (`Job N failed`)

---

## Troubleshooting

### "Pedidos aprovados em A não aparecem em C"

1. Verifica se os webhooks estão configurados corretamente no Tiny A
2. `curl /admin/events?limit=5` — veio webhook desses pedidos?
3. `curl /health` — worker vivo?
4. `curl /admin/tokens/health` — tokens válidos?
5. Se tudo verde, verifica `/admin/jobs` filtrando por `status=queued` — pode ser fila lenta
6. Se worker parou: reinicia máquina Fly (`fly machine restart <id>`)

### "Pedido criado em C mas sem rastreio"

- Se webhook `pronto_envio` chegou, deveria ter tentado 5× com delay progressivo
- Se todas as tentativas falharam, aparece em "Sem Rastreio (2d)" no dashboard — clica "Reprocessar"
- Se persistir, a transportadora provavelmente não populou o código em C (verificar direto no Tiny C)

### "Erro 400: Cliente CPF/CNPJ ausente"

- CPF do cliente em A está vazio → equipe da Muy Bela precisa completar em A → depois reprocessar via botão "Reprocessar" do card "Hoje Erro"
- O fix de "dados frescos" (jul/2026) faz o sistema buscar dados atuais de A a cada tentativa, então basta corrigir e reprocessar

### "SKU não mapeado"

- Card "Aguardando SKU" mostra os pedidos travados
- Clica "Editar produtos" → abre `products_map` no Supabase → João preenche `id_c` e `ativo=true`
- Clica "Reprocessar" no card

### "OAuth 403 Forbidden com HTML do Cloudflare"

- Cloudflare bloqueou o IP do servidor
- Solução: migrar máquina Fly pra outra região (rápido) ou pedir whitelist na Tiny (definitivo)
- Ver histórico: aconteceu em maio/2026 quando estávamos em `gru`; migração pra `iad` resolveu

### "Worker travado"

- Card "Worker instável" aparece no dashboard após 5min
- Fly reinicia automaticamente após ~30min (health check falha)
- Se quiser acelerar: `fly machine restart <id>`

---

## Estrutura do código

```
app/
├── main.py           # FastAPI: rotas de webhook, admin, auth, healthcheck
├── worker.py         # Loop async: fetch_and_lock_jobs → dispatch por job_type
├── db.py             # Todas as queries + init_db + migrations idempotentes
├── tiny_client.py    # Cliente HTTP para API v3 do Tiny (com rate limit inteligente)
├── tiny_oauth.py     # OAuth: exchange, refresh, headers anti-Cloudflare
├── utils.py          # determine_job_type, generate_dedupe_key, normalize_status
├── settings.py       # Env vars
├── schemas.py        # Pydantic models (WebhookResponse, HealthResponse, etc)
└── static/
    ├── dashboard.html   # UI principal
    └── import.html      # UI de importação em massa

fly.toml              # Config do Fly (região, healthcheck, VM)
Dockerfile            # Python 3.11-slim + deps
pyproject.toml        # asyncpg, fastapi, httpx, pydantic, uvicorn
```

### Constantes importantes

- `MAX_ATTEMPTS = 5`: em `db.py`, define quando job vira `dead`
- `WORKER_HEARTBEAT_MAX_AGE_SECONDS = 1800`: em `main.py`, threshold do `/health`
- `TRACKING_DELAYS = [1, 1, 1, 2, 3]`: em `worker.py`, retry progressivo do sync de rastreio
- `SCOPES = "openid"`: em `tiny_oauth.py`

---

## Histórico resumido de mudanças importantes

| Data | Mudança |
|---|---|
| abr/2026 | Setup inicial, rate limit inteligente via headers, primeiro deploy Fly.io |
| mai/2026 | Fix tracking sync (delay + retry progressivo); batch de 2083 pedidos com rastreio corrigido |
| mai/2026 | Waiting SKU + auto-fill de products_map |
| mai/2026 | Fresh fetch no create_order_c (elimina snapshots velhos com CPF errado) |
| mai/2026 | `numeroOrdemCompra` recebe `numeroPedidoEcommerce` (Shopify) |
| jun/2026 | Migração Fly `gru` → `iad` (bypass Cloudflare no OAuth); RLS habilitado no Supabase |
| jul/2026 | `/health` com heartbeat + card "Worker instável"; threshold 30min |

---

## Contatos

- **Cliente**: Muy Bela / Rejuderme
- **Operação**: João (contas Tiny + suporte)
- **Suporte**: Leanna (Muy Bela)
- **Dev**: Alexandre Venturin / AXVT
