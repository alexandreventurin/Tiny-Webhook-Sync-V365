# Project handoff: Tiny Webhook Sync V365

## Repository

- Remote: `https://github.com/alexandreventurin/Tiny-Webhook-Sync-V365.git`
- Working branch: `codex/update-contact-delivery-address`
- Runtime: Python 3.11+, FastAPI, PostgreSQL/Supabase, Tiny/Olist APIs, Fly.io
- Fly.io apps: `tiny-webhook-sync-v365` and `sync-rj`

After cloning, run `git status` and `git log -5 --oneline` to confirm the branch and latest commit. Do not begin from another local copy of the project.

## Current work carried by this branch

- Expanded the Gestão de Pedidos dashboard with period controls, queue/origin/synchronized/cancelled views, filters, error handling, divergence actions, and a separate `app/static/dashboard.js` asset.
- Added Rejuderme/V365 webhook and OAuth aliases while preserving the existing integration terminology.
- Added order comparison helpers, including delivery-address selection and normalized shipping labels.
- Added contact synchronization helpers and database support for deterministic contact mappings.
- Expanded worker flows for snapshot refresh, invoice observations, tracking synchronization, reconciliation, scheduled exports, and daily system jobs.
- Added unit coverage for contact synchronization and order-comparison behavior.
- Added the operational/technical manual under `docs/` and a second Fly.io configuration in `fly.sync-rj.toml`.

Review the actual diff and tests before changing behavior; this document is an orientation aid, not a replacement for the code.

## Local setup on the new computer

1. Clone the repository and check out `codex/update-contact-delivery-address`.
2. Create a virtual environment with Python 3.11 or newer.
3. Install the project dependencies from `pyproject.toml`/`uv.lock`.
4. Copy `.env.example` to `.env` and provide the real values through a secure channel.
5. Never copy a populated `.env` into Git or a Codex prompt.
6. Authenticate GitHub and Fly.io separately on the new computer when those tools are needed.

Typical local start command:

```text
uvicorn app.main:app --host 0.0.0.0 --port 5000 --reload
```

## Required security follow-up

- `ADMIN_PASSWORD` and `ADMIN_SESSION_SECRET` must be configured as environment/Fly.io secrets. Admin login is deliberately disabled when `ADMIN_PASSWORD` is empty.
- A previous administrative password existed in Git history. Removing it from the current files does not erase history, so rotate that credential before relying on the migrated checkout or performing the next deployment.
- Fly.io secrets are not stored in Git and will not be downloaded with the repository.

## Verification already used for the handoff

```text
python -m unittest discover -s tests -v
python -m compileall -q app
git diff --check
```

At handoff preparation time, all 13 unit tests passed. Live Tiny, Supabase, OAuth, and Fly.io integration tests were not run because this computer did not have an active Fly.io login and the migration process must not expose production credentials.

## Recommended first task on the new computer

Read `AGENTS.md`, this file, the latest commits, and the current diff. Confirm that the expected branch is checked out, rerun the verification commands, identify any remaining TODOs, and present a short continuation plan before modifying or deploying anything.
