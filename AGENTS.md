# Repository guidance for Codex

## Start here

- Read `HANDOFF.md` before continuing work.
- Inspect `git status` and the current branch before editing.
- Treat this repository as the source of truth. Keep durable decisions in checked-in documentation instead of relying only on chat history.

## Project context

- This is a Python 3.11+ FastAPI service integrating Rejuderme/Tiny origin data with V365/Tiny destination data.
- The code still uses the historical aliases `A` for Rejuderme and `C` (sometimes legacy `B`) for V365. Preserve compatibility when renaming domain concepts.
- PostgreSQL/Supabase stores webhook events, jobs, OAuth tokens, snapshots, mappings, feature flags, and contact mappings.
- Fly.io configurations are `fly.toml` (`tiny-webhook-sync-v365`) and `fly.sync-rj.toml` (`sync-rj`).

## Safety

- Never print, commit, or paste credentials, OAuth tokens, database URLs, cookies, or populated `.env` files.
- Use `.env.example` only as a list of required variables; keep real values in local `.env` files or Fly.io secrets.
- Do not deploy, rotate credentials, change production data, or run mutating Tiny/Supabase operations unless the user explicitly requests it.
- Preserve existing API routes and database compatibility unless a migration is intentional and documented.

## Verification

Run these checks before committing code changes:

```text
python -m unittest discover -s tests -v
python -m compileall -q app
git diff --check
```

For runtime or integration changes, also verify the relevant health/admin endpoint in a safe environment and report what was not tested against live services.
