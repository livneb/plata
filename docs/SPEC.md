# Plata — project spec

Last updated **v2.24.031**. This file is the canonical, agent-readable spec. Update it on every feature that meaningfully changes goals, data flow, or guarantees.

## What it is

Plata is a multi-agent crypto-derivative trading system with knowledge-graph reasoning. It scrapes market-moving news/social signals, enriches each into a graph event, embeds it for KNN comparison against a historical seed of dramatic market events, asks an LLM to propose a trade if conviction is high, gates the trade through risk + HITL approval, executes against Bybit, and runs post-mortems.

Defaults to **paper mode**. Live Bybit trades only happen when `paper_trading_mode=false` in risk config.

## Goals

1. **Macro-news driven**: catch headlines that historically moved markets and react fast.
2. **Comparative**: never trade off "this looks scary" alone — only when a vector-similar past event also moved its associated symbol meaningfully.
3. **Auditable**: every trade carries a `proposal_id` → strategist reasoning + analog events used; every HITL decision in `audit_log`.
4. **Safe-by-default**: paper mode default, kill switch, automatic halt on dead critical agents / DLQ spikes.
5. **Cheap to operate**: LLM daily-budget caps with auto-halt; rate-limit-aware embeddings; Bybit testnet for development.

## Non-goals

- HFT / sub-second latency. Loop time is measured in seconds.
- Equities. Bybit is crypto-only; an equities adapter would need a new `ExecutionClient`.
- Multi-user trading. One admin user; the HITL approver list is global.

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Runtime | Python 3.12 asyncio | Single dependency model across agents |
| Web | FastAPI + Jinja2 + Flowbite + htmx + Cytoscape.js | Lightweight, no SPA build, server-rendered |
| Streams | Redis Streams + consumer groups | Built-in DLQ pattern; pub/sub for halt |
| Graph | Redis JSON + RediSearch HNSW | Same instance; cheap KNN over embeddings |
| DB | Postgres (asyncpg + SQLAlchemy async + Alembic) | Audit + ledger |
| LLM | OpenRouter (Claude / GPT / Bedrock via one API) | Provider diversity; cost ceilings per agent |
| Embeddings | Voyage `voyage-3-large` 1024d | Best quality/$ for finance text at our volume |
| Observability | structlog + Langfuse (best-effort) | Stdout-first; tracing optional |
| Exchange | Bybit (CCXT) | Crypto perps; testnet for development |
| HITL | Telegram bot (python-telegram-bot) | Lowest-friction approve/reject UI |
| Hosting | Railway (Dockerfile build), 3 services | Cheap; matches secret-scoping needs |

## Data contract guarantees

- Every Pydantic schema with float bounds **must** clamp at the producer. KNN cosine `1-score` clamps to `[0,1]`; sentiment magnitudes coerced via `abs()` to `[0,1]`. Don't pass LLM floats through unguarded.
- JSON-schema sent to OpenRouter must not contain `minimum` / `maximum` / `pattern` (Bedrock rejects them). Sanitized in `core/llm._sanitize_schema`.
- DLQ is the system of record for failed messages. Replays use `{"data": <json>}` wire format to match `publish()`.
- Every consumer-loop agent records `processed_total` / `errors_total` / `agent_activity:<name>` (last 50). Event-driven agents (orchestrator, telegram_bot) use `log_action()` instead.

## Versioning

- File `VERSION` is authoritative. The topbar reads it through settings.
- Scheme: `<major>.<YY>.<NNN>`. Patch increments on every commit (`001` → `999` then `+1` minor).
- Each commit message is prefixed with the version (`v2.24.NNN: ...`).
- `CHANGELOG.md` is the source for the in-app version carousel modal.

## Active environment variables

| Var | Required by | Purpose |
|---|---|---|
| `SERVICE_ENTRYPOINT` | all containers | Dispatcher: ingestion_hub / intelligence_sandbox / execution_vault |
| `REDIS_URL` | all | Redis Stack connection |
| `POSTGRES_URL` | all | Async Postgres URL (`postgresql+asyncpg://…`) |
| `OPENROUTER_API_KEY` | intelligence + historian | LLM gateway |
| `VOYAGE_API_KEY` | intelligence | Embeddings (free-tier: 3 RPM, 10K TPM) |
| `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY` | optional | Tracing |
| `TELEGRAM_BOT_TOKEN` | ingestion_hub | HITL approvals; must not be set elsewhere (conflict) |
| `TELEGRAM_ALLOWED_USER_IDS` | ingestion_hub | Comma-separated; gate the bot |
| `BYBIT_API_KEY` / `BYBIT_API_SECRET` | execution_vault | Trading; testnet ↔ mainnet via `BYBIT_TESTNET` |
| `DASHBOARD_SESSION_SECRET` | ingestion_hub | Signed session cookies |
| `DASHBOARD_ADMIN_EMAIL` / `DASHBOARD_ADMIN_PASSWORD` | ingestion_hub | Bootstrap admin user (argon2-hashed on first boot) |
| `PORT` / `DASHBOARD_PORT` | ingestion_hub | Railway-injected port; dashboard binds here |
| `APP_VERSION` | optional | Override `VERSION` file value |

## Known sharp edges

- Voyage free tier (3 RPM) bottlenecks the whole pipeline. Add a payment method to unlock standard limits before expecting throughput.
- Railway deploys kill in-flight `asyncio.create_task` jobs (e.g. Historian seed). The status hash will read `STALE` after 3 minutes — click **Reset status** to recover.
- Mermaid-based architecture diagram lives in `docs/ARCHITECTURE.md`, not in the app, to avoid drift.
- The strategist's `SENTIMENT_TRIGGER_THRESHOLD = 0.5` filters most casual headlines. If proposals never appear, lower the threshold or feed richer sources.

## Roadmap pointers

- Cytoscape graph could overlay price-impact metrics (size nodes by magnitude).
- Equities adapter (Alpaca / IBKR) parallel to `bybit_client.py`.
- Curated historical events (replace LLM-generated seed for the well-known crises).
- Per-symbol position-sizing limits beyond global `risk_per_trade_pct`.
