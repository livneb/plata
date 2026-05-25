# Plata

Multi-agent autonomous trading system with knowledge-graph reasoning.

## Architecture

Seven agents (+ a Historian seed agent) coordinated via Redis Streams, with a knowledge graph stored in Redis Stack (RedisJSON + RediSearch + Vector) and durable system-of-record in PostgreSQL. LLM calls flow through OpenRouter and are traced with Langfuse. Trading executes against Bybit Testnet via `ccxt`. Human-in-the-Loop approvals via Telegram bot + FastAPI dashboard (Flowbite UI).

See the full design in [`/root/.claude/plans/playful-gathering-catmull.md`](/root/.claude/plans/playful-gathering-catmull.md).

## Containers

Same image, three Railway services selected by `SERVICE_ENTRYPOINT`:

| Container | Processes | Has Bybit keys? |
|---|---|---|
| `ingestion_hub` | orchestrator, scraper, dashboard, telegram_bot | No |
| `intelligence_sandbox` | graph_ingestion, strategist, reviewer | No |
| `execution_vault` | risk_manager, executor | **Yes (only here)** |

## Local dev

```bash
cp .env.example .env  # fill in keys
docker compose up --build
# dashboard: http://localhost:8080
```

## Status

🚧 Bootstrap in progress — see plan for the 17-step implementation order.
