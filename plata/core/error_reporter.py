"""Central error sink. Captures to Postgres `error_log` + structlog + (CRITICAL) Telegram."""
from __future__ import annotations

import asyncio
import os
import traceback
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from plata.core.db import ErrorLog, session_scope
from plata.core.observability import get_logger

_log = get_logger("error_reporter")


VALID_SEVERITIES = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"}

# Known external providers + dashboard URLs the user can click to remedy a limit.
PROVIDER_INFO = {
    "openrouter": {"label": "OpenRouter", "url": "https://openrouter.ai/settings/credits"},
    "voyage":     {"label": "Voyage",     "url": "https://dashboard.voyageai.com/"},
    "alpaca":     {"label": "Alpaca",     "url": "https://app.alpaca.markets/"},
    "bybit":      {"label": "Bybit",      "url": "https://www.bybit.com/"},
    "telegram":   {"label": "Telegram",   "url": "https://core.telegram.org/bots"},
    "langfuse":   {"label": "Langfuse",   "url": "https://cloud.langfuse.com/"},
}


def _provider_from(error_type: str, message: str) -> str | None:
    m = (message or "").lower()
    if "openrouter" in m or "openrouter.ai" in m: return "openrouter"
    if "voyage" in m or "voyageai" in m or error_type == "EmbeddingRateLimited": return "voyage"
    if "alpaca" in m: return "alpaca"
    if "bybit" in m: return "bybit"
    if "telegram" in m: return "telegram"
    if "langfuse" in m: return "langfuse"
    return None


async def flag_api_limit(provider: str, message: str, *, ttl_s: int = 6 * 3600) -> None:
    """Record that an upstream provider is rate-limited / out-of-credits. The Activity
    page surfaces this next to the provider card with a link to fix it."""
    try:
        from plata.core.bus import get_redis
        info = PROVIDER_INFO.get(provider) or {"label": provider, "url": ""}
        import json as _json
        from datetime import datetime, timezone
        await get_redis().set(
            f"api_limit:{provider}",
            _json.dumps({
                "provider": provider,
                "label": info["label"],
                "url": info["url"],
                "message": (message or "")[:200],
                "at": datetime.now(timezone.utc).isoformat(),
            }),
            ex=ttl_s,
        )
    except Exception:  # noqa: BLE001
        pass


def humanize(error_type: str, message: str) -> tuple[str, str]:
    """Map noisy upstream errors to short, actionable messages.

    Returns (severity_override_or_empty, friendly_message).
    """
    m = (message or "").lower()
    if error_type == "EmbeddingRateLimited" or "voyageai" in m and "rate" in m:
        return ("WARN",
                "Voyage embeddings rate-limited. Add a payment method at "
                "dashboard.voyageai.com to lift the free-tier 3 RPM / 10K TPM cap.")
    if error_type == "BadRequestError" and "minimum, minimum" in m or "bedrock" in m and "schema" in m:
        return ("ERROR",
                "LLM provider (Bedrock) rejected the JSON schema. The client already strips "
                "minimum/maximum; if you see this again the schema has another unsupported keyword.")
    if "openai.ratelimiterror" in (error_type or "").lower() or "rate limit" in m:
        return ("WARN", "LLM provider rate-limited the request. Will retry on the next message.")
    if "budgetexceedederror" in (error_type or "").lower():
        return ("CRITICAL",
                "LLM daily budget exceeded — system was halted. Raise the budget in Risk Config "
                "(`llm.daily_budget_usd_total`) or wait until the daily counter rolls over.")
    return ("", message)


class ErrorReporter:
    """Singleton-style reporter accessible from any agent."""

    def __init__(self) -> None:
        self._critical_callbacks: list = []

    def register_critical_callback(self, callback) -> None:
        """Register a coroutine to call on every CRITICAL error (e.g., Telegram push)."""
        self._critical_callbacks.append(callback)

    async def capture(
        self,
        *,
        agent: str,
        severity: str,
        error_type: str,
        message: str,
        traceback_str: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        severity = severity.upper()
        if severity not in VALID_SEVERITIES:
            severity = "ERROR"

        # Apply humanization to make upstream errors actionable in the dashboard.
        override_severity, friendly = humanize(error_type, message)
        if override_severity:
            severity = override_severity
        message = friendly

        # If the error looks like a credit / rate-limit problem, flag the provider
        # so the Activity page can surface a "limit reached" badge with a fix link.
        provider = _provider_from(error_type, message)
        if provider:
            low = (message or "").lower()
            if any(k in low for k in ("rate limit", "rate-limit", "rate_limit",
                                       "credit", "402", "429", "billing", "payment",
                                       "quota", "max_tokens")):
                await flag_api_limit(provider, message)

        container = os.environ.get("SERVICE_ENTRYPOINT", "unknown")
        context = context or {}

        # Always log to stdout first — survives DB outages.
        _log.bind(agent=agent, severity=severity, error_type=error_type, **context).error(
            "error_captured", message=message
        )

        # Persist to Postgres. Don't crash the caller if DB is unavailable.
        try:
            async with session_scope() as session:
                session.add(
                    ErrorLog(
                        container=container,
                        agent=agent,
                        severity=severity,
                        error_type=error_type,
                        message=message,
                        traceback=traceback_str,
                        context=context,
                    )
                )
        except SQLAlchemyError:  # pragma: no cover
            _log.exception("error_log_persist_failed")

        # CRITICAL → fire callbacks (e.g., Telegram). Don't await — fire and forget.
        if severity == "CRITICAL":
            for cb in self._critical_callbacks:
                try:
                    asyncio.create_task(cb(agent, error_type, message, context))
                except Exception:  # pragma: no cover
                    _log.exception("critical_callback_failed")

    async def capture_exception(
        self,
        exc: BaseException,
        *,
        agent: str,
        severity: str = "ERROR",
        context: dict[str, Any] | None = None,
    ) -> None:
        await self.capture(
            agent=agent,
            severity=severity,
            error_type=type(exc).__name__,
            message=str(exc) or repr(exc),
            traceback_str="".join(traceback.format_exception(exc)),
            context=context,
        )


_singleton: ErrorReporter | None = None


def get_error_reporter() -> ErrorReporter:
    global _singleton
    if _singleton is None:
        _singleton = ErrorReporter()
    return _singleton
