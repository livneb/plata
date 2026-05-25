"""Central error sink. Captures to Postgres `error_log` + structlog + (CRITICAL) Telegram."""
from __future__ import annotations

import asyncio
import os
import traceback
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from inkcliq.core.db import ErrorLog, session_scope
from inkcliq.core.observability import get_logger

_log = get_logger("error_reporter")


VALID_SEVERITIES = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"}


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
