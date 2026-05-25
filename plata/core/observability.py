"""structlog + Langfuse setup. Call `configure()` once per process at boot."""
from __future__ import annotations

import logging
import os
import sys
from functools import lru_cache

import structlog
from structlog.types import EventDict, Processor

from plata.config.settings import get_settings


def _add_container(_, __, event_dict: EventDict) -> EventDict:
    event_dict.setdefault("container", os.environ.get("SERVICE_ENTRYPOINT", "unknown"))
    return event_dict


def configure(level: str = "INFO") -> None:
    """Set up JSON logging to stdout. Idempotent."""
    if structlog.is_configured():
        return

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_container,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    configure()
    return structlog.get_logger(name)  # type: ignore[return-value]


@lru_cache
def get_langfuse_client():
    """Returns a Langfuse client if configured, else None.

    Langfuse is optional at runtime; missing keys → no-op, system keeps working.
    """
    settings = get_settings()
    if not (settings.langfuse_public_key and settings.langfuse_secret_key):
        return None
    try:
        from langfuse import Langfuse

        return Langfuse(
            public_key=settings.langfuse_public_key.get_secret_value(),
            secret_key=settings.langfuse_secret_key.get_secret_value(),
            host=settings.langfuse_host,
        )
    except Exception:  # pragma: no cover
        get_logger("observability").exception("langfuse_init_failed")
        return None
