from plata.core.db.engine import get_engine, get_sessionmaker, session_scope
from plata.core.db.models import (
    AgentActivityLog,
    ApiCredential,
    AuditLog,
    BacktestRun,
    BacktestTrade,
    Base,
    ConfigSetting,
    ErrorLog,
    EventPriceWindow,
    LLMCost,
    Proposal,
    SignalArchive,
    SysopFinding,
    TradeLedger,
    User,
)


async def ensure_aux_tables() -> None:
    """Auto-create tables that we don't manage with Alembic (small, additive
    helpers). Safe to call from every service's startup — uses checkfirst=True
    so it no-ops if tables already exist.

    Called by:
      • dashboard lifespan
      • execution_vault entrypoint (so the strategist's record_drop() can
        write rows even if the dashboard service hasn't booted yet)
      • intelligence_sandbox entrypoint (same reason — strategist lives there)
    """
    import logging
    log = logging.getLogger("db.ensure_aux_tables")
    tables = [Proposal, AgentActivityLog, ApiCredential, LLMCost, SysopFinding]
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            for model in tables:
                try:
                    await conn.run_sync(lambda c, m=model: m.__table__.create(c, checkfirst=True))
                except Exception as exc:  # noqa: BLE001
                    log.warning("ensure_aux_table_failed %s: %s", model.__tablename__, str(exc)[:160])
    except Exception as exc:  # noqa: BLE001
        log.warning("ensure_aux_tables_failed: %s", str(exc)[:160])


__all__ = [
    "Base",
    "TradeLedger",
    "SignalArchive",
    "EventPriceWindow",
    "LLMCost",
    "SysopFinding",
    "ConfigSetting",
    "User",
    "AuditLog",
    "ErrorLog",
    "BacktestRun",
    "BacktestTrade",
    "AgentActivityLog",
    "ApiCredential",
    "Proposal",
    "get_engine",
    "get_sessionmaker",
    "session_scope",
    "ensure_aux_tables",
]
