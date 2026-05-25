"""Centralised settings, loaded from environment variables via pydantic-settings."""
from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from typing import Annotated

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServiceEntrypoint(StrEnum):
    INGESTION_HUB = "ingestion_hub"
    INTELLIGENCE_SANDBOX = "intelligence_sandbox"
    EXECUTION_VAULT = "execution_vault"


class Settings(BaseSettings):
    """All runtime configuration. Field presence is required only where used;
    consumers should fail loudly if they need a missing one."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Service selection ---
    service_entrypoint: ServiceEntrypoint = ServiceEntrypoint.INGESTION_HUB
    env: str = "development"

    # --- Infrastructure (shared) ---
    redis_url: str = "redis://localhost:6379/0"
    postgres_url: str = "postgresql+asyncpg://plata:plata@localhost:5432/plata"

    # --- LLM / embeddings ---
    openrouter_api_key: SecretStr | None = None
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    voyage_api_key: SecretStr | None = None

    # --- Observability ---
    langfuse_public_key: SecretStr | None = None
    langfuse_secret_key: SecretStr | None = None
    langfuse_host: str = "https://cloud.langfuse.com"

    # --- Scraper sources ---
    reddit_client_id: SecretStr | None = None
    reddit_client_secret: SecretStr | None = None
    reddit_user_agent: str = "plata-scraper/0.1"
    cryptopanic_api_key: SecretStr | None = None
    newsapi_key: SecretStr | None = None
    cryptonews_api_key: SecretStr | None = None
    lunarcrush_api_key: SecretStr | None = None
    whalealert_api_key: SecretStr | None = None

    # --- Trading (execution_vault only) ---
    bybit_api_key: SecretStr | None = None
    bybit_api_secret: SecretStr | None = None
    bybit_testnet: bool = True

    # --- HITL / dashboard ---
    telegram_bot_token: SecretStr | None = None
    telegram_allowed_user_ids: str = ""  # comma-separated chat IDs
    dashboard_session_secret: SecretStr | None = None
    dashboard_port: Annotated[
        int,
        Field(ge=1, le=65535, validation_alias=AliasChoices("DASHBOARD_PORT", "PORT")),
    ] = 8080
    dashboard_admin_email: str | None = None
    dashboard_admin_password: SecretStr | None = None
    app_version: Annotated[
        str,
        Field(validation_alias=AliasChoices("APP_VERSION", "RAILWAY_GIT_COMMIT_SHA")),
    ] = "dev"

    # --- Bootstrap defaults (used only if risk_config has no row yet) ---
    default_paper_trading_mode: bool = True
    default_risk_per_trade_pct: float = 1.0
    default_max_open_positions: int = 3
    default_max_daily_loss_pct: float = 5.0
    default_llm_daily_budget_usd: float = 20.0

    @property
    def allowed_telegram_ids(self) -> set[int]:
        if not self.telegram_allowed_user_ids:
            return set()
        return {int(x.strip()) for x in self.telegram_allowed_user_ids.split(",") if x.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
