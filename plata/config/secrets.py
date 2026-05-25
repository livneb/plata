"""Defense-in-depth secret scoping.

Even though Railway env-vars are configured per container, this layer ensures
that even if a wrong env-var slipped in, an unauthorized agent cannot read it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from pydantic import SecretStr

from plata.config.settings import Settings


class SecretAccessError(RuntimeError):
    """Raised when an agent attempts to read a secret it isn't authorized for."""


# Maps agent name → set of setting fields it is allowed to read.
# Anything not listed is denied.
AGENT_PERMISSIONS: Final[dict[str, frozenset[str]]] = {
    # ingestion_hub agents
    "orchestrator": frozenset({"langfuse_public_key", "langfuse_secret_key"}),
    "scraper": frozenset({
        "reddit_client_id", "reddit_client_secret",
        "cryptopanic_api_key", "newsapi_key", "cryptonews_api_key",
        "lunarcrush_api_key", "whalealert_api_key",
        "voyage_api_key",  # for dedup embedding
        "openrouter_api_key",  # only if a source itself needs LLM cleanup
        "langfuse_public_key", "langfuse_secret_key",
    }),
    "telegram_bot": frozenset({"telegram_bot_token", "langfuse_public_key", "langfuse_secret_key"}),
    "dashboard": frozenset({"dashboard_session_secret", "langfuse_public_key", "langfuse_secret_key"}),
    # intelligence_sandbox agents
    "graph_ingestion": frozenset({
        "openrouter_api_key", "voyage_api_key",
        "langfuse_public_key", "langfuse_secret_key",
    }),
    "strategist": frozenset({
        "openrouter_api_key", "voyage_api_key",
        "langfuse_public_key", "langfuse_secret_key",
    }),
    "reviewer": frozenset({
        "openrouter_api_key", "voyage_api_key",
        "langfuse_public_key", "langfuse_secret_key",
    }),
    "historian": frozenset({
        "openrouter_api_key", "voyage_api_key",
        "langfuse_public_key", "langfuse_secret_key",
    }),
    "oracle": frozenset({"langfuse_public_key", "langfuse_secret_key"}),
    # execution_vault agents — the ONLY ones with Bybit access
    "risk_manager": frozenset({
        "bybit_api_key", "bybit_api_secret",
        "openrouter_api_key",  # reasoning second-opinion
        "langfuse_public_key", "langfuse_secret_key",
    }),
    "executor": frozenset({
        "bybit_api_key", "bybit_api_secret",
        "langfuse_public_key", "langfuse_secret_key",
    }),
}


@dataclass(slots=True, frozen=True)
class ScopedSecrets:
    """Wraps `Settings` and only returns secrets the calling agent is whitelisted for."""

    agent: str
    _settings: Settings

    def get(self, key: str) -> SecretStr | str | bool | int | float | None:
        allowed = AGENT_PERMISSIONS.get(self.agent)
        if allowed is None:
            raise SecretAccessError(f"Unknown agent: {self.agent!r}")
        if key not in allowed:
            raise SecretAccessError(
                f"Agent {self.agent!r} is not authorized to read secret {key!r}"
            )
        return getattr(self._settings, key)

    def reveal(self, key: str) -> str:
        """Helper that returns the plaintext value of a SecretStr (after auth check)."""
        value = self.get(key)
        if value is None:
            raise SecretAccessError(f"Secret {key!r} is not configured")
        if isinstance(value, SecretStr):
            return value.get_secret_value()
        return str(value)
