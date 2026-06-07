"""OpenRouter LLM client wrapper.

Responsibilities:
  - Route every agent to its configured model.
  - Wrap calls in Langfuse traces (decorator-style, opt-in).
  - Track per-agent and global cost in Redis with atomic INCRBYFLOAT.
  - Enforce daily / monthly budget caps (80% WARN, 100% circuit-breaker).
"""
from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Any

from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from plata.config.settings import get_settings
from plata.core.bus import Channels, get_redis, publish_channel
from plata.core.observability import get_langfuse_client, get_logger

_log = get_logger("llm")


class BudgetExceededError(RuntimeError):
    """Raised when an agent's daily LLM budget is hit (circuit-breaker)."""


# Per-agent paid-model defaults.
AGENT_MODELS: dict[str, str] = {
    "graph_ingestion": "anthropic/claude-haiku-4-5",
    "strategist": "anthropic/claude-sonnet-4-6",
    "reviewer": "anthropic/claude-sonnet-4-6",
    "historian": "anthropic/claude-sonnet-4-6",
    "risk_manager": "openai/o3-mini",
    "scraper": "anthropic/claude-haiku-4-5",
    "position_monitor": "anthropic/claude-haiku-4-5",
    "translator": "openai/gpt-4o-mini",
}

# Per-agent FREE OpenRouter models (the ":free" suffix triggers the free tier).
# Tradeoffs per agent:
#   graph_ingestion needs reliable structured-output → llama 3.3 70B is the
#     most reliable free option for JSON; Qwen also works.
#   strategist / reviewer want long context + reasoning → DeepSeek R1 or
#     Llama 3.3 70B; Gemini 2.0 Flash Exp also fine.
#   historian seeds with many events → DeepSeek R1 free has very high context.
#   risk_manager is mostly structured rules → small fast model is fine.
#   position_monitor / translator are short prompts → Gemini Flash or Qwen.
AGENT_MODELS_FREE: dict[str, str] = {
    "graph_ingestion": "meta-llama/llama-3.3-70b-instruct:free",
    "strategist":      "deepseek/deepseek-chat:free",
    "reviewer":        "deepseek/deepseek-chat:free",
    "historian":       "deepseek/deepseek-chat:free",
    "risk_manager":    "google/gemini-2.0-flash-exp:free",
    "scraper":         "qwen/qwen-2.5-72b-instruct:free",
    "position_monitor":"google/gemini-2.0-flash-exp:free",
    "translator":      "google/gemini-2.0-flash-exp:free",
}

# Fallback chain for any free model that's currently 404 / "No endpoints found".
# OpenRouter's free tier rotates providers; the listed model can have zero
# active endpoints for hours. The LLM client tries the next one in this list.
FREE_FALLBACKS: list[str] = [
    "deepseek/deepseek-chat:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "qwen/qwen-2.5-72b-instruct:free",
    "nousresearch/hermes-3-llama-3.1-405b:free",
    "mistralai/mistral-small-24b-instruct-2501:free",
]

# Curated suggestions surfaced in the Settings → Models tab so the user
# can pick from a vetted list per agent without typing model strings.
MODEL_CATALOG_FREE: list[str] = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "deepseek/deepseek-r1:free",
    "deepseek/deepseek-chat:free",
    "google/gemini-2.0-flash-exp:free",
    "qwen/qwen-2.5-72b-instruct:free",
    "nousresearch/hermes-3-llama-3.1-405b:free",
    "mistralai/mistral-small-24b-instruct-2501:free",
]
MODEL_CATALOG_PAID: list[str] = [
    "anthropic/claude-sonnet-4-6",
    "anthropic/claude-haiku-4-5",
    "anthropic/claude-opus-4-8",
    "openai/o3-mini",
    "openai/gpt-4o",
    "openai/gpt-4o-mini",
    "google/gemini-2.5-pro",
    "google/gemini-2.5-flash",
    "deepseek/deepseek-chat-v3",
]


# Resolve the model to use for a given agent based on Redis-stored config.
# Reads `llm_config` hash: `mode` (paid|auto|free) and `override:<agent>`.
async def resolve_model(agent: str) -> tuple[str, str]:
    """Return (model_id, mode) — mode is the effective mode after resolution."""
    redis = get_redis()
    try:
        cfg = await redis.hgetall("llm_config") or {}
    except Exception:  # noqa: BLE001
        cfg = {}
    mode = (cfg.get("mode") or "paid").lower()
    # Per-agent explicit override beats the mode rule.
    override = cfg.get(f"override:{agent}")
    if override:
        return override, mode
    if mode == "free":
        return AGENT_MODELS_FREE.get(agent, "google/gemini-2.0-flash-exp:free"), mode
    if mode == "auto":
        # If we recently hit 402, stay on free until the sticky pin expires.
        try:
            pinned = await redis.get("llm_config:auto_active_free")
        except Exception:  # noqa: BLE001
            pinned = None
        if pinned:
            return AGENT_MODELS_FREE.get(agent, "google/gemini-2.0-flash-exp:free"), "auto-free"
    return AGENT_MODELS.get(agent, "anthropic/claude-haiku-4-5"), mode


# Approximate prices (USD per 1M tokens) — updated by hand or via OpenRouter pricing API.
# These are estimates; OpenRouter's actual usage response is the source of truth when present.
MODEL_PRICES: dict[str, tuple[float, float]] = {
    "anthropic/claude-haiku-4-5":   (1.00, 5.00),
    "anthropic/claude-sonnet-4-6":  (3.00, 15.00),
    "openai/o3-mini":               (1.10, 4.40),
    "openai/gpt-4o-mini":           (0.15, 0.60),
}


_OPENAI_CLIENT_CACHE: dict[str, AsyncOpenAI] = {}


def _client() -> AsyncOpenAI:
    """Build (and cache) the OpenAI client. Prefers UI-set credentials over env."""
    settings = get_settings()
    from plata.config import credentials as _creds
    api_key = _creds.get_sync("openrouter") or (
        settings.openrouter_api_key.get_secret_value() if settings.openrouter_api_key else None
    )
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not configured")
    cached = _OPENAI_CLIENT_CACHE.get(api_key)
    if cached is not None:
        return cached
    cli = AsyncOpenAI(api_key=api_key, base_url=settings.openrouter_base_url)
    _OPENAI_CLIENT_CACHE.clear()  # only keep the current key around
    _OPENAI_CLIENT_CACHE[api_key] = cli
    return cli


_BEDROCK_INCOMPATIBLE_KEYS = {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum",
                              "multipleOf", "minLength", "maxLength", "pattern",
                              "minItems", "maxItems", "uniqueItems"}


def _sanitize_schema(node):
    """Strip JSON-schema keywords that Bedrock-backed structured output rejects."""
    if isinstance(node, dict):
        return {
            k: _sanitize_schema(v)
            for k, v in node.items()
            if k not in _BEDROCK_INCOMPATIBLE_KEYS
        }
    if isinstance(node, list):
        return [_sanitize_schema(x) for x in node]
    return node


def _today_key() -> str:
    return f"cost:daily:{date.today().isoformat()}"


def _month_key() -> str:
    return f"cost:monthly:{datetime.now(timezone.utc).strftime('%Y-%m')}"


def _estimate_cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    prices = MODEL_PRICES.get(model)
    if prices is None:
        # Unknown model → conservative high estimate so it still counts
        prompt_price, completion_price = 5.0, 15.0
    else:
        prompt_price, completion_price = prices
    return (prompt_tokens / 1_000_000) * prompt_price + (completion_tokens / 1_000_000) * completion_price


async def _read_budget_caps() -> dict[str, float]:
    """Pull current caps from Redis risk_config hash. Falls back to defaults."""
    redis = get_redis()
    settings = get_settings()
    raw = await redis.hgetall("risk_config")
    caps: dict[str, float] = {}
    caps["daily_total"] = float(raw.get("llm.daily_budget_usd_total", settings.default_llm_daily_budget_usd))
    caps["monthly_total"] = float(raw.get("llm.monthly_budget_usd_total", caps["daily_total"] * 30))
    # Per-agent caps stored as e.g. "llm.daily_budget_usd_per_agent.strategist"
    for k, v in raw.items():
        if k.startswith("llm.daily_budget_usd_per_agent."):
            agent = k.split(".")[-1]
            caps[f"daily_agent.{agent}"] = float(v)
    return caps


async def _record_and_check(agent: str, cost_usd: float,
                             model: str | None = None,
                             prompt_tokens: int | None = None,
                             completion_tokens: int | None = None) -> None:
    """Increment counters, persist to Postgres, enforce caps."""
    redis = get_redis()
    daily = _today_key()
    monthly = _month_key()
    daily_agent = f"{daily}:agent:{agent}"

    new_daily = float(await redis.incrbyfloat(daily, cost_usd))
    new_monthly = float(await redis.incrbyfloat(monthly, cost_usd))
    new_daily_agent = float(await redis.incrbyfloat(daily_agent, cost_usd))
    # Expire keys so they roll over naturally (history lives in Postgres).
    await redis.expire(daily, 60 * 60 * 36)
    await redis.expire(daily_agent, 60 * 60 * 36)
    await redis.expire(monthly, 60 * 60 * 24 * 35)

    # Durable history: one row per LLM call. Fire-and-forget — never let a
    # cost-row insert failure break the calling agent's actual work.
    try:
        from decimal import Decimal as _Dec
        from plata.core.db import LLMCost as _LLMCost, session_scope as _ss
        async with _ss() as session:
            session.add(_LLMCost(
                agent=agent, model=model,
                prompt_tokens=prompt_tokens, completion_tokens=completion_tokens,
                cost_usd=_Dec(str(cost_usd)),
            ))
    except Exception as exc:  # noqa: BLE001
        _log.warning("llm_cost_persist_failed", agent=agent, error=str(exc)[:160])

    caps = await _read_budget_caps()
    daily_cap = caps.get("daily_total", float("inf"))
    monthly_cap = caps.get("monthly_total", float("inf"))
    agent_cap = caps.get(f"daily_agent.{agent}", float("inf"))

    async def _warn_or_break(metric: str, current: float, cap: float) -> None:
        if cap <= 0:
            return
        ratio = current / cap
        if ratio >= 1.0:
            _log.error("llm_budget_exceeded", metric=metric, current=current, cap=cap)
            await publish_channel(Channels.SYSTEM_HALT, {"reason": "llm_budget_exceeded", "metric": metric})
            raise BudgetExceededError(f"{metric} exceeded: ${current:.4f} / ${cap:.4f}")
        if ratio >= 0.8:
            _log.warning("llm_budget_threshold", metric=metric, current=current, cap=cap, ratio=ratio)

    await _warn_or_break("daily_total", new_daily, daily_cap)
    await _warn_or_break("monthly_total", new_monthly, monthly_cap)
    await _warn_or_break(f"daily_agent.{agent}", new_daily_agent, agent_cap)


class LLMClient:
    """High-level LLM facade. One instance per agent."""

    def __init__(self, agent: str, model: str | None = None):
        self.agent = agent
        # Explicit override at construction time wins. Otherwise resolved
        # per-call via resolve_model() so live config edits take effect.
        self._explicit_model = model
        self.model = model or AGENT_MODELS.get(agent, "anthropic/claude-haiku-4-5")
        self._openai = _client()
        self._langfuse = get_langfuse_client()

    async def _refresh_model(self) -> str:
        """Resolve the current model from Redis llm_config; returns it and updates self.model."""
        if self._explicit_model:
            return self._explicit_model
        try:
            picked, _mode = await resolve_model(self.agent)
            self.model = picked
        except Exception:  # noqa: BLE001
            pass
        return self.model

    async def complete(
        self,
        *,
        messages: list[dict[str, Any]],
        response_format: dict[str, Any] | None = None,
        temperature: float = 0.2,
        max_tokens: int = 2048,
        metadata: dict[str, Any] | None = None,
    ) -> ChatCompletion:
        """Run a chat completion. Records cost and enforces budget caps."""
        # Pick up live config changes (mode / per-agent override).
        await self._refresh_model()
        trace = None
        if self._langfuse and hasattr(self._langfuse, "trace"):
            try:
                trace = self._langfuse.trace(
                    name=f"{self.agent}.complete",
                    metadata={"agent": self.agent, "model": self.model, **(metadata or {})},
                )
            except Exception:  # noqa: BLE001 — never let observability break the pipeline
                trace = None

        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            # Ask OpenRouter to include the actual billed cost in usage.
            # Without this, we estimate cost from prompt/completion tokens ×
            # local per-1M prices, which under-counts: cached input tokens,
            # reasoning tokens (for thinking models), and image tokens are
            # billed at different rates and the response's plain
            # prompt_tokens / completion_tokens don't differentiate them.
            "extra_body": {"usage": {"include": True}},
        }
        if response_format is not None:
            kwargs["response_format"] = response_format

        # If OpenRouter says "402 — can only afford N tokens", parse N and shrink.
        # Strip internal sentinel keys (anything starting with "_") before the
        # SDK call — the OpenAI client rejects unknown kwargs.
        async def _attempt_once():
            clean = {k: v for k, v in kwargs.items() if not k.startswith("_")}
            return await self._openai.chat.completions.create(**clean)

        response = None
        for _try in range(3):
            try:
                response = await _attempt_once()
                break
            except Exception as exc:  # noqa: BLE001
                msg = str(exc)
                low = msg.lower()
                # Credit-limit hint embedded in the OpenRouter message body.
                m = None
                if "can only afford" in low:
                    import re as _re
                    m = _re.search(r"can only afford (\d+)", low)
                if m:
                    affordable = max(64, int(m.group(1)) - 16)
                    if affordable < int(kwargs.get("max_tokens") or 0):
                        kwargs["max_tokens"] = affordable
                        _log.warning("llm_max_tokens_shrunk", new_max=affordable)
                        continue
                # Free-pool 404 / 429 / "No endpoints found" / "rate limit":
                # the model's free tier is unavailable or capped right now
                # (e.g. llama-3.3-70b-instruct:free has 8 RPM — fills fast).
                # Walk the FREE_FALLBACKS chain to a different free provider.
                is_free_404 = (
                    ":free" in (kwargs.get("model") or "")
                    and ("no endpoints found" in low or "404" in msg
                          or "rate limit" in low or "429" in msg)
                )
                if is_free_404:
                    tried = set(kwargs.setdefault("_tried_free", []))
                    tried.add(kwargs.get("model"))
                    next_model = None
                    for cand in FREE_FALLBACKS:
                        if cand not in tried:
                            next_model = cand; break
                    if next_model:
                        _log.warning("llm_free_404_fallback",
                                     from_model=kwargs.get("model"),
                                     to_model=next_model)
                        kwargs["model"] = next_model
                        kwargs["_tried_free"] = list(tried)
                        self.model = next_model
                        continue
                # Flag the provider as out-of-credits so the Activity page lights up.
                is_credit_error = ("402" in msg or "credit" in low or "billing" in low
                                   or "payment" in low or "insufficient" in low)
                if is_credit_error:
                    try:
                        from plata.core.error_reporter import flag_api_limit
                        await flag_api_limit("openrouter", msg)
                    except Exception:  # noqa: BLE001
                        pass
                    # AUTO mode: fall back to the free model for this agent and
                    # retry once. Sticky for 1h so we don't keep hitting 402.
                    try:
                        cfg = await get_redis().hgetall("llm_config") or {}
                    except Exception:  # noqa: BLE001
                        cfg = {}
                    if (cfg.get("mode") or "paid").lower() == "auto" \
                            and not kwargs.get("_already_fallback"):
                        free_model = AGENT_MODELS_FREE.get(
                            self.agent, "google/gemini-2.0-flash-exp:free"
                        )
                        _log.warning("llm_auto_fallback_to_free",
                                     agent=self.agent, from_model=self.model,
                                     to_model=free_model)
                        kwargs["model"] = free_model
                        kwargs["_already_fallback"] = True
                        try:
                            # Sticky pin: until openrouter credits return,
                            # subsequent calls also go to free immediately.
                            await get_redis().set(
                                "llm_config:auto_active_free", "1", ex=3600
                            )
                        except Exception:  # noqa: BLE001
                            pass
                        # Don't count this as a retry; reset try counter.
                        self.model = free_model
                        continue
                if _try == 2:
                    raise
                await asyncio.sleep(1 + _try * 2)
        if response is None:
            raise RuntimeError("LLM call returned no response")

        usage = response.usage
        if usage:
            # Prefer OpenRouter's actual billed cost (returned when we ask via
            # extra_body.usage.include=True). Falls back to the local estimate
            # when the field is missing (other gateways / mock responses).
            reported_cost: float | None = None
            try:
                # OpenAI SDK exposes unknown fields via .model_extra on Pydantic models.
                extra = getattr(usage, "model_extra", None) or {}
                if "cost" in extra:
                    reported_cost = float(extra["cost"])
                elif hasattr(usage, "cost") and getattr(usage, "cost") is not None:
                    reported_cost = float(getattr(usage, "cost"))
            except Exception:  # noqa: BLE001
                reported_cost = None
            estimated = _estimate_cost_usd(self.model, usage.prompt_tokens, usage.completion_tokens)
            cost = reported_cost if (reported_cost is not None and reported_cost > 0) else estimated
            await _record_and_check(
                self.agent, cost,
                model=self.model,
                prompt_tokens=getattr(usage, "prompt_tokens", None),
                completion_tokens=getattr(usage, "completion_tokens", None),
            )
            if trace is not None:
                try:
                    trace.update(
                        metadata={
                            "cost_usd": cost,
                            "cost_reported": reported_cost,
                            "cost_estimated": estimated,
                            "prompt_tokens": usage.prompt_tokens,
                            "completion_tokens": usage.completion_tokens,
                        }
                    )
                except Exception:  # noqa: BLE001
                    pass

        return response

    async def structured(
        self,
        *,
        messages: list[dict[str, Any]],
        schema: dict[str, Any],
        schema_name: str,
        temperature: float = 0.1,
        max_tokens: int = 8192,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """JSON-schema-locked output. Strongest defense against prompt injection.

        Default max_tokens is generous (8192) so structured-output calls with
        large schemas (e.g. historian's 10-event batches) don't get cut off
        mid-string — that surfaced as `JSONDecodeError: Unterminated string`
        in the historian. Caller can override with a smaller cap when the
        schema is known to be small.
        """
        response = await self.complete(
            messages=messages,
            response_format={
                "type": "json_schema",
                "json_schema": {"name": schema_name, "schema": _sanitize_schema(schema), "strict": True},
            },
            temperature=temperature,
            max_tokens=max_tokens,
            metadata=metadata,
        )
        content = response.choices[0].message.content or "{}"
        try:
            return json.loads(content)
        except json.JSONDecodeError as exc:
            # Truncated mid-string almost always means we hit max_tokens. Give
            # the caller a usable error instead of a raw decode trace, and
            # include the finish_reason + content tail so it's diagnosable.
            finish = "?"
            try:
                finish = response.choices[0].finish_reason or "?"
            except Exception:  # noqa: BLE001
                pass
            tail = content[-200:].replace("\n", " ")
            raise RuntimeError(
                f"LLM structured response was not valid JSON "
                f"(finish_reason={finish}, tail={tail!r}, original={exc.msg})"
            ) from exc
