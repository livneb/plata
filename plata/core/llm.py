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
    "researcher": "anthropic/claude-sonnet-4-6",   # quality matters for the brain
    "council": "anthropic/claude-sonnet-4-6",      # deliberation aggregator
    "postmortem": "anthropic/claude-haiku-4-5",    # short lesson notes — Haiku is enough
}

# Agents whose system prompts should be prepended with the current
# knowledge_briefing (written by the researcher agent every ~30 min).
# Decision-makers benefit from market context; ingestion-side agents
# (graph_ingestion, scraper, translator) don't — they're classifying
# news, not making trades.
BRIEFING_AWARE_AGENTS: frozenset[str] = frozenset({
    "strategist", "position_monitor", "reviewer", "council", "historian",
    "postmortem",
})

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
    "risk_manager":    "qwen/qwen-2.5-72b-instruct:free",
    "scraper":         "qwen/qwen-2.5-72b-instruct:free",
    "position_monitor":"qwen/qwen-2.5-72b-instruct:free",
    "translator":      "qwen/qwen-2.5-72b-instruct:free",
}

# Fallback chain for any free model that's currently 404 / 429 / dead.
# Kept short and battle-tested. Retired-by-OpenRouter models stay out:
#   - mistral-small-24b-instruct-2501:free (retired ~v2.24.140)
#   - google/gemini-2.0-flash-exp:free (returns persistent "no endpoints
#     found" — removed v2.24.167 after 31x reviewer/graph_ingestion errors)
# Dynamically-discovered dead models are cached in Redis
# (key `llm:dead_free_models`, set, 24h TTL) and pre-filtered before each
# call, so this static list is the safety net, not the only mechanism.
#
# v2.24.174: chain now interleaves OpenRouter free + Google AI Studio free
# models, so a busy OpenRouter quota doesn't drown the whole pipeline.
# `google-ai-studio/<model>` prefix means "send to Google's API". Google AI
# Studio's free tier is generous (Flash: ~250 RPM, Pro: ~5 RPM) with no card.
FREE_FALLBACKS: list[str] = [
    # Google AI Studio (likely available when OpenRouter is saturated)
    "google-ai-studio/gemini-2.5-flash",
    "google-ai-studio/gemini-2.0-flash",
    "google-ai-studio/gemini-2.5-flash-lite",
    # OpenRouter free pool
    "deepseek/deepseek-chat:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "qwen/qwen-2.5-72b-instruct:free",
    "nousresearch/hermes-3-llama-3.1-405b:free",
    "deepseek/deepseek-r1:free",
    # Google AI Studio (heavier reasoning, stricter quota)
    "google-ai-studio/gemini-2.5-pro",
]


def _is_free_model(model: str) -> bool:
    """Return True if `model` is from any free pool — OpenRouter `:free`
    suffix or our `google-ai-studio/` prefix. Used everywhere we previously
    just checked `":free" in model`."""
    m = model or ""
    return m.endswith(":free") or m.startswith(PROVIDER_PREFIX_GOOGLE_AI_STUDIO)

# Permanently retired free models — always treated as dead regardless of
# what Redis cache or OpenRouter catalog says. Belt-and-suspenders so we
# never select these as the initial model either.
PERMANENTLY_RETIRED_FREE: frozenset[str] = frozenset({
    "google/gemini-2.0-flash-exp:free",
    "mistralai/mistral-small-24b-instruct-2501:free",
})


async def refresh_free_catalog() -> int:
    """Pull OpenRouter's current model catalog, filter to free models, cache.

    OpenRouter retires/adds free models often (mistral-small example). A static
    FREE_FALLBACKS list goes stale. This task — run daily from the dashboard
    lifespan — keeps the in-memory catalog current.

    Returns the number of free models discovered, or 0 on failure (static list
    remains the fallback).
    """
    import httpx as _httpx
    try:
        async with _httpx.AsyncClient(timeout=20) as client:
            r = await client.get("https://openrouter.ai/api/v1/models")
            r.raise_for_status()
            data = r.json() or {}
    except Exception as exc:  # noqa: BLE001
        _log.warning("free_catalog_refresh_failed", error=str(exc)[:160])
        return 0
    free: list[str] = []
    for m in (data.get("data") or []):
        slug = (m.get("id") or "")
        if not slug.endswith(":free"):
            continue
        pricing = m.get("pricing") or {}
        try:
            prompt_p = float(pricing.get("prompt") or 0)
            comp_p = float(pricing.get("completion") or 0)
        except (TypeError, ValueError):
            continue
        if prompt_p == 0 and comp_p == 0:
            if slug in PERMANENTLY_RETIRED_FREE:
                continue
            free.append(slug)
    if not free:
        _log.warning("free_catalog_refresh_empty", item_count=len(data.get("data") or []))
        return 0
    try:
        r_ = get_redis()
        pipe = r_.pipeline()
        pipe.delete("llm:free_catalog")
        pipe.sadd("llm:free_catalog", *free)
        pipe.expire("llm:free_catalog", 48 * 3600)  # 2-day safety margin
        # Mark every permanently-retired model as dead so the dead-cache stays
        # authoritative for them too (24h TTL, refreshed on every scan).
        if PERMANENTLY_RETIRED_FREE:
            pipe.sadd("llm:dead_free_models", *PERMANENTLY_RETIRED_FREE)
            pipe.expire("llm:dead_free_models", 24 * 3600)
        await pipe.execute()
    except Exception as exc:  # noqa: BLE001
        _log.warning("free_catalog_redis_write_failed", error=str(exc)[:160])
        return 0
    # Limited reconciliation: ONLY scrub overrides that point at a
    # permanently-retired model (where there's no possible recovery). Do
    # NOT auto-write new overrides — the chain walk handles stale defaults
    # at call time, and the operator's "(default)" / explicit choice in
    # /settings/?tab=models is authoritative. Pre-v2.24.178 we used to
    # repoint stale defaults via auto-written overrides, which fought the
    # operator: clearing an override on the UI would silently reappear at
    # the next catalog refresh.
    try:
        r_ = get_redis()
        existing_cfg = await r_.hgetall("llm_config") or {}
        to_delete: list[str] = []
        for k, v in existing_cfg.items():
            if not k.startswith("override:"):
                continue
            if v in PERMANENTLY_RETIRED_FREE:
                to_delete.append(k)
        if to_delete:
            await r_.hdel("llm_config", *to_delete)
            _log.info("override_scrubbed_retired", count=len(to_delete),
                      keys=to_delete)
        # If the sticky auto-active pin points at a retired model, wipe it.
        try:
            pinned = await r_.get("llm_config:auto_active_free")
            if pinned and pinned in PERMANENTLY_RETIRED_FREE:
                await r_.delete("llm_config:auto_active_free")
        except Exception:  # noqa: BLE001
            pass
        if repointed or to_delete:
            _log.info("free_catalog_reconciled",
                      repointed=len(repointed), deleted=len(to_delete))
    except Exception as exc:  # noqa: BLE001
        _log.warning("free_catalog_repoint_failed", error=str(exc)[:160])
    _log.info("free_catalog_refreshed", count=len(free))
    return len(free)


def _extract_first_json_object(text: str) -> str | None:
    """Best-effort: pull the outermost {...} block out of text that may have
    prose around it. Used when models append commentary like "Note: truncated
    for brevity" after their JSON. Returns None if no balanced object found.
    """
    if not text or "{" not in text:
        return None
    start = text.find("{")
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if esc:
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return None


def _looks_like_loop_output(content: str) -> bool:
    """Detect the pathological output some free models produce: long runs of
    a single whitespace or character (tabs, spaces, newlines, dots, dashes)
    that fill up max_tokens and never close the JSON. Heuristic: any single
    character (excluding common JSON syntax) repeats 50+ times in a row.
    """
    if not content or len(content) < 50:
        return False
    import re as _re
    return bool(_re.search(r"([^{}\[\],:\"0-9a-zA-Z_])\1{49,}", content))


async def _is_dead_free(model: str) -> bool:
    """Cached lookup: did the upstream provider recently tell us this free
    model is permanently unavailable, OR did it recently produce loopy
    garbage output? Two TTLs: dead-cache is 24h, garbage-producer is 10 min."""
    if not _is_free_model(model):
        return False
    if model in PERMANENTLY_RETIRED_FREE:
        return True
    try:
        r = get_redis()
        if await r.sismember("llm:dead_free_models", model):
            return True
        if await r.exists(f"llm:garbage_producer:{model}"):
            return True
    except Exception:  # noqa: BLE001
        return False
    return False


async def _throttle_free(model: str) -> None:
    """OpenRouter's free tier throttles per-model. Hammering the same model
    with rapid calls trips "no endpoints found" / 429s. Enforce a minimum
    interval between calls to each free model — read from llm_config
    (`free_throttle_sec`, default 6s → ~10 RPM per model).

    Uses Redis SETNX-with-EX to coordinate across agent processes."""
    if not _is_free_model(model):
        return
    try:
        r = get_redis()
        cfg = await r.hgetall("llm_config") or {}
        try:
            min_sec = float(cfg.get("free_throttle_sec") or 6)
        except (TypeError, ValueError):
            min_sec = 6.0
        if min_sec <= 0:
            return
        # Spin until the lock is free. Each iteration: try to take a TTL lock;
        # if taken (other call in flight) sleep and retry. Caps at ~30s total.
        deadline = asyncio.get_event_loop().time() + 30
        while asyncio.get_event_loop().time() < deadline:
            ok = await r.set(
                f"llm:free_throttle:{model}", "1",
                ex=max(1, int(min_sec)), nx=True,
            )
            if ok:
                return
            await asyncio.sleep(min(1.0, min_sec / 3))
    except Exception:  # noqa: BLE001
        pass


async def _mark_dead_free(model: str) -> None:
    try:
        r = get_redis()
        await r.sadd("llm:dead_free_models", model)
        await r.expire("llm:dead_free_models", 24 * 3600)
    except Exception:  # noqa: BLE001
        pass


async def _free_provider_preference() -> str:
    """Read llm_config:default_free_provider — `openrouter` / `google_ai_studio`
    / `both` (default). Used by the chain walk to filter candidates."""
    try:
        cfg = await get_redis().hgetall("llm_config") or {}
        v = (cfg.get("default_free_provider") or "both").lower().strip()
        if v in ("openrouter", "google_ai_studio", "both"):
            return v
    except Exception:  # noqa: BLE001
        pass
    return "both"


def _matches_provider_preference(model: str, preference: str) -> bool:
    """True if `model` is allowed under the current `default_free_provider`
    setting. Both → everything; otherwise filter by prefix."""
    if preference == "both":
        return True
    provider = _provider_for(model)
    return provider == preference


async def _load_briefing_block() -> str | None:
    """Load the researcher's current market briefing and render it as a
    compact <system_context>...</system_context> block. Returns None if no
    briefing exists yet (researcher hasn't run, or just booted)."""
    try:
        r = get_redis()
        h = await r.hgetall("knowledge_briefing:current")
        if not h:
            return None
        body = h.get("body")
        if not body:
            return None
        # Bound the size so we don't burn tokens on huge briefings.
        body_short = body if len(body) <= 3000 else body[:3000] + "...(truncated)"
        ts = h.get("ts", "")
        return (
            "<system_context source=\"researcher\" updated=\"" + ts + "\">\n"
            "Current market briefing — use as context for every decision below.\n"
            "" + body_short + "\n"
            "</system_context>"
        )
    except Exception:  # noqa: BLE001
        return None


async def _next_free_candidate(tried: set[str], *, relax_garbage: bool = False) -> str | None:
    """Walk free-model candidates in this strict priority order:
        Stage 1: FREE_FALLBACKS, filtered only by `tried` + PERMANENTLY_RETIRED +
                 24h hard-dead cache. Soft (garbage) cache is IGNORED at this
                 stage so we always prefer a known-good model over a random
                 obscure live-catalog model — even if the known-good one was
                 briefly rate-limited 5 min ago.
        Stage 2: live catalog (alphabetical), full filter — only used if every
                 curated model has been tried OR is hard-dead.
        Stage 3: live catalog, relax_garbage — fallback when even live is fully
                 filtered.

    Earlier shape walked Stage 1+2 together with full filter, which sent the
    chain through 12 obscure live-catalog models (dolphin-mistral / nvidia-
    nemotron / liquid-lfm-1.2b) before ever trying llama-3.3-70b — because
    all 5 curated FREE_FALLBACKS were in the 10-min garbage cache from
    earlier 429s.
    """
    # Read provider preference once per walk. "both" = no filter; otherwise
    # we skip candidates whose provider doesn't match.
    preference = await _free_provider_preference()
    # Stage 1: curated FREE_FALLBACKS, hard-dead filter only.
    for cand in FREE_FALLBACKS:
        if cand in tried:
            continue
        if cand in PERMANENTLY_RETIRED_FREE:
            continue
        if not _matches_provider_preference(cand, preference):
            continue
        if _provider_for(cand) in _UNREACHABLE_PROVIDERS:
            continue
        try:
            if await get_redis().sismember("llm:dead_free_models", cand):
                continue
        except Exception:  # noqa: BLE001
            pass
        return cand
    # Stage 2 + 3: live catalog
    try:
        live_raw = list(await get_redis().smembers("llm:free_catalog") or [])
    except Exception:  # noqa: BLE001
        live_raw = []
    live_sorted = sorted(live_raw)
    candidates: list[str] = []
    for cand in live_sorted:
        if cand not in FREE_FALLBACKS:
            candidates.append(cand)
    for cand in candidates:
        if cand in tried:
            continue
        if cand in PERMANENTLY_RETIRED_FREE:
            continue
        if not _matches_provider_preference(cand, preference):
            continue
        if _provider_for(cand) in _UNREACHABLE_PROVIDERS:
            continue
        if relax_garbage:
            # Only filter by the 24h hard-dead cache, not the 10-min garbage.
            try:
                if await get_redis().sismember("llm:dead_free_models", cand):
                    continue
            except Exception:  # noqa: BLE001
                pass
        else:
            if await _is_dead_free(cand):
                continue
        return cand
    return None

# Curated suggestions surfaced in the Settings → Models tab so the user
# can pick from a vetted list per agent without typing model strings.
MODEL_CATALOG_FREE: list[str] = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "deepseek/deepseek-r1:free",
    "deepseek/deepseek-chat:free",
    "qwen/qwen-2.5-72b-instruct:free",
    "nousresearch/hermes-3-llama-3.1-405b:free",
]
# Google AI Studio free pool — separate catalog because it uses a different
# API endpoint and different rate-limit structure. UI groups it separately.
MODEL_CATALOG_GOOGLE_FREE: list[str] = [
    "google-ai-studio/gemini-2.5-flash",
    "google-ai-studio/gemini-2.0-flash",
    "google-ai-studio/gemini-2.5-flash-lite",
    "google-ai-studio/gemini-2.5-pro",
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
    # Global default-paid override (set on /settings/?tab=models) wins over
    # the per-agent paid default when in paid / auto-paid mode and the agent
    # has no specific override.
    default_paid = (cfg.get("default_paid_model") or "").strip()
    free_pref = (cfg.get("default_free_provider") or "both").lower()
    def _free_default_for_agent() -> str:
        # If the operator pinned "google_ai_studio", reach for a Google model
        # even when AGENT_MODELS_FREE has an OpenRouter default for the agent.
        if free_pref == "google_ai_studio":
            return "google-ai-studio/gemini-2.5-flash"
        # AGENT_MODELS_FREE entries are OpenRouter models. Honor "openrouter".
        # For "both", we still pick the per-agent OpenRouter default; the chain
        # walk on failure will hop providers.
        return AGENT_MODELS_FREE.get(agent, FREE_FALLBACKS[0])
    if mode == "free":
        return _free_default_for_agent(), mode
    if mode == "auto":
        # If we recently hit 402, stay on free until the sticky pin expires.
        try:
            pinned = await redis.get("llm_config:auto_active_free")
        except Exception:  # noqa: BLE001
            pinned = None
        if pinned:
            return _free_default_for_agent(), "auto-free"
    paid = default_paid or AGENT_MODELS.get(agent, "anthropic/claude-haiku-4-5")
    return paid, mode


# Approximate prices (USD per 1M tokens) — updated by hand or via OpenRouter pricing API.
# These are estimates; OpenRouter's actual usage response is the source of truth when present.
MODEL_PRICES: dict[str, tuple[float, float]] = {
    "anthropic/claude-haiku-4-5":   (1.00, 5.00),
    "anthropic/claude-sonnet-4-6":  (3.00, 15.00),
    "openai/o3-mini":               (1.10, 4.40),
    "openai/gpt-4o-mini":           (0.15, 0.60),
}


# Provider prefix on a model string identifies which API to hit. Default
# (no prefix) is OpenRouter for backward compat — all existing strings like
# "anthropic/claude-haiku-4-5" or "deepseek/deepseek-chat:free" stay valid.
# New: "google-ai-studio/gemini-2.5-flash" routes to Google AI Studio.
PROVIDER_PREFIX_OPENROUTER = ""  # implicit / default
PROVIDER_PREFIX_GOOGLE_AI_STUDIO = "google-ai-studio/"


def _provider_for(model: str) -> str:
    """Return the provider key based on the model string's prefix."""
    if (model or "").startswith(PROVIDER_PREFIX_GOOGLE_AI_STUDIO):
        return "google_ai_studio"
    return "openrouter"


def _strip_provider_prefix(model: str) -> str:
    """Strip our internal provider prefix so the underlying API sees the
    bare model id it expects (Google AI Studio uses 'gemini-2.5-flash',
    not 'google-ai-studio/gemini-2.5-flash')."""
    if (model or "").startswith(PROVIDER_PREFIX_GOOGLE_AI_STUDIO):
        return model[len(PROVIDER_PREFIX_GOOGLE_AI_STUDIO):]
    return model


# Cached AsyncOpenAI clients, keyed by `(provider, api_key)` so a credential
# rotation invalidates only the rotated client, not all of them.
_OPENAI_CLIENT_CACHE: dict[tuple[str, str], AsyncOpenAI] = {}

# Providers we've already confirmed unconfigured in this process (no env var,
# no UI cred, or decrypt failed). Skipped by the chain walk so we don't keep
# walking to candidates that are guaranteed to RuntimeError.
_UNREACHABLE_PROVIDERS: set[str] = set()


def _is_provider_configured(provider: str) -> bool:
    """Cheap check: does the API key for `provider` exist (UI creds OR env)?
    Used by the chain walk to skip candidates whose provider isn't set up,
    so we don't burn an attempt on a guaranteed RuntimeError."""
    settings = get_settings()
    from plata.config import credentials as _creds
    try:
        v = _creds.get_sync(provider)
    except Exception:  # noqa: BLE001
        v = None
    if v:
        return True
    if provider == "google_ai_studio":
        return bool(settings.google_ai_studio_api_key)
    if provider == "openrouter":
        return bool(settings.openrouter_api_key)
    return False


async def _client_for(provider: str) -> AsyncOpenAI:
    """Build (and cache) the AsyncOpenAI client for a given provider.

    Async because UI-saved credentials live in Postgres — the in-process
    sync cache is empty in fresh agent containers, so we must do a real DB
    lookup the first time. credentials.get() handles its own caching with a
    TTL, so subsequent calls are cheap.
    """
    settings = get_settings()
    from plata.config import credentials as _creds
    if provider == "google_ai_studio":
        api_key = await _creds.get("google_ai_studio") or (
            settings.google_ai_studio_api_key.get_secret_value()
            if settings.google_ai_studio_api_key else None
        )
        base_url = settings.google_ai_studio_base_url
        if not api_key:
            raise RuntimeError("GOOGLE_AI_STUDIO_API_KEY not configured")
    else:  # openrouter (default)
        api_key = await _creds.get("openrouter") or (
            settings.openrouter_api_key.get_secret_value()
            if settings.openrouter_api_key else None
        )
        base_url = settings.openrouter_base_url
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY not configured")
    cached = _OPENAI_CLIENT_CACHE.get((provider, api_key))
    if cached is not None:
        return cached
    cli = AsyncOpenAI(api_key=api_key, base_url=base_url)
    # Drop any older cache entry for this provider (likely rotated key).
    for k in [k for k in _OPENAI_CLIENT_CACHE if k[0] == provider]:
        _OPENAI_CLIENT_CACHE.pop(k, None)
    _OPENAI_CLIENT_CACHE[(provider, api_key)] = cli
    return cli


def _client() -> AsyncOpenAI:
    """Back-compat sync shim — used only by legacy callers that build a
    one-off client outside of LLMClient. Uses sync env/UI-cache lookup.
    New async code should `await _client_for(provider)` instead."""
    settings = get_settings()
    from plata.config import credentials as _creds
    api_key = _creds.get_sync("openrouter") or (
        settings.openrouter_api_key.get_secret_value()
        if settings.openrouter_api_key else None
    )
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not configured")
    return AsyncOpenAI(api_key=api_key, base_url=settings.openrouter_base_url)


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
    # Free-tier calls are actually free. Don't synthesize a "conservative
    # high estimate" against the daily budget — that's how `llm_mode=free`
    # was hitting BudgetExceededError at $20/day despite zero real spend.
    if _is_free_model(model):
        return 0.0
    prices = MODEL_PRICES.get(model)
    if prices is None:
        # Unknown PAID model → conservative high estimate so it still counts
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
        # Don't build an AsyncOpenAI client here — _client_for() is awaited
        # per attempt and picks the right provider for the current model.
        # Eagerly building OpenRouter in __init__ failed when the only
        # configured key was Google AI Studio (UI-saved, not in env).
        self._openai = None
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
        # Prepend the current market briefing for decision-making agents
        # so every trade-related LLM call reflects the researcher's latest
        # synthesis (regime + narratives + sector outlook). Cheap — reads
        # one Redis hash. Skips silently if no briefing exists yet.
        if self.agent in BRIEFING_AWARE_AGENTS:
            briefing_block = await _load_briefing_block()
            if briefing_block:
                # Prepend (don't replace) — caller's system prompt still wins
                # for agent-specific instructions; briefing is context.
                messages = [
                    {"role": "system", "content": briefing_block},
                    *messages,
                ]
        # If the selected model is a `:free` one that we've previously
        # marked as permanently dead, walk to a healthy candidate up
        # front instead of consuming an attempt only to retry. Avoids
        # the 10-times-an-hour error spam pattern we just shipped a fix for.
        if _is_free_model(self.model) and await _is_dead_free(self.model):
            next_model = await _next_free_candidate({self.model})
            if next_model:
                _log.info("llm_skip_known_dead_free",
                          agent=self.agent,
                          from_model=self.model, to_model=next_model)
                self.model = next_model
        # Same idea for unreachable-provider models: if our starting model
        # is e.g. google-ai-studio/... but we have no Google key wired into
        # this process, swap to a reachable free candidate up front.
        if _provider_for(self.model) in _UNREACHABLE_PROVIDERS:
            next_model = await _next_free_candidate({self.model})
            if next_model:
                _log.info("llm_skip_unreachable_provider",
                          agent=self.agent,
                          from_model=self.model, to_model=next_model)
                self.model = next_model
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
        # Multi-provider: pick the AsyncOpenAI client whose base_url + key
        # match the current model's provider, and strip our internal prefix
        # so the upstream API sees the bare model id.
        async def _attempt_once():
            clean = {k: v for k, v in kwargs.items() if not k.startswith("_")}
            current_model = clean.get("model") or ""
            provider = _provider_for(current_model)
            try:
                client = await _client_for(provider)
            except RuntimeError as exc:
                # Provider not configured. Don't crash the agent — mark
                # the provider as unreachable for the rest of this process
                # and re-raise as a transient-looking error so the chain
                # walks to the next candidate.
                _UNREACHABLE_PROVIDERS.add(provider)
                _log.warning("llm_provider_unconfigured_skip",
                              provider=provider, model=current_model,
                              error=str(exc)[:160])
                raise RuntimeError(
                    f"provider_unreachable: {provider} — {exc}"
                ) from exc
            if provider == "google_ai_studio":
                clean["model"] = _strip_provider_prefix(current_model)
                # Google AI Studio's OpenAI-compat endpoint doesn't accept
                # OpenRouter's extra_body.usage.include — drop it.
                clean.pop("extra_body", None)
            return await client.chat.completions.create(**clean)

        response = None
        # Headroom needs to cover the free-fallback chain (6 models) PLUS
        # 3 actual retry attempts on the survivor model. Was range(3) — when
        # every free model 429'd, the loop ran out of attempts before reaching
        # one that worked and raised RuntimeError("LLM call returned no response").
        # Budget covers the curated chain + a sample of the live catalog
        # + 3 actual retries on the survivor model. Capped at 12 so a giant
        # OpenRouter catalog doesn't make one bad call take forever.
        try:
            live_size = await get_redis().scard("llm:free_catalog") or 0
        except Exception:  # noqa: BLE001
            live_size = 0
        # Budget = curated FREE_FALLBACKS (~5) + up to 2 live-catalog
        # bonus attempts + 3 retries on a survivor. Capped at 10. The
        # earlier `+ live_size` shape burned the whole budget on
        # obscure live-catalog models when curated were all in garbage
        # cache, with zero successes.
        max_attempts = min(10, len(FREE_FALLBACKS) + 2 + 3)
        consumed_retry = 0
        for _try in range(max_attempts):
            try:
                # Throttle per-model on free tier so we don't trip
                # OpenRouter's per-model RPM cap. Paid models pass through.
                await _throttle_free(kwargs.get("model") or "")
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
                # Free-pool failure classification (re-derived in v2.24.164):
                #   PERMANENT (mark dead 24h)
                #     - "unavailable for free" (OpenRouter explicit retirement)
                #     - "response_format is not supported" (model can't do
                #       structured output at all)
                #   TRANSIENT (10-min garbage cooldown; model may come back)
                #     - "no endpoints found" (provider pool is loaded RIGHT NOW)
                #     - 404 (could be misconfig OR temporary)
                #     - "rate limit" / "429"
                # Both walk FREE_FALLBACKS to the next non-tried non-dead model.
                current_model = kwargs.get("model") or ""
                is_free = _is_free_model(current_model)
                is_perm_unavail = is_free and (
                    "unavailable for free" in low
                    or "response_format is not supported" in low
                    or ("response_format" in low and "not supported" in low)
                )
                is_transient = is_free and (
                    "no endpoints found" in low
                    or "404" in msg
                    or "rate limit" in low
                    or "429" in msg
                    or "provider_unreachable" in msg
                )
                if is_perm_unavail:
                    await _mark_dead_free(current_model)
                elif is_transient:
                    # Short cooldown so we don't keep hitting the same
                    # overloaded model within minutes. Re-tries after 10 min.
                    try:
                        await get_redis().setex(
                            f"llm:garbage_producer:{current_model}", 600, "1"
                        )
                    except Exception:  # noqa: BLE001
                        pass
                if is_perm_unavail or is_transient:
                    tried = set(kwargs.setdefault("_tried_free", []))
                    tried.add(current_model)
                    next_model = await _next_free_candidate(tried)
                    # Second pass: if first walk exhausted (every other free
                    # model is in the 10-min garbage cooldown), retry one of
                    # them anyway. Better to attempt a recently-rate-limited
                    # model than raise "all exhausted" on a single-model
                    # Tried list, which was happening repeatedly when
                    # OpenRouter throttled the whole pool simultaneously.
                    if not next_model:
                        next_model = await _next_free_candidate(
                            tried, relax_garbage=True
                        )
                        if next_model:
                            _log.warning("llm_free_fallback_relaxed",
                                         agent=self.agent,
                                         to_model=next_model)
                    if next_model:
                        _log.warning("llm_free_fallback",
                                     from_model=current_model,
                                     to_model=next_model,
                                     reason=("perm_unavail" if is_perm_unavail
                                              else "rate_limited"))
                        kwargs["model"] = next_model
                        kwargs["_tried_free"] = list(tried)
                        self.model = next_model
                        continue
                    # Free chain exhausted. In auto mode → reach for paid (the
                    # whole point of auto: "use free when possible, else paid").
                    # In free mode → raise (user explicitly opted in).
                    try:
                        cfg = await get_redis().hgetall("llm_config") or {}
                    except Exception:  # noqa: BLE001
                        cfg = {}
                    mode = (cfg.get("mode") or "paid").lower()
                    if mode == "auto" and not kwargs.get("_paid_rescue_tried"):
                        paid = AGENT_MODELS.get(self.agent, "anthropic/claude-haiku-4-5")
                        if paid and paid != current_model:
                            _log.warning("llm_free_exhausted_paid_rescue",
                                         agent=self.agent,
                                         tried_free=list(tried),
                                         paid=paid)
                            kwargs["model"] = paid
                            kwargs["_paid_rescue_tried"] = True
                            self.model = paid
                            # Clear the sticky free pin so subsequent calls go
                            # to paid first rather than free.
                            try:
                                await get_redis().delete("llm_config:auto_active_free")
                            except Exception:  # noqa: BLE001
                                pass
                            continue
                    raise RuntimeError(
                        f"All free models exhausted while trying to serve "
                        f"agent={self.agent}. Tried: {sorted(tried)}. "
                        f"Mode={mode}. Switch llm_mode to `paid` on "
                        f"/settings/?tab=models if you have OpenRouter credits, "
                        f"or wait ~1h for free quotas / endpoints to recover."
                    ) from exc
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
                        # When OpenRouter credits hit 402, OpenRouter free
                        # tier is usually impacted too (shared per-account
                        # quota). Prefer Google AI Studio if its key is set;
                        # otherwise fall back to the agent's OpenRouter free.
                        free_model = AGENT_MODELS_FREE.get(
                            self.agent, FREE_FALLBACKS[0]
                        )
                        try:
                            from plata.config import credentials as _creds
                            if await _creds.get("google_ai_studio"):
                                free_model = "google-ai-studio/gemini-2.5-flash"
                        except Exception:  # noqa: BLE001
                            pass
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
                # Only count attempts that DIDN'T trigger a model swap as
                # "real" retries — otherwise the chain walk burns through
                # our budget instead of giving the new model a chance.
                consumed_retry += 1
                if consumed_retry >= 3:
                    raise
                # Honor Retry-After when the provider gives one (OpenRouter
                # surfaces it in the error metadata for 429s).
                retry_after = 0.0
                import re as _re_local
                ra = _re_local.search(r"retry_after_seconds[^0-9]+([0-9.]+)", msg)
                if ra:
                    try:
                        retry_after = min(60.0, float(ra.group(1)))
                    except ValueError:
                        retry_after = 0.0
                await asyncio.sleep(max(retry_after, 1 + consumed_retry * 2))
        if response is None:
            # Final paid-rescue: even if chain didn't formally "exhaust"
            # (each attempt found SOME next_model in the live catalog), the
            # 12-attempt budget burned out without a single success. In auto
            # mode, fall to paid before raising — same logic as the
            # mid-chain rescue branch above. Only do this once per call.
            try:
                cfg = await get_redis().hgetall("llm_config") or {}
            except Exception:  # noqa: BLE001
                cfg = {}
            mode = (cfg.get("mode") or "paid").lower()
            if mode == "auto" and not kwargs.get("_paid_rescue_tried"):
                paid = AGENT_MODELS.get(self.agent, "anthropic/claude-haiku-4-5")
                if paid:
                    _log.warning("llm_budget_exhausted_paid_rescue",
                                  agent=self.agent,
                                  tried_free=list(kwargs.get("_tried_free") or []),
                                  paid=paid)
                    kwargs["model"] = paid
                    kwargs["_paid_rescue_tried"] = True
                    self.model = paid
                    try:
                        await get_redis().delete("llm_config:auto_active_free")
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        response = await _attempt_once()
                    except Exception as exc:  # noqa: BLE001
                        raise RuntimeError(
                            f"LLM call returned no response after {max_attempts} "
                            f"free attempts AND paid rescue ({paid}) also failed: "
                            f"{type(exc).__name__}: {str(exc)[:200]}"
                        ) from exc
        if response is None:
            raise RuntimeError(
                f"LLM call returned no response after {max_attempts} attempts "
                f"(tried free models: {kwargs.get('_tried_free', [])}). "
                f"All free providers may be rate-limited; switch llm_mode to "
                f"paid on /settings/?tab=models if you have credits, or wait "
                f"~1 min for free quotas to reset."
            )

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
        # Structured output is fragile on cheap/free models. Failure modes:
        #   1. Tab/whitespace loops until max_tokens (no JSON closure)
        #   2. Chatty prose appended after JSON ("Note: truncated for brevity")
        #   3. response.choices is None/empty (SDK edge case)
        #   4. JSON parses but required keys are missing (model ignored schema)
        # All four → walk to a different free model, same machinery as 404/429.
        tried_garbage: set[str] = set()
        last_error: tuple[str, str] | None = None
        # Required-key inference: the JSON-schema "required" array tells us
        # which top-level fields MUST be present. A model that returns valid
        # JSON missing one of these is unusable — treat like loop garbage.
        required_keys: set[str] = set(schema.get("required") or [])
        for attempt in range(5):
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

            # 1. Defensive extraction — some providers return choices=None
            choices = getattr(response, "choices", None) or []
            if not choices:
                content = ""
                finish = "no_choices"
            else:
                first = choices[0]
                msg_obj = getattr(first, "message", None)
                content = (getattr(msg_obj, "content", None) or "") if msg_obj else ""
                finish = getattr(first, "finish_reason", None) or "?"

            # 2. Try parsing as-is, then with prose-stripping fallback
            parsed: dict | None = None
            parse_err: Exception | None = None
            try:
                parsed = json.loads(content) if content else None
            except json.JSONDecodeError as exc:
                parse_err = exc
                # Some models add commentary after JSON: try to extract the
                # outermost {...} block.
                stripped = _extract_first_json_object(content)
                if stripped:
                    try:
                        parsed = json.loads(stripped)
                        parse_err = None
                    except json.JSONDecodeError:
                        pass

            # 3. Schema-shape check: required keys present?
            schema_ok = True
            missing_keys: list[str] = []
            if parsed is not None and required_keys:
                missing_keys = sorted(required_keys - set(parsed.keys()))
                schema_ok = not missing_keys

            if parsed is not None and schema_ok:
                return parsed

            # Failure — classify and switch model.
            tail = (content or "")[-200:].replace("\n", " ")
            why = (
                f"missing_keys={missing_keys}" if (parsed is not None and not schema_ok)
                else f"json_error={parse_err.msg if parse_err else 'no_content'}"
            )
            last_error = (finish, f"{why} | tail={tail!r}")
            # If the response hit max_tokens (finish_reason="length"), the
            # model wasn't producing garbage — it just ran out of room. Bump
            # max_tokens and retry with the SAME model before falling back.
            if finish == "length" and max_tokens < 8192:
                new_max = min(8192, max_tokens * 2)
                _log.warning("llm_structured_truncated_bump_tokens",
                              agent=self.agent, model=self.model,
                              old_max=max_tokens, new_max=new_max)
                max_tokens = new_max
                continue
            if self.model not in tried_garbage:
                try:
                    await get_redis().setex(
                        f"llm:garbage_producer:{self.model}", 600, "1"
                    )
                except Exception:  # noqa: BLE001
                    pass
                tried_garbage.add(self.model)
                next_model = await _next_free_candidate(tried_garbage | {self.model})
                if next_model:
                    _log.warning("llm_structured_garbage_switch_model",
                                  agent=self.agent, finish=finish,
                                  reason=why,
                                  from_model=self.model, to_model=next_model)
                    self.model = next_model
                    continue
            # Same model again or chain exhausted
            raise RuntimeError(
                f"LLM structured response was not valid (finish_reason={finish}, "
                f"reason={why})"
            )
        finish, tail = last_error or ("?", "")
        raise RuntimeError(
            f"LLM structured response was not valid after 5 model swaps "
            f"(finish_reason={finish}, last_error={tail}). Free pool may be "
            f"degraded; switch llm_mode to paid on /settings/?tab=models."
        )
