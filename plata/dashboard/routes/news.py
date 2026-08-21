"""News pipeline editor — sources, RSS feeds, Telegram channels, content filters.

Lives under the Knowledge sidebar group.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from plata.agents.scraper.news_config import (
    DEFAULTS as NEWS_DEFAULTS,
)
from plata.agents.scraper.news_config import (
    get_config as get_news_config,
)
from plata.agents.scraper.news_config import (
    save_config as save_news_config,
)
from plata.core.bus import get_redis
from plata.dashboard import templates

router = APIRouter(prefix="/news", tags=["news"])


SOURCE_NAMES = ["gdelt", "reddit", "cryptopanic", "rss", "market_ticker"]


async def _diagnose(name: str, h: dict, cfg: dict, now_ts: float,
                     scraper_status: dict, settings_obj) -> tuple[str, str]:
    """Return (severity, message) — '' severity means healthy."""
    # 1. Scraper agent stale or never booted?
    scraper_hb = scraper_status.get("last_heartbeat")
    if scraper_hb:
        from datetime import datetime as _dt
        try:
            age = (_dt.now().astimezone() - _dt.fromisoformat(scraper_hb)).total_seconds()
            if age > 180:
                return ("error", f"Scraper agent heartbeat is {int(age // 60)}m old — "
                        "the ingestion_hub container is probably dead. "
                        "Restart it on Railway; no source will poll until then.")
        except Exception:  # noqa: BLE001
            pass
    elif scraper_status:
        return ("error", "Scraper agent has never heartbeated since boot — its "
                "background loop may have crashed before reaching the poll routine. "
                "Check /errors/.")

    # 2. Last poll too long ago? (interval × 3 is generous slack for jitter)
    last_poll = h.get("last_poll_at")
    interval = int(h.get("interval_sec") or 0)
    if last_poll and interval:
        from datetime import datetime as _dt
        try:
            age = (_dt.now().astimezone() - _dt.fromisoformat(last_poll)).total_seconds()
            if age > interval * 3 and age > 600:
                hrs = int(age // 3600); mins = int((age % 3600) // 60)
                return ("error", f"Last poll was {hrs}h{mins}m ago — interval is "
                        f"{interval}s, so this should have polled multiple times "
                        "by now. Scraper task is wedged or the container restarted "
                        "without re-starting the poll loops.")
        except Exception:  # noqa: BLE001
            pass

    # 3. Source-specific prerequisites
    # Reddit no longer needs an OAuth app since v2.24.198. The credential
    # check that used to live here has been removed.
    if name == "rss":
        feeds = cfg.get("rss_feeds") or []
        if not feeds:
            return ("warn", "No RSS feeds configured. Add at least one feed in the "
                    "'RSS feeds' section below (format: 'Name | https://feed/url'), "
                    "then save.")

    # 4. Disabled in config
    enabled_key = f"{name}_enabled"
    if enabled_key in cfg and not cfg.get(enabled_key):
        return ("info", "This source is disabled in the Sources panel below. "
                "Enable the checkbox + Save to start polling.")

    # 5. Last error?
    last_err = h.get("last_error") or ""
    if last_err:
        return ("warn", f"Last poll errored: {last_err}")

    # 6. Polled but raw=0 consistently
    polls = int(h.get("lifetime_polls") or 0)
    raw = int(h.get("lifetime_raw") or 0)
    if polls >= 5 and raw == 0:
        if name == "gdelt":
            return ("warn", "GDELT has run 5+ times and returned zero articles — "
                    "either the query is too narrow or you're getting rate-limited. "
                    "Edit the GDELT query in the panel below.")
        return ("warn", f"Source has run {polls} times with zero results — the "
                "upstream API may be returning empty for the current query/config.")

    return ("", "")


async def _source_rows(redis) -> list[dict]:
    import time

    from plata.config.settings import get_settings as _gs
    settings_obj = _gs()
    cfg = await get_news_config()
    scraper_status = await redis.hgetall("agent_status:scraper") or {}
    rows = []
    now_ts = time.time()
    for name in SOURCE_NAMES:
        h = await redis.hgetall(f"scraper:source:{name}") or {}
        probe = await redis.hgetall(f"scraper:source:{name}:probe") or {}
        next_poll_at = h.get("next_poll_at")
        seconds_until = None
        if next_poll_at:
            try:
                seconds_until = int(next_poll_at) - int(now_ts)
            except ValueError:
                pass
        severity, message = await _diagnose(name, h, cfg, now_ts, scraper_status, settings_obj)
        rows.append({
            "name": name,
            "status": h.get("status") or "—",
            "last_poll_at": h.get("last_poll_at"),
            "last_fetched": h.get("last_fetched"),
            "last_published": h.get("last_published"),
            "last_dup": h.get("last_dup"),
            "last_filtered": h.get("last_filtered"),
            "last_filtered_reasons": h.get("last_filtered_reasons"),
            "last_error": h.get("last_error"),
            "interval_sec": h.get("interval_sec"),
            "seconds_until_next": seconds_until,
            "run_now_pending": h.get("run_now") == "1",
            "lifetime_raw": h.get("lifetime_raw") or "0",
            "lifetime_published": h.get("lifetime_published") or "0",
            "lifetime_dup": h.get("lifetime_dup") or "0",
            "lifetime_filtered": h.get("lifetime_filtered") or "0",
            "lifetime_polls": h.get("lifetime_polls") or "0",
            "diagnose_severity": severity,
            "diagnose_message": message,
            "probe": probe,
        })
    return rows


@router.get("/source/{name}/log", response_class=HTMLResponse)
async def source_log(name: str, request: Request):
    """Last 20 polls for a single source — what came in, what got dropped, why."""
    if name not in SOURCE_NAMES:
        return RedirectResponse(url="/news/", status_code=303)
    redis = get_redis()
    import json as _json
    raw = await redis.lrange(f"scraper:source:{name}:log", 0, 19) or []
    entries = []
    for r in raw:
        try:
            entries.append(_json.loads(r))
        except Exception:  # noqa: BLE001
            continue
    h = await redis.hgetall(f"scraper:source:{name}") or {}
    return templates.TemplateResponse(
        request, "pages/news_source_log.html",
        {"active": "news", "source_name": name, "entries": entries, "stats": h},
    )


async def _telegram_user_panel(redis, cfg: dict) -> dict:
    """State for the 'Public signal groups (user account)' section.

    Written by the TelegramUserListener agent (plata/agents/scraper/telegram_user.py)
    and the login routes below.
    """
    import json as _json

    from plata.config import credentials as _creds
    api_ok = bool(await _creds.get("telegram_api_id") and await _creds.get("telegram_api_hash"))
    session_ok = bool(await _creds.get("telegram_user_session"))
    info = await redis.hgetall("telegram:user_info") or {}
    join_raw = await redis.hgetall("telegram:user_join_status") or {}
    joins = {}
    for k, v in join_raw.items():
        try:
            joins[k] = _json.loads(v)
        except _json.JSONDecodeError:
            continue
    channels = []
    for link in cfg.get("telegram_user_channels") or []:
        st = joins.get(link) or {}
        channels.append({
            "link": link,
            "status": st.get("status") or "pending",
            "title": st.get("title") or link,
            "error": st.get("error") or "",
            "msg_count": st.get("msg_count") or 0,
            "last_msg_at": st.get("last_msg_at"),
        })
    return {"api_ok": api_ok, "session_ok": session_ok, "info": info,
            "channels": channels, "login_pending": _login_state.get("phone") is not None}


async def _telegram_panel(redis, cfg: dict) -> dict:
    """Bot identity + known chats for the 'Telegram groups & channels' panel.

    Both keys are written by the telegram_bot agent (plata/hitl/telegram_bot.py):
    bot info on startup, known chats on membership updates / incoming messages.
    """
    import json as _json
    bot_info = await redis.hgetall("telegram:bot_info") or {}
    raw = await redis.hgetall("telegram:known_chats") or {}
    listen_ids = {int(i) for i in (cfg.get("telegram_channel_ids") or [])}
    chats = []
    for cid, blob in raw.items():
        try:
            e = _json.loads(blob)
            e["id"] = int(e.get("id", cid))
        except (ValueError, TypeError, _json.JSONDecodeError):
            continue
        e["gone"] = e.get("status") in ("left", "kicked")
        e["listening"] = e["id"] in listen_ids
        chats.append(e)
    chats.sort(key=lambda e: (e["gone"], str(e.get("title") or "").lower()))
    return {"bot": bot_info, "chats": chats, "listen_ids": listen_ids}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    redis = get_redis()
    cfg = await get_news_config()
    drops = await redis.hgetall("scraper:filter_drops") or {}
    sources = await _source_rows(redis)
    telegram = await _telegram_panel(redis, cfg)
    telegram_user = await _telegram_user_panel(redis, cfg)
    return templates.TemplateResponse(
        request, "pages/news.html",
        {"active": "news", "news_cfg": cfg, "news_drops": drops,
         "news_defaults": NEWS_DEFAULTS, "sources": sources,
         "telegram": telegram, "telegram_user": telegram_user},
    )


# ---------------------------------------------------------------------------
# Telegram USER-account (MTProto) login + public-channel management.
#
# Bots can't join public groups the operator doesn't admin, so signal-group
# ingestion runs on a logged-in user account (Telethon). The login handshake
# (phone -> SMS/app code -> optional 2FA password) happens right here in the
# dashboard; the resulting StringSession is stored in the encrypted
# credentials store where the TelegramUserListener agent picks it up.
#
# _login_state holds the half-open Telethon client between send_code and
# sign_in. In-memory is fine: the dashboard is a single process, and if it
# restarts mid-login the user just requests a new code.
# ---------------------------------------------------------------------------

_login_state: dict = {}


async def _login_client():
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    from plata.config import credentials as _creds
    api_id = await _creds.get("telegram_api_id")
    api_hash = await _creds.get("telegram_api_hash")
    if not (api_id and api_hash):
        return None
    client = TelegramClient(StringSession(), int(api_id), api_hash)
    await client.connect()
    return client


@router.post("/telegram/user/setup")
async def telegram_user_setup(request: Request):
    """Store api_id + api_hash from my.telegram.org in the credentials store."""
    from plata.config import credentials as _creds
    from plata.dashboard.auth import current_user_email
    form = await request.form()
    api_id = (form.get("api_id") or "").strip()
    api_hash = (form.get("api_hash") or "").strip()
    if not api_id.isdigit() or not api_hash:
        return JSONResponse({"ok": False, "error": "api_id must be a number and api_hash non-empty."})
    by = current_user_email(request)
    await _creds.set_("telegram_api_id", api_id, by=by)
    await _creds.set_("telegram_api_hash", api_hash, by=by)
    return JSONResponse({"ok": True})


@router.post("/telegram/user/send_code")
async def telegram_user_send_code(request: Request):
    form = await request.form()
    phone = (form.get("phone") or "").strip().replace(" ", "")
    if not phone.startswith("+"):
        return JSONResponse({"ok": False, "error": "Phone must be international format, e.g. +9725…"})
    old = _login_state.pop("client", None)
    if old is not None:
        import contextlib
        with contextlib.suppress(Exception):
            await old.disconnect()
    _login_state.clear()
    try:
        client = await _login_client()
        if client is None:
            return JSONResponse({"ok": False, "error": "Set API ID + hash first."})
        sent = await client.send_code_request(phone)
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)[:200]})
    _login_state.update({"client": client, "phone": phone,
                         "phone_code_hash": sent.phone_code_hash})
    return JSONResponse({"ok": True})


@router.post("/telegram/user/sign_in")
async def telegram_user_sign_in(request: Request):
    from telethon.errors import SessionPasswordNeededError

    from plata.config import credentials as _creds
    from plata.dashboard.auth import current_user_email
    form = await request.form()
    code = (form.get("code") or "").strip()
    password = (form.get("password") or "").strip()
    client = _login_state.get("client")
    if client is None:
        return JSONResponse({"ok": False, "error": "No login in progress — send a code first."})
    try:
        try:
            await client.sign_in(
                phone=_login_state["phone"], code=code,
                phone_code_hash=_login_state["phone_code_hash"],
            )
        except SessionPasswordNeededError:
            if not password:
                return JSONResponse({"ok": False, "error": "2FA enabled — enter your Telegram password too."})
            await client.sign_in(password=password)
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)[:200]})
    session_str = client.session.save()
    await _creds.set_("telegram_user_session", session_str,
                      by=current_user_email(request))
    import contextlib
    with contextlib.suppress(Exception):
        await client.disconnect()
    _login_state.clear()
    return JSONResponse({"ok": True})


@router.post("/telegram/user/disconnect")
async def telegram_user_disconnect():
    """Invalidate + forget the stored user session."""
    import contextlib

    from telethon.sessions import StringSession

    from plata.config import credentials as _creds
    session_str = await _creds.get("telegram_user_session")
    if session_str:
        # Best-effort remote logout so the session doesn't linger in the
        # account's device list.
        with contextlib.suppress(Exception):
            from telethon import TelegramClient
            api_id = await _creds.get("telegram_api_id")
            api_hash = await _creds.get("telegram_api_hash")
            client = TelegramClient(StringSession(session_str), int(api_id), api_hash)
            await client.connect()
            await client.log_out()
    await _creds.delete("telegram_user_session")
    redis = get_redis()
    await redis.delete("telegram:user_info")
    await redis.delete("telegram:user_join_status")
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/telegram/user/channels/add")
async def telegram_user_channel_add(request: Request):
    from plata.agents.scraper.telegram_user import _normalize_link
    form = await request.form()
    link = _normalize_link(form.get("link") or "")
    if not link:
        return JSONResponse({"ok": False, "error": "Paste a t.me link or @username."})
    cfg = await get_news_config()
    links = list(cfg.get("telegram_user_channels") or [])
    if link not in links:
        links.append(link)
        await save_news_config({"telegram_user_channels": links})
    return JSONResponse({"ok": True})


@router.post("/telegram/user/channels/remove")
async def telegram_user_channel_remove(request: Request):
    form = await request.form()
    link = (form.get("link") or "").strip()
    cfg = await get_news_config()
    links = [c for c in (cfg.get("telegram_user_channels") or []) if c != link]
    await save_news_config({"telegram_user_channels": links})
    await get_redis().hdel("telegram:user_join_status", link)
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/telegram/chat/{chat_id}/toggle")
async def telegram_chat_toggle(chat_id: int):
    """One-click listen/unlisten for a chat the bot is a member of.

    Adding a chat also flips telegram_channels_enabled on — clicking 👂 Listen
    means 'ingest this chat', no second switch to remember.
    """
    cfg = await get_news_config()
    ids = [int(i) for i in (cfg.get("telegram_channel_ids") or [])]
    updates: dict = {}
    if chat_id in ids:
        ids = [i for i in ids if i != chat_id]
    else:
        ids.append(chat_id)
        if not cfg.get("telegram_channels_enabled"):
            updates["telegram_channels_enabled"] = True
    updates["telegram_channel_ids"] = ids
    await save_news_config(updates)
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/telegram/chat/{chat_id}/forget")
async def telegram_chat_forget(chat_id: int):
    """Drop a chat from the known list (and from the listen list)."""
    redis = get_redis()
    await redis.hdel("telegram:known_chats", str(chat_id))
    cfg = await get_news_config()
    ids = [int(i) for i in (cfg.get("telegram_channel_ids") or []) if int(i) != chat_id]
    await save_news_config({"telegram_channel_ids": ids})
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/source/{name}/run_now")
async def run_now(name: str):
    if name not in SOURCE_NAMES:
        return RedirectResponse(url="/news/", status_code=303)
    redis = get_redis()
    await redis.hset(f"scraper:source:{name}", "run_now", "1")
    # Also flip status off "halted" if user is forcing a run.
    if (await redis.hget(f"scraper:source:{name}", "status")) == "halted":
        await redis.hset(f"scraper:source:{name}", "status", "idle")
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/source/{name}/toggle")
async def toggle_source(name: str):
    """Halt/resume a single source from the news page."""
    if name not in SOURCE_NAMES:
        return RedirectResponse(url="/news/", status_code=303)
    redis = get_redis()
    cur = (await redis.hget(f"scraper:source:{name}", "status")) or "idle"
    new = "halted" if cur != "halted" else "idle"
    await redis.hset(f"scraper:source:{name}", "status", new)
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/save")
async def save(request: Request):
    form = await request.form()
    updates: dict = {}
    # Sweep ALL boolean keys defined in DEFAULTS so newly-added sources
    # (sec_edgar_enabled, hackernews_enabled, etc. added in v2.24.198)
    # automatically save without needing to be hand-listed here.
    for k, default in NEWS_DEFAULTS.items():
        if isinstance(default, bool):
            updates[k] = (form.get(k) == "on")
    if "gdelt_query" in form:
        updates["gdelt_query"] = (form.get("gdelt_query") or "").strip()
    if "min_title_len" in form:
        try:
            updates["min_title_len"] = int(form.get("min_title_len") or 0)
        except ValueError:
            pass

    def _lines(name: str) -> list[str]:
        raw = (form.get(name) or "").strip()
        return [ln.strip() for ln in raw.splitlines() if ln.strip()] if raw else []

    if "reddit_subreddits" in form:
        updates["reddit_subreddits"] = _lines("reddit_subreddits")
    if "market_ticker_threshold_pct" in form:
        try:
            updates["market_ticker_threshold_pct"] = float(form.get("market_ticker_threshold_pct") or 3.0)
        except ValueError:
            pass
    if "market_ticker_window_min" in form:
        try:
            updates["market_ticker_window_min"] = int(form.get("market_ticker_window_min") or 60)
        except ValueError:
            pass
    if "market_ticker_crypto_ids" in form:
        updates["market_ticker_crypto_ids"] = _lines("market_ticker_crypto_ids")
    if "market_ticker_stock_symbols" in form:
        updates["market_ticker_stock_symbols"] = [s.upper() for s in _lines("market_ticker_stock_symbols")]
    if "require_keywords" in form:
        updates["require_keywords"] = _lines("require_keywords")
    if "block_keywords" in form:
        updates["block_keywords"] = _lines("block_keywords")
    if "telegram_channel_ids" in form:
        ids = []
        for ln in _lines("telegram_channel_ids"):
            try:
                ids.append(int(ln))
            except ValueError:
                pass
        updates["telegram_channel_ids"] = ids
    if "rss_feeds" in form:
        feeds = []
        for ln in _lines("rss_feeds"):
            if "|" in ln:
                name, url = ln.split("|", 1)
                feeds.append({"name": name.strip(), "url": url.strip(), "enabled": True})
            else:
                feeds.append({"name": ln, "url": ln, "enabled": True})
        updates["rss_feeds"] = feeds
    await save_news_config(updates)
    return RedirectResponse(url="/news/", status_code=303)


@router.post("/filter_drops/reset")
async def reset_drops():
    redis = get_redis()
    await redis.delete("scraper:filter_drops")
    return RedirectResponse(url="/news/", status_code=303)
