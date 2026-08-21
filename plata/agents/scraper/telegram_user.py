"""Telegram USER-account listener (MTProto via Telethon).

The bot API can't get Plata into public signal groups the operator doesn't
admin: bots can never join a chat by themselves, and only a group's admins can
add one. A logged-in USER account (same protocol as the official apps) can
join any public group/channel on its own — so this agent runs alongside the
bot and covers exactly that case.

Everything is driven from the dashboard (/news/), no restarts needed:
  credentials  telegram_api_id / telegram_api_hash / telegram_user_session
               (encrypted credentials store; session produced by the /news/
               phone+code login flow)
  config       news_config.telegram_user_channels — list of @usernames /
               t.me links to follow; the agent joins missing ones itself
  status out   telegram:user_info         — hash: logged-in account identity
               telegram:user_join_status  — hash link -> JSON {status, chat_id,
                                            title, error, ts, msg_count}

Only messages from chats in the followed list are ingested — the account's
personal DMs/groups are never read. Joins are throttled to one new channel
per sync cycle; FloodWait from Telegram is respected and surfaced in the UI.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from typing import Any

from plata.agents.base import BaseAgent, log_action
from plata.core.bus import get_redis
from plata.core.observability import get_logger

_log = get_logger("telegram_user")

USER_INFO_KEY = "telegram:user_info"
JOIN_STATUS_KEY = "telegram:user_join_status"
SYNC_INTERVAL_SEC = 60


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_link(raw: str) -> str:
    """'https://t.me/foo' / 't.me/foo' / '@foo' / 'foo' -> canonical form.

    Public chats normalize to '@username'; invite links (t.me/+hash or
    t.me/joinchat/hash) keep their hash prefixed with '+'.
    """
    s = (raw or "").strip()
    for prefix in ("https://", "http://"):
        s = s.removeprefix(prefix)
    s = s.removeprefix("t.me/").removeprefix("telegram.me/")
    s = s.removeprefix("joinchat/")
    s = s.rstrip("/")
    if not s:
        return ""
    if s.startswith("+"):
        return s
    return "@" + s.lstrip("@")


class TelegramUserListener(BaseAgent):
    name = "telegram_user"

    async def run(self) -> None:
        await asyncio.gather(
            self._listener_loop(),
            self._heartbeat_loop(),
        )

    async def handle(self, payload):  # not used; run() is overridden
        return None

    # ------------------------------------------------------------------
    async def _listener_loop(self) -> None:
        """Outer loop: (re)connect whenever credentials appear or change."""
        current_session: str | None = None
        client = None
        while True:
            try:
                creds = await self._get_creds()
                if creds is None:
                    if client is not None:
                        await self._disconnect(client)
                        client = None
                        current_session = None
                    await self._set_state("unconfigured")
                    await asyncio.sleep(30)
                    continue
                api_id, api_hash, session_str = creds
                if client is None or session_str != current_session:
                    if client is not None:
                        await self._disconnect(client)
                    client = await self._connect(api_id, api_hash, session_str)
                    if client is None:
                        current_session = None
                        await asyncio.sleep(30)
                        continue
                    current_session = session_str
                await self._sync_channels(client)
                await asyncio.sleep(SYNC_INTERVAL_SEC)
            except asyncio.CancelledError:
                if client is not None:
                    await self._disconnect(client)
                raise
            except Exception as exc:  # noqa: BLE001
                _log.exception("telegram_user_loop_error")
                await self._set_state("error", error=str(exc)[:200])
                if client is not None:
                    await self._disconnect(client)
                    client = None
                    current_session = None
                await asyncio.sleep(30)

    async def _get_creds(self) -> tuple[int, str, str] | None:
        from plata.config import credentials as creds
        api_id = await creds.get("telegram_api_id")
        api_hash = await creds.get("telegram_api_hash")
        session_str = await creds.get("telegram_user_session")
        if not (api_id and api_hash and session_str):
            return None
        try:
            return int(api_id), api_hash, session_str
        except ValueError:
            await self._set_state("error", error="telegram_api_id is not a number")
            return None

    async def _connect(self, api_id: int, api_hash: str, session_str: str):
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession

        client = TelegramClient(StringSession(session_str), api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await self._set_state(
                "session_invalid",
                error="Stored session is no longer authorized — reconnect from /news/.",
            )
            await self._disconnect(client)
            return None
        me = await client.get_me()
        await get_redis().hset(USER_INFO_KEY, mapping={
            "id": str(me.id),
            "username": me.username or "",
            "name": " ".join(p for p in (me.first_name, me.last_name) if p),
            "phone": me.phone or "",
            "state": "connected",
            "error": "",
            "updated_at": _now(),
        })
        client.add_event_handler(self._on_message, events.NewMessage(incoming=True))
        await log_action(self.name, f"Connected as {me.first_name or me.username} — listening")
        return client

    async def _disconnect(self, client) -> None:
        with contextlib.suppress(Exception):
            await client.disconnect()

    async def _set_state(self, state: str, *, error: str = "") -> None:
        with contextlib.suppress(Exception):
            await get_redis().hset(USER_INFO_KEY, mapping={
                "state": state, "error": error, "updated_at": _now(),
            })

    # ------------------------------------------------------------------
    async def _sync_channels(self, client) -> None:
        """Join channels listed in config that we're not in yet (one per cycle)."""
        from plata.agents.scraper.news_config import get_config
        cfg = await get_config()
        wanted = [n for n in (_normalize_link(c) for c in cfg.get("telegram_user_channels") or []) if n]
        redis = get_redis()
        raw = await redis.hgetall(JOIN_STATUS_KEY) or {}
        status: dict[str, dict] = {}
        for k, v in raw.items():
            with contextlib.suppress(Exception):
                status[k] = json.loads(v)
        # Drop status rows for links removed from config.
        for stale in set(status) - set(wanted):
            await redis.hdel(JOIN_STATUS_KEY, stale)
        for link in wanted:
            st = status.get(link) or {}
            if st.get("status") == "joined":
                continue
            until = st.get("retry_after_epoch") or 0
            if until and datetime.now(UTC).timestamp() < float(until):
                continue  # FloodWait not over yet
            await self._join_one(client, link)
            break  # at most one join attempt per cycle — Telegram flood-limits joins

    async def _join_one(self, client, link: str) -> None:
        from telethon.errors import (
            FloodWaitError,
            InviteHashExpiredError,
            InviteHashInvalidError,
            UserAlreadyParticipantError,
        )
        from telethon.tl.functions.channels import JoinChannelRequest
        from telethon.tl.functions.messages import ImportChatInviteRequest

        redis = get_redis()
        entry: dict[str, Any] = {"link": link, "ts": _now()}
        try:
            if link.startswith("+"):  # private invite link
                try:
                    result = await client(ImportChatInviteRequest(link[1:]))
                    chat = result.chats[0] if result.chats else None
                except UserAlreadyParticipantError:
                    chat = await client.get_entity(link)
            else:
                chat = await client.get_entity(link)
                await client(JoinChannelRequest(chat))
            entry.update({
                "status": "joined",
                "chat_id": getattr(chat, "id", None),
                "title": getattr(chat, "title", None) or link,
            })
            await log_action(self.name, f"Joined {entry['title']} ({link})")
        except FloodWaitError as exc:
            entry.update({
                "status": "flood_wait",
                "error": f"Telegram rate limit — retrying in {exc.seconds}s",
                "retry_after_epoch": datetime.now(UTC).timestamp() + exc.seconds,
            })
            _log.warning("join_flood_wait", link=link, seconds=exc.seconds)
        except (InviteHashExpiredError, InviteHashInvalidError):
            entry.update({"status": "error", "error": "Invite link is invalid or expired"})
        except Exception as exc:  # noqa: BLE001
            entry.update({"status": "error", "error": str(exc)[:200]})
            _log.warning("join_failed", link=link, error=str(exc)[:200])
        await redis.hset(JOIN_STATUS_KEY, link, json.dumps(entry))

    # ------------------------------------------------------------------
    async def _followed_chat_ids(self) -> set[int]:
        redis = get_redis()
        raw = await redis.hgetall(JOIN_STATUS_KEY) or {}
        ids: set[int] = set()
        for v in raw.values():
            with contextlib.suppress(Exception):
                e = json.loads(v)
                if e.get("status") == "joined" and e.get("chat_id") is not None:
                    ids.add(int(e["chat_id"]))
        return ids

    async def _on_message(self, event) -> None:
        """Ingest a message from a followed chat into the signal pipeline."""
        try:
            chat = await event.get_chat()
            # Telethon chat ids are positive for channels; normalize both ways.
            chat_id = getattr(chat, "id", None)
            if chat_id is None or chat_id not in await self._followed_chat_ids():
                return
            body = (event.raw_text or "").strip()
            if not body:
                return
            from plata.core.bus import Streams, publish
            from plata.core.schemas import RawSignal, SignalSource
            username = getattr(chat, "username", None)
            link = (
                f"https://t.me/{username}/{event.id}"
                if username else f"tg://user_listener/{chat_id}/{event.id}"
            )
            sig = RawSignal(
                source=SignalSource.TELEGRAM,
                url=link,
                title=body.splitlines()[0][:300],
                body=body[:4000],
                source_published_at=event.date,
                metadata={
                    "chat_id": chat_id,
                    "chat_title": getattr(chat, "title", None),
                    "message_id": event.id,
                    "via": "user_account",
                },
            )
            await publish(Streams.RAW_SIGNALS, sig)
            await self._bump_msg_count(chat_id)
        except Exception:  # noqa: BLE001
            _log.exception("user_listener_ingest_failed")

    async def _bump_msg_count(self, chat_id: int) -> None:
        """Per-channel ingest counter so /news/ can show 'it's actually working'."""
        with contextlib.suppress(Exception):
            redis = get_redis()
            raw = await redis.hgetall(JOIN_STATUS_KEY) or {}
            for k, v in raw.items():
                e = json.loads(v)
                if e.get("chat_id") == chat_id:
                    e["msg_count"] = int(e.get("msg_count") or 0) + 1
                    e["last_msg_at"] = _now()
                    await redis.hset(JOIN_STATUS_KEY, k, json.dumps(e))
                    return
