"""Telegram bot: HITL inline-keyboard approvals + slash commands + alerts."""
from __future__ import annotations

import asyncio
import json
from typing import Any

from plata.agents.base import BaseAgent, log_action
from plata.config.settings import get_settings
from plata.core.bus import Channels, get_redis, publish_channel, subscribe
from plata.core.observability import get_logger
from plata.hitl.approval_store import list_pending, resolve

_log = get_logger("telegram_bot")


class TelegramBot(BaseAgent):
    name = "telegram_bot"

    async def run(self) -> None:
        settings = get_settings()
        if not settings.telegram_bot_token:
            self.log.warning("telegram_token_missing_bot_disabled")
            await super().run()
            return

        from telegram import (
            InlineKeyboardButton,
            InlineKeyboardMarkup,
            KeyboardButton,
            ReplyKeyboardMarkup,
            Update,
        )
        from telegram.ext import (
            ApplicationBuilder,
            CallbackQueryHandler,
            CommandHandler,
            ContextTypes,
            MessageHandler,
            filters,
        )

        MENU = ReplyKeyboardMarkup(
            [
                [KeyboardButton("/status"), KeyboardButton("/positions")],
                [KeyboardButton("/halt"), KeyboardButton("/resume")],
                [KeyboardButton("/paper on"), KeyboardButton("/paper off")],
                [KeyboardButton("/help")],
            ],
            resize_keyboard=True,
        )

        token = settings.telegram_bot_token.get_secret_value()
        allowed = settings.allowed_telegram_ids
        app = ApplicationBuilder().token(token).build()

        def _gated(handler):
            async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
                uid = update.effective_user.id if update.effective_user else None
                if allowed and uid not in allowed:
                    if update.effective_message:
                        await update.effective_message.reply_text("Unauthorized.")
                    await log_action(self.name, f"Rejected unauthorized user {uid}", kind="err")
                    return
                # Log the inbound command/text for visibility
                text = ""
                if update.effective_message and update.effective_message.text:
                    text = update.effective_message.text[:80]
                if text:
                    await log_action(self.name, f"Received: {text} (user {uid})")
                await handler(update, ctx)
            return wrapper

        HELP_TEXT = (
            "Plata bot ready.\n\n"
            "Commands:\n"
            "/status — system state + pending approvals\n"
            "/halt — emergency halt all agents\n"
            "/resume — resume agents after halt\n"
            "/paper on|off — toggle paper trading mode\n"
            "/positions — last reported executor state\n"
            "/joininfo — channel-ingestion setup help (run inside the target chat)\n"
            "/help — this message\n\n"
            "Trade proposals will arrive here with Approve/Reject buttons."
        )

        @_gated
        async def cmd_start(update, _):
            uid = update.effective_user.id if update.effective_user else "?"
            await update.message.reply_text(
                f"Hi! Your Telegram user ID is {uid}.\n\n" + HELP_TEXT,
                reply_markup=MENU,
            )

        @_gated
        async def cmd_help(update, _):
            await update.message.reply_text(HELP_TEXT, reply_markup=MENU)

        @_gated
        async def cmd_status(update, _):
            redis = get_redis()
            state = await redis.get("system:state")
            pending = await list_pending()
            await update.message.reply_text(
                f"System: {state}\nPending approvals: {len(pending)}"
            )

        @_gated
        async def cmd_halt(update, _):
            await publish_channel(Channels.SYSTEM_HALT, {"reason": "telegram_killswitch"})
            await update.message.reply_text("🛑 Halt requested.")

        @_gated
        async def cmd_resume(update, _):
            await publish_channel(Channels.SYSTEM_RESUME, {"actor": "telegram"})
            await update.message.reply_text("▶️ Resume requested.")

        @_gated
        async def cmd_paper(update, ctx):
            args = ctx.args
            if not args or args[0] not in ("on", "off"):
                await update.message.reply_text("Usage: /paper on|off")
                return
            redis = get_redis()
            value = "true" if args[0] == "on" else "false"
            await redis.hset("risk_config", "paper_trading_mode", value)
            await publish_channel(Channels.CONFIG_UPDATED, {"key": "paper_trading_mode", "value": value})
            await update.message.reply_text(f"Paper mode: {args[0]}")

        @_gated
        async def cmd_joininfo(update, _):
            chat = update.effective_chat
            uid = update.effective_user.id if update.effective_user else "?"
            text = (
                "📡 Channel ingestion setup\n\n"
                f"This chat's ID: <code>{chat.id}</code>\n"
                f"Your user ID: <code>{uid}</code>\n\n"
                "To make Plata listen to a channel/group:\n"
                "1. Add this bot as a member (admin not required for public groups; "
                "channels require admin with 'Post messages' read).\n"
                "2. In Plata → Settings → 📰 News, paste the chat ID into "
                "'Telegram channel IDs' and turn 'Listen to Telegram channels' ON.\n\n"
                "Tip: forward a message from the target channel to @userinfobot to "
                "discover its ID quickly. Channel IDs are negative (e.g. -1001234567890)."
            )
            await update.message.reply_text(text, parse_mode="HTML")

        async def on_channel_message(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
            """Ingest messages from channels/groups Plata is watching."""
            try:
                from plata.agents.scraper.news_config import get_config as _news_cfg
                from plata.core.bus import Streams as _S, publish as _publish
                from plata.core.schemas import RawSignal as _Raw, SignalSource as _Src
                cfg = await _news_cfg()
            except Exception:  # noqa: BLE001
                return
            if not cfg.get("telegram_channels_enabled"):
                return
            allowed_chats = set(cfg.get("telegram_channel_ids") or [])
            msg = update.effective_message
            if not msg:
                return
            chat_id = msg.chat_id
            if allowed_chats and chat_id not in allowed_chats:
                return
            body = (msg.text or msg.caption or "").strip()
            if not body:
                return
            link = None
            try:
                if msg.chat.username:
                    link = f"https://t.me/{msg.chat.username}/{msg.message_id}"
            except Exception:  # noqa: BLE001
                link = None
            title = body.splitlines()[0][:300]
            sig = _Raw(
                source=_Src.TELEGRAM,
                url=link or f"tg://{chat_id}/{msg.message_id}",
                title=title,
                body=body[:4000],
                source_published_at=msg.date,
                metadata={
                    "chat_id": chat_id,
                    "chat_title": getattr(msg.chat, "title", None),
                    "message_id": msg.message_id,
                },
            )
            await _publish(_S.RAW_SIGNALS, sig)

        @_gated
        async def cmd_positions(update, _):
            # Best-effort: read latest from agent_status:executor
            redis = get_redis()
            data = await redis.hgetall("agent_status:executor")
            await update.message.reply_text(json.dumps(data, indent=2) if data else "No data.")

        @_gated
        async def cb_approval(update, _):
            query = update.callback_query
            await query.answer()
            try:
                action, proposal_ulid = query.data.split(":", 1)
            except ValueError:
                return
            approved = action == "approve"
            user = query.from_user.username or str(query.from_user.id)
            first = await resolve(proposal_ulid, approved=approved, actor=f"telegram:{user}")
            verdict = "✅ Approved" if approved else "❌ Rejected"
            if not first:
                verdict += " (already decided)"
            await query.edit_message_text(text=f"{verdict}\nProposal: {proposal_ulid}")

        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("help", cmd_help))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("halt", cmd_halt))
        app.add_handler(CommandHandler("resume", cmd_resume))
        app.add_handler(CommandHandler("paper", cmd_paper))
        app.add_handler(CommandHandler("positions", cmd_positions))
        app.add_handler(CommandHandler("joininfo", cmd_joininfo))
        app.add_handler(CallbackQueryHandler(cb_approval))
        # Channel/group ingestion — listens to any chat the bot is a member of;
        # the on_channel_message handler self-gates by news_config.telegram_channel_ids.
        try:
            chan_filter = filters.UpdateType.CHANNEL_POST | (
                (filters.ChatType.GROUP | filters.ChatType.SUPERGROUP) & ~filters.COMMAND
            )
            app.add_handler(MessageHandler(chan_filter, on_channel_message))
        except Exception:  # noqa: BLE001
            _log.exception("channel_ingest_handler_register_failed")

        # Start the bot and the alert subscriber
        # Silence the noisy traceback on `Conflict: terminated by other getUpdates`
        # (happens when the bot token is set on more than one service or a previous
        # deploy is briefly still running). One concise log per occurrence.
        from telegram.error import Conflict
        _conflict_warned = {"v": False}

        def _on_polling_error(exc: Exception) -> None:
            if isinstance(exc, Conflict):
                if not _conflict_warned["v"]:
                    _log.warning(
                        "telegram_conflict",
                        msg="Another bot instance is polling the same token. "
                            "Make sure TELEGRAM_BOT_TOKEN is only set on ingestion_hub.",
                    )
                    _conflict_warned["v"] = True
                return
            _log.warning("telegram_polling_error", error_type=type(exc).__name__, error=str(exc))

        await app.initialize()
        await app.start()
        await app.updater.start_polling(error_callback=_on_polling_error)

        try:
            await asyncio.gather(
                self._hitl_subscriber(app),
                self._heartbeat_loop(),
            )
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    async def handle(self, payload):  # not used; run() is overridden
        return None

    async def _hitl_subscriber(self, app: Any) -> None:
        """Listen for new HITL requests and push them to all allowed users."""
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        settings = get_settings()
        allowed = settings.allowed_telegram_ids
        if not allowed:
            return
        async for channel, payload in subscribe(Channels.hitl_requested()):
            try:
                proposal_ulid = payload.get("proposal_ulid") if isinstance(payload, dict) else None
                reason = payload.get("reason", "") if isinstance(payload, dict) else ""
                if not proposal_ulid:
                    continue
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Approve", callback_data=f"approve:{proposal_ulid}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"reject:{proposal_ulid}"),
                ]])
                for uid in allowed:
                    try:
                        await app.bot.send_message(
                            chat_id=uid,
                            text=f"⏳ HITL required\nProposal: {proposal_ulid}\nReason: {reason}",
                            reply_markup=kb,
                        )
                    except Exception:  # pragma: no cover
                        _log.exception("hitl_push_failed", uid=uid)
                await log_action(
                    self.name,
                    f"Sent HITL prompt for {proposal_ulid} to {len(allowed)} user(s)",
                )
            except Exception:  # pragma: no cover
                _log.exception("hitl_subscriber_error")
