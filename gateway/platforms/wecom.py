"""
WeCom (Enterprise WeChat) platform adapter.

Uses the WeCom AI Bot WebSocket gateway for inbound and outbound messages.
The adapter focuses on the core gateway path:

- authenticate via ``aibot_subscribe``
- receive inbound ``aibot_msg_callback`` events
- send outbound markdown messages via ``aibot_send_msg``
- upload outbound media via ``aibot_upload_media_*`` and send native attachments
- best-effort download of inbound image/file attachments for agent context

Configuration in config.yaml:
    platforms:
      wecom:
        enabled: true
        extra:
          bot_id: "your-bot-id"          # or WECOM_BOT_ID env var
          secret: "your-secret"          # or WECOM_SECRET env var
          websocket_url: "wss://openws.work.weixin.qq.com"
          dm_policy: "open"              # open | allowlist | disabled | pairing
          allow_from: ["user_id_1"]
          group_policy: "open"           # open | allowlist | disabled
          group_allow_from: ["group_id_1"]
          groups:
            group_id_1:
              allow_from: ["user_id_1"]
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import mimetypes
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None  # type: ignore[assignment]

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_document_from_bytes,
    cache_image_from_bytes,
)

logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://openws.work.weixin.qq.com"

APP_CMD_SUBSCRIBE = "aibot_subscribe"
APP_CMD_CALLBACK = "aibot_msg_callback"
APP_CMD_LEGACY_CALLBACK = "aibot_callback"
APP_CMD_EVENT_CALLBACK = "aibot_event_callback"
APP_CMD_SEND = "aibot_send_msg"
APP_CMD_RESPONSE = "aibot_respond_msg"
APP_CMD_PING = "ping"
APP_CMD_UPLOAD_MEDIA_INIT = "aibot_upload_media_init"
APP_CMD_UPLOAD_MEDIA_CHUNK = "aibot_upload_media_chunk"
APP_CMD_UPLOAD_MEDIA_FINISH = "aibot_upload_media_finish"

CALLBACK_COMMANDS = {APP_CMD_CALLBACK, APP_CMD_LEGACY_CALLBACK}
NON_RESPONSE_COMMANDS = CALLBACK_COMMANDS | {APP_CMD_EVENT_CALLBACK}

MAX_MESSAGE_LENGTH = 4000
CONNECT_TIMEOUT_SECONDS = 20.0
REQUEST_TIMEOUT_SECONDS = 15.0
HEARTBEAT_INTERVAL_SECONDS = 30.0
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]

STREAM_EXPIRED_ERRCODE = 846608
_STREAM_EXPIRED_RETRY_GRACE = 1.0

DEDUP_MAX_SIZE = 1000

# 模板卡片类型
VALID_CARD_TYPES = frozenset({
    "text_notice",
    "news_notice",
    "button_interaction",
    "vote_interaction",
    "multiple_interaction",
})

# 正则：匹配 markdown JSON 代码块，用于提取模板卡片
_TEMPLATE_CARD_BLOCK_RE = re.compile(r"```(?:json)?\s*\n([\s\S]*?)\n```", re.MULTILINE)
# 正则：匹配未闭合的代码块尾部（LLM 正在输出中的模板卡片）
_TEMPLATE_CARD_UNCLOSED_RE = re.compile(r"```(?:json)?\s*\n[\s\S]*$", re.MULTILINE)

IMAGE_MAX_BYTES = 10 * 1024 * 1024
VIDEO_MAX_BYTES = 10 * 1024 * 1024
VOICE_MAX_BYTES = 2 * 1024 * 1024
FILE_MAX_BYTES = 20 * 1024 * 1024
ABSOLUTE_MAX_BYTES = FILE_MAX_BYTES
UPLOAD_CHUNK_SIZE = 512 * 1024
MAX_UPLOAD_CHUNKS = 100
VOICE_SUPPORTED_MIMES = {"audio/amr"}


def check_wecom_requirements() -> bool:
    """Check if WeCom runtime dependencies are available."""
    return AIOHTTP_AVAILABLE and HTTPX_AVAILABLE


def _urlesc(value: str) -> str:
    """URL-encode a string using quote from urllib.parse."""
    from urllib.parse import quote as _quote
    return _quote(str(value), safe="")


def _coerce_list(value: Any) -> List[str]:
    """Coerce config values into a trimmed string list."""
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _normalize_entry(raw: str) -> str:
    """Normalize allowlist entries such as ``wecom:user:foo``."""
    value = str(raw).strip()
    value = re.sub(r"^wecom:", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^(user|group):", "", value, flags=re.IGNORECASE)
    return value.strip()


def _entry_matches(entries: List[str], target: str) -> bool:
    """Case-insensitive allowlist match with ``*`` support."""
    normalized_target = str(target).strip().lower()
    for entry in entries:
        normalized = _normalize_entry(entry).lower()
        if normalized == "*" or normalized == normalized_target:
            return True
    return False


class WeComAdapter(BasePlatformAdapter):
    """WeCom AI Bot adapter backed by a persistent WebSocket connection."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    SUPPORTS_MESSAGE_EDITING = False
    # Threshold for detecting WeCom client-side message splits.
    # When a chunk is near the 4000-char limit, a continuation is almost certain.
    _SPLIT_THRESHOLD = 3900

    # Signals to ``GatewayStreamConsumer`` that this adapter uses a single
    # native stream for the entire response — opened by send_typing() /
    # send(streaming=True), continued by edit_message(), closed by
    # finalize_stream(). Tool-boundary segment breaks must NOT close the
    # stream, because WeCom's reply channel only allows one stream per
    # reply_req_id (reopening triggers errcode 6000 "data version conflict").
    native_streaming_unified = True
    # Must finalize every stream with finish=True, even when the stream
    # consumer's mid-stream edit already delivered the content.
    # Without this, the WeCom server keeps the stream open permanently
    # and send_typing() can't open new bubbles for subsequent replies.
    REQUIRES_EDIT_FINALIZE = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.WECOM)

        extra = config.extra or {}
        self._bot_id = str(extra.get("bot_id") or os.getenv("WECOM_BOT_ID", "")).strip()
        self._secret = str(extra.get("secret") or os.getenv("WECOM_SECRET", "")).strip()
        self._ws_url = str(
            extra.get("websocket_url")
            or extra.get("websocketUrl")
            or os.getenv("WECOM_WEBSOCKET_URL", DEFAULT_WS_URL)
        ).strip() or DEFAULT_WS_URL

        self._dm_policy = str(extra.get("dm_policy") or os.getenv("WECOM_DM_POLICY", "open")).strip().lower()
        self._allow_from = _coerce_list(extra.get("allow_from") or extra.get("allowFrom"))

        self._group_policy = str(extra.get("group_policy") or os.getenv("WECOM_GROUP_POLICY", "open")).strip().lower()
        self._group_allow_from = _coerce_list(extra.get("group_allow_from") or extra.get("groupAllowFrom"))
        self._groups = extra.get("groups") if isinstance(extra.get("groups"), dict) else {}

        # Agent HTTP API 回退配置
        raw_agent = extra.get("agent")
        self._agent_corp_id: str = ""
        self._agent_corp_secret: str = ""
        self._agent_id: int = 0
        self._agent_configured: bool = False
        if isinstance(raw_agent, dict):
            self._agent_corp_id = str(raw_agent.get("corp_id") or raw_agent.get("corpId") or "").strip()
            self._agent_corp_secret = str(raw_agent.get("corp_secret") or raw_agent.get("corpSecret") or "").strip()
            raw_agent_id = raw_agent.get("agent_id") or raw_agent.get("agentId") or 0
            try:
                self._agent_id = int(raw_agent_id)
            except (TypeError, ValueError):
                self._agent_id = 0
            self._agent_configured = bool(self._agent_corp_id and self._agent_corp_secret and self._agent_id > 0)

        # Agent API token 缓存
        self._agent_token: str = ""
        self._agent_token_expires_at: float = 0.0
        self._agent_token_refresh_lock: asyncio.Lock = asyncio.Lock()

        self._session: Optional["aiohttp.ClientSession"] = None
        self._ws: Optional["aiohttp.ClientWebSocketResponse"] = None
        self._http_client: Optional["httpx.AsyncClient"] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._pending_responses: Dict[str, asyncio.Future] = {}
        self._dedup = MessageDeduplicator(max_size=DEDUP_MAX_SIZE)
        self._reply_req_ids: Dict[str, str] = {}

        # Text batching: merge rapid successive messages (Telegram-style).
        # WeCom clients split long messages around 4000 chars.
        self._text_batch_delay_seconds = float(os.getenv("HERMES_WECOM_TEXT_BATCH_DELAY_SECONDS", "0.6"))
        self._text_batch_split_delay_seconds = float(os.getenv("HERMES_WECOM_TEXT_BATCH_SPLIT_DELAY_SECONDS", "2.0"))
        self._text_batch_max_wait_seconds = float(os.getenv("HERMES_WECOM_TEXT_BATCH_MAX_WAIT_SECONDS", "5.0"))
        self._pending_text_batches: Dict[str, MessageEvent] = {}
        self._pending_text_batch_tasks: Dict[str, asyncio.Task] = {}
        self._pending_text_batch_start: Dict[str, float] = {}  # monotonic timestamps
        self._device_id = uuid.uuid4().hex
        self._last_chat_req_ids: Dict[str, str] = {}

        # Active native-streaming sessions: message_id -> (reply_req_id, stream_id).
        # Populated by send() when ``metadata["streaming"]`` is truthy, consumed
        # by edit_message() / finalize_stream() to continue / close the stream.
        #
        # The message_id we use is ``reply_req_id`` itself — WeCom's reply
        # channel only supports one stream per request, so this mapping is
        # effectively reply_req_id -> stream_id.
        self._active_streams: Dict[str, Tuple[str, str]] = {}

        # Tracks finalized streams so send_typing() doesn't reopen them.
        self._finalized_streams: Set[str] = set()

        # Track chats whose last inbound req_id came from an event callback.
        # Event callbacks cannot use reply-mode streams (no thinking bubble),
        # and their req_ids may not support reply stream at all — must use
        # proactive send (APP_CMD_SEND) for the final frame, skip mid-stream.
        self._event_callback_chats: Set[str] = set()

        # Stream-level ack tracking for non-blocking send.
        # Key: reply_req_id, Value: asyncio.Event that is set when the
        # finish=True response is received or the entry is cleaned up.
        self._stream_ack_events: Dict[str, asyncio.Event] = {}

        # Heartbeat health tracking (Issue 6): count consecutive pings with
        # no pong response. Reset on any successful ws message receive.
        self._missed_pongs: int = 0
        self._MAX_MISSED_PONGS: int = 3

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to the WeCom AI Bot gateway."""
        if not AIOHTTP_AVAILABLE:
            message = "WeCom startup failed: aiohttp not installed"
            self._set_fatal_error("wecom_missing_dependency", message, retryable=True)
            logger.warning("[%s] %s. Run: pip install aiohttp", self.name, message)
            return False
        if not HTTPX_AVAILABLE:
            message = "WeCom startup failed: httpx not installed"
            self._set_fatal_error("wecom_missing_dependency", message, retryable=True)
            logger.warning("[%s] %s. Run: pip install httpx", self.name, message)
            return False
        if not self._bot_id or not self._secret:
            message = "WeCom startup failed: WECOM_BOT_ID and WECOM_SECRET are required"
            self._set_fatal_error("wecom_missing_credentials", message, retryable=True)
            logger.warning("[%s] %s", self.name, message)
            return False

        try:
            # Tighter keepalive so idle CLOSE_WAIT drains promptly (#18451).
            from gateway.platforms._http_client_limits import platform_httpx_limits
            self._http_client = httpx.AsyncClient(
                timeout=30.0, follow_redirects=True, limits=platform_httpx_limits(),
            )
            await self._open_connection()
            self._mark_connected()
            self._listen_task = asyncio.create_task(self._listen_loop())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            logger.info("[%s] Connected to %s", self.name, self._ws_url)
            return True
        except Exception as exc:
            message = f"WeCom startup failed: {exc}"
            self._set_fatal_error("wecom_connect_error", message, retryable=True)
            logger.error("[%s] Failed to connect: %s", self.name, exc, exc_info=True)
            await self._cleanup_ws()
            if self._http_client:
                await self._http_client.aclose()
                self._http_client = None
            return False

    async def disconnect(self) -> None:
        """Disconnect from WeCom."""
        self._running = False
        self._mark_disconnected()

        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        self._fail_pending_responses(RuntimeError("WeCom adapter disconnected"))
        await self._cleanup_ws()

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        self._dedup.clear()
        logger.info("[%s] Disconnected", self.name)

    async def _cleanup_ws(self) -> None:
        """Close the live websocket/session, if any."""
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _open_connection(self) -> None:
        """Open and authenticate a websocket connection."""
        await self._cleanup_ws()
        self._session = aiohttp.ClientSession(trust_env=True)
        self._ws = await self._session.ws_connect(
            self._ws_url,
            heartbeat=HEARTBEAT_INTERVAL_SECONDS * 2,
            timeout=CONNECT_TIMEOUT_SECONDS,
        )

        req_id = self._new_req_id("subscribe")
        await self._send_json(
            {
                "cmd": APP_CMD_SUBSCRIBE,
                "headers": {"req_id": req_id},
                "body": {
                    "bot_id": self._bot_id,
                    "secret": self._secret,
                    "device_id": self._device_id,
                },
            }
        )

        auth_payload = await self._wait_for_handshake(req_id)
        errcode = auth_payload.get("errcode", 0)
        if errcode not in (0, None):
            errmsg = auth_payload.get("errmsg", "authentication failed")
            raise RuntimeError(f"{errmsg} (errcode={errcode})")

    async def _wait_for_handshake(self, req_id: str) -> Dict[str, Any]:
        """Wait for the subscribe acknowledgement."""
        if not self._ws:
            raise RuntimeError("WebSocket not initialized")

        deadline = asyncio.get_running_loop().time() + CONNECT_TIMEOUT_SECONDS
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for WeCom subscribe acknowledgement")

            msg = await asyncio.wait_for(self._ws.receive(), timeout=remaining)
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = self._parse_json(msg.data)
                if not payload:
                    continue
                if payload.get("cmd") == APP_CMD_PING:
                    continue
                if self._payload_req_id(payload) == req_id:
                    return payload
                logger.debug("[%s] Ignoring pre-auth payload: %s", self.name, payload.get("cmd"))
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("WeCom websocket closed during authentication")

    async def _listen_loop(self) -> None:
        """Read websocket events forever, reconnecting on errors."""
        backoff_idx = 0
        while self._running:
            try:
                await self._read_events()
                backoff_idx = 0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._running:
                    return
                logger.warning("[%s] WebSocket error: %s", self.name, exc)
                self._fail_pending_responses(RuntimeError("WeCom connection interrupted"))

                delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
                backoff_idx += 1
                await asyncio.sleep(delay)

                try:
                    await self._open_connection()
                    backoff_idx = 0
                    logger.info("[%s] Reconnected", self.name)
                except Exception as reconnect_exc:
                    logger.warning("[%s] Reconnect failed: %s", self.name, reconnect_exc)

    async def _read_events(self) -> None:
        """Read websocket frames until the connection closes."""
        if not self._ws:
            raise RuntimeError("WebSocket not connected")

        while self._running and self._ws and not self._ws.closed:
            msg = await self._ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = self._parse_json(msg.data)
                if payload:
                    # Any incoming message from the server resets the
                    # heartbeat health counter (server is alive).
                    self._missed_pongs = 0
                    await self._dispatch_payload(payload)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("WeCom websocket closed")

    async def _heartbeat_loop(self) -> None:
        """Send lightweight application-level pings with timeout detection.

        Increments ``_missed_pongs`` on each ping.  If it reaches
        ``_MAX_MISSED_PONGS`` (i.e. 3 consecutive pings with no incoming
        message from the server), raises a ``RuntimeError`` to trigger
        reconnection via ``_listen_loop``'s error handler.

        ``_read_events`` resets ``_missed_pongs = 0`` on every server frame.
        """
        try:
            while self._running:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                if not self._ws or self._ws.closed:
                    continue
                try:
                    await self._send_json(
                        {
                            "cmd": APP_CMD_PING,
                            "headers": {"req_id": self._new_req_id("ping")},
                            "body": {},
                        }
                    )
                except Exception as exc:
                    logger.debug("[%s] Heartbeat send failed: %s", self.name, exc)

                self._missed_pongs += 1
                if self._missed_pongs >= self._MAX_MISSED_PONGS:
                    logger.warning(
                        "[%s] Heartbeat timeout: %d consecutive pings without response — forcing reconnect",
                        self.name, self._missed_pongs,
                    )
                    raise RuntimeError(
                        f"Heartbeat timeout after {self._missed_pongs} missed pongs"
                    )
        except asyncio.CancelledError:
            pass

    async def _dispatch_payload(self, payload: Dict[str, Any]) -> None:
        """Route inbound websocket payloads."""
        req_id = self._payload_req_id(payload)
        cmd = str(payload.get("cmd") or "")

        if req_id and req_id in self._pending_responses and cmd not in NON_RESPONSE_COMMANDS:
            future = self._pending_responses.get(req_id)
            if future and not future.done():
                future.set_result(payload)
            return

        if cmd in CALLBACK_COMMANDS:
            await self._on_message(payload)
            return
        if cmd == APP_CMD_PING:
            return
        if cmd == APP_CMD_EVENT_CALLBACK:
            # Event callbacks carry a chat context but cannot use reply-mode
            # streams — store the req_id as event-callback-originated so
            # send() can choose proactive mode instead.
            chat_id = self._extract_chat_id_from_event_callback(payload)
            if chat_id:
                evt_req_id = self._payload_req_id(payload)
                if evt_req_id:
                    self._remember_chat_req_id(chat_id, evt_req_id)
                    # self._event_callback_chats.add(chat_id)  # disabled: causes reply to skip reply-stream, which breaks forwarding and table swipe on WeCom
            # 检查是否包含模板卡片事件回调（按钮点击、提交等）—
            # 需要路由给 _on_message 处理
            body = payload.get("body") if isinstance(payload.get("body"), dict) else {}
            if str(body.get("msgtype") or "").lower() == "event":
                event_body = body.get("event") if isinstance(body.get("event"), dict) else {}
                if str(event_body.get("eventtype") or "").strip() == "template_card_event":
                    await self._on_message(payload)
            return

        logger.debug("[%s] Ignoring websocket payload: %s", self.name, cmd or payload)

    def _fail_pending_responses(self, exc: Exception) -> None:
        """Fail all outstanding request futures."""
        for req_id, future in list(self._pending_responses.items()):
            if not future.done():
                future.set_exception(exc)
            self._pending_responses.pop(req_id, None)

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        """Send a raw JSON frame over the active websocket."""
        if not self._ws or self._ws.closed:
            raise RuntimeError("WeCom websocket is not connected")
        cmd = payload.get("cmd", "?")
        body = payload.get("body", {})
        msgtype = body.get("msgtype") if isinstance(body, dict) else "?"
        stream_info = ""
        if msgtype == "stream" and isinstance(body, dict):
            s = body.get("stream", {})
            stream_info = f" finish={s.get('finish')} id={s.get('id','?')[:12]}"
        logger.info(
            "[%s] WS >> cmd=%s msgtype=%s%s",
            self.name, cmd, msgtype, stream_info,
        )
        await self._ws.send_json(payload)

    async def _send_request(self, cmd: str, body: Dict[str, Any], timeout: float = REQUEST_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """Send a JSON request and await the correlated response."""
        if not self._ws or self._ws.closed:
            raise RuntimeError("WeCom websocket is not connected")

        req_id = self._new_req_id(cmd)
        future = asyncio.get_running_loop().create_future()
        self._pending_responses[req_id] = future
        try:
            await self._send_json({"cmd": cmd, "headers": {"req_id": req_id}, "body": body})
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
        finally:
            self._pending_responses.pop(req_id, None)

    async def _send_reply_request(
        self,
        reply_req_id: str,
        body: Dict[str, Any],
        cmd: str = APP_CMD_RESPONSE,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        """Send a reply frame correlated to an inbound callback req_id."""
        if not self._ws or self._ws.closed:
            raise RuntimeError("WeCom websocket is not connected")

        normalized_req_id = str(reply_req_id or "").strip()
        if not normalized_req_id:
            raise ValueError("reply_req_id is required")

        future = asyncio.get_running_loop().create_future()
        self._pending_responses[normalized_req_id] = future
        try:
            await self._send_json(
                {"cmd": cmd, "headers": {"req_id": normalized_req_id}, "body": body}
            )
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
        finally:
            self._pending_responses.pop(normalized_req_id, None)

    @staticmethod
    def _new_req_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex}"

    @staticmethod
    def _payload_req_id(payload: Dict[str, Any]) -> str:
        headers = payload.get("headers")
        if isinstance(headers, dict):
            return str(headers.get("req_id") or "")
        return ""

    @staticmethod
    def _parse_json(raw: Any) -> Optional[Dict[str, Any]]:
        try:
            payload = json.loads(raw)
        except Exception:
            logger.debug("Failed to parse WeCom payload: %r", raw)
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _extract_chat_id_from_event_callback(payload: Dict[str, Any]) -> Optional[str]:
        """Extract chat_id from an event callback payload."""
        body = payload.get("body")
        if not isinstance(body, dict):
            return None
        sender = body.get("from") if isinstance(body.get("from"), dict) else {}
        sender_id = str(sender.get("userid") or "").strip()
        chat_id = str(body.get("chatid") or sender_id).strip()
        return chat_id or None

    # ------------------------------------------------------------------
    # Inbound message parsing
    # ------------------------------------------------------------------

    async def _on_message(self, payload: Dict[str, Any]) -> None:
        """Process an inbound WeCom message callback event."""
        body = payload.get("body")
        if not isinstance(body, dict):
            return

        msg_id = str(body.get("msgid") or self._payload_req_id(payload) or uuid.uuid4().hex)
        if self._dedup.is_duplicate(msg_id):
            logger.debug("[%s] Duplicate message %s ignored", self.name, msg_id)
            return
        self._remember_reply_req_id(msg_id, self._payload_req_id(payload))

        sender = body.get("from") if isinstance(body.get("from"), dict) else {}
        sender_id = str(sender.get("userid") or "").strip()
        chat_id = str(body.get("chatid") or sender_id).strip()
        if not chat_id:
            logger.debug("[%s] Missing chat id, skipping message", self.name)
            return

        is_group = str(body.get("chattype") or "").lower() == "group"
        if is_group:
            if not self._is_group_allowed(chat_id, sender_id):
                logger.debug("[%s] Group %s / sender %s blocked by policy", self.name, chat_id, sender_id)
                return
        elif not self._is_dm_allowed(sender_id):
            logger.debug("[%s] DM sender %s blocked by policy", self.name, sender_id)
            return

        # Cache the inbound req_id after policy checks so proactive sends to
        # this chat can fall back to APP_CMD_RESPONSE (required for groups —
        # WeCom AI Bots cannot initiate APP_CMD_SEND in group chats).
        self._remember_chat_req_id(chat_id, self._payload_req_id(payload))

        text, reply_text = self._extract_text(body)
        # Strip leading @mention in group chats so slash commands like
        # "@BotName /approve" are correctly recognized as "/approve".
        # Mirrors what the Telegram adapter does (re.sub @botname).
        if is_group and text:
            text = re.sub(r"^@\S+\s*", "", text).strip()
        media_urls, media_types = await self._extract_media(body)
        message_type = self._derive_message_type(body, text, media_types)
        has_reply_context = bool(reply_text and (text or media_urls))

        if not text and reply_text and not media_urls:
            text = reply_text

        if not text and not media_urls:
            logger.debug("[%s] Empty WeCom message skipped", self.name)
            return

        source = self.build_source(
            chat_id=chat_id,
            chat_type="group" if is_group else "dm",
            user_id=sender_id or None,
            user_name=sender_id or None,
        )

        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            raw_message=payload,
            message_id=msg_id,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=f"quote:{msg_id}" if has_reply_context else None,
            reply_to_text=reply_text if has_reply_context else None,
            timestamp=datetime.now(tz=timezone.utc),
        )

        # Only batch plain text messages — commands, media, etc. dispatch
        # immediately since they won't be split by the WeCom client.
        if message_type == MessageType.TEXT and self._text_batch_delay_seconds > 0:
            self._enqueue_text_event(event)
        else:
            await self.handle_message(event)

    # ------------------------------------------------------------------
    # Text message aggregation (handles WeCom client-side splits)
    # ------------------------------------------------------------------

    def _text_batch_key(self, event: MessageEvent) -> str:
        """Session-scoped key for text message batching."""
        from gateway.session import build_session_key
        return build_session_key(
            event.source,
            group_sessions_per_user=self.config.extra.get("group_sessions_per_user", True),
            thread_sessions_per_user=self.config.extra.get("thread_sessions_per_user", False),
        )

    def _enqueue_text_event(self, event: MessageEvent) -> None:
        """Buffer a text event and reset the flush timer.

        When WeCom splits a long user message at 4000 chars, the chunks
        arrive within a few hundred milliseconds.  This merges them into
        a single event before dispatching.

        Enforces ``_text_batch_max_wait_seconds`` (default 5.0): if the
        first event in a batch has been waiting longer than the max, the
        batch is flushed immediately instead of resetting the timer.
        """
        import time as _time
        key = self._text_batch_key(event)
        existing = self._pending_text_batches.get(key)
        chunk_len = len(event.text or "")
        now = _time.monotonic()
        if existing is None:
            event._last_chunk_len = chunk_len  # type: ignore[attr-defined]
            self._pending_text_batches[key] = event
            self._pending_text_batch_start[key] = now
        else:
            # Check if the batch has been waiting too long — flush immediately.
            start_ts = self._pending_text_batch_start.get(key, now)
            if now - start_ts >= self._text_batch_max_wait_seconds:
                logger.info(
                    "[WeCom] Forcing text batch flush for %s after %.1fs (max=%ds)",
                    key, now - start_ts, self._text_batch_max_wait_seconds,
                )
                # Merge and dispatch now
                if event.text:
                    existing.text = f"{existing.text}\n{event.text}" if existing.text else event.text
                if event.media_urls:
                    existing.media_urls.extend(event.media_urls)
                    existing.media_types.extend(event.media_types)
                prior_task = self._pending_text_batch_tasks.get(key)
                if prior_task and not prior_task.done():
                    prior_task.cancel()
                self._pending_text_batch_tasks.pop(key, None)
                self._pending_text_batch_start.pop(key, None)
                dispatched_event = self._pending_text_batches.pop(key, None)
                if dispatched_event:
                    asyncio.create_task(self.handle_message(dispatched_event))
                return

            if event.text:
                existing.text = f"{existing.text}\n{event.text}" if existing.text else event.text
            existing._last_chunk_len = chunk_len  # type: ignore[attr-defined]
            # Merge any media that might be attached
            if event.media_urls:
                existing.media_urls.extend(event.media_urls)
                existing.media_types.extend(event.media_types)

        # Cancel any pending flush and restart the timer
        prior_task = self._pending_text_batch_tasks.get(key)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        self._pending_text_batch_tasks[key] = asyncio.create_task(
            self._flush_text_batch(key)
        )

    async def _flush_text_batch(self, key: str) -> None:
        """Wait for the quiet period then dispatch the aggregated text.

        Uses a longer delay when the latest chunk is near WeCom's 4000-char
        split point, since a continuation chunk is almost certain.
        """
        current_task = asyncio.current_task()
        try:
            pending = self._pending_text_batches.get(key)
            last_len = getattr(pending, "_last_chunk_len", 0) if pending else 0
            if last_len >= self._SPLIT_THRESHOLD:
                delay = self._text_batch_split_delay_seconds
            else:
                delay = self._text_batch_delay_seconds
            await asyncio.sleep(delay)
            event = self._pending_text_batches.pop(key, None)
            if not event:
                return
            logger.info(
                "[WeCom] Flushing text batch %s (%d chars)",
                key, len(event.text or ""),
            )
            await self.handle_message(event)
        finally:
            if self._pending_text_batch_tasks.get(key) is current_task:
                self._pending_text_batch_tasks.pop(key, None)

    @staticmethod
    def _extract_text(body: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        """Extract plain text and quoted text from a callback payload."""
        # 处理模板卡片事件回调（用户点击按钮、提交表单等）
        msgtype = str(body.get("msgtype") or "").lower()
        if msgtype == "event":
            event_body = body.get("event") if isinstance(body.get("event"), dict) else {}
            event_type = str(event_body.get("eventtype") or "").strip()
            if event_type == "template_card_event":
                card_event = event_body.get("template_card_event") if isinstance(event_body.get("template_card_event"), dict) else {}
                return WeComAdapter._format_template_card_event_text(body, card_event), None

        text_parts: List[str] = []
        reply_text: Optional[str] = None
        msgtype = str(body.get("msgtype") or "").lower()

        if msgtype == "mixed":
            _raw_mixed = body.get("mixed")
            mixed = _raw_mixed if isinstance(_raw_mixed, dict) else {}
            _raw_items = mixed.get("msg_item")
            items = _raw_items if isinstance(_raw_items, list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                if str(item.get("msgtype") or "").lower() == "text":
                    _raw_text = item.get("text")
                    text_block = _raw_text if isinstance(_raw_text, dict) else {}
                    content = str(text_block.get("content") or "").strip()
                    if content:
                        text_parts.append(content)
        else:
            text_block = body.get("text") if isinstance(body.get("text"), dict) else {}
            content = str(text_block.get("content") or "").strip()
            if content:
                text_parts.append(content)

            if msgtype == "voice":
                voice_block = body.get("voice") if isinstance(body.get("voice"), dict) else {}
                voice_text = str(voice_block.get("content") or "").strip()
                if voice_text:
                    text_parts.append(voice_text)

            # Extract appmsg title (filename) for WeCom AI Bot attachments
            if msgtype == "appmsg":
                appmsg = body.get("appmsg") if isinstance(body.get("appmsg"), dict) else {}
                title = str(appmsg.get("title") or "").strip()
                if title:
                    text_parts.append(title)

        quote = body.get("quote") if isinstance(body.get("quote"), dict) else {}
        quote_type = str(quote.get("msgtype") or "").lower()
        if quote_type == "text":
            quote_text = quote.get("text") if isinstance(quote.get("text"), dict) else {}
            reply_text = str(quote_text.get("content") or "").strip() or None
        elif quote_type == "voice":
            quote_voice = quote.get("voice") if isinstance(quote.get("voice"), dict) else {}
            reply_text = str(quote_voice.get("content") or "").strip() or None

        return "\n".join(part for part in text_parts if part).strip(), reply_text

    async def _extract_media(self, body: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        """Best-effort extraction of inbound media to local cache paths."""
        media_paths: List[str] = []
        media_types: List[str] = []
        refs: List[Tuple[str, Dict[str, Any]]] = []
        msgtype = str(body.get("msgtype") or "").lower()

        if msgtype == "mixed":
            _raw_mixed = body.get("mixed")
            mixed = _raw_mixed if isinstance(_raw_mixed, dict) else {}
            _raw_items = mixed.get("msg_item")
            items = _raw_items if isinstance(_raw_items, list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                item_type = str(item.get("msgtype") or "").lower()
                if item_type == "image" and isinstance(item.get("image"), dict):
                    refs.append(("image", item["image"]))
        else:
            if isinstance(body.get("image"), dict):
                refs.append(("image", body["image"]))
            if msgtype == "file" and isinstance(body.get("file"), dict):
                refs.append(("file", body["file"]))
            # Handle appmsg (WeCom AI Bot attachments with PDF/Word/Excel)
            if msgtype == "appmsg" and isinstance(body.get("appmsg"), dict):
                appmsg = body["appmsg"]
                if isinstance(appmsg.get("file"), dict):
                    refs.append(("file", appmsg["file"]))
                elif isinstance(appmsg.get("image"), dict):
                    refs.append(("image", appmsg["image"]))

        quote = body.get("quote") if isinstance(body.get("quote"), dict) else {}
        quote_type = str(quote.get("msgtype") or "").lower()
        if quote_type == "image" and isinstance(quote.get("image"), dict):
            refs.append(("image", quote["image"]))
        elif quote_type == "file" and isinstance(quote.get("file"), dict):
            refs.append(("file", quote["file"]))

        for kind, ref in refs:
            cached = await self._cache_media(kind, ref)
            if cached:
                path, content_type = cached
                media_paths.append(path)
                media_types.append(content_type)

        return media_paths, media_types

    async def _cache_media(self, kind: str, media: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        """Cache an inbound image/file/media reference to local storage."""
        if "base64" in media and media.get("base64"):
            try:
                raw = self._decode_base64(media["base64"])
            except Exception as exc:
                logger.debug("[%s] Failed to decode %s base64 media: %s", self.name, kind, exc)
                return None

            if kind == "image":
                ext = self._detect_image_ext(raw)
                try:
                    return cache_image_from_bytes(raw, ext), self._mime_for_ext(ext, fallback="image/jpeg")
                except ValueError as exc:
                    logger.warning("[%s] Rejected non-image bytes: %s", self.name, exc)
                    return None

            filename = str(media.get("filename") or media.get("name") or "wecom_file")
            return cache_document_from_bytes(raw, filename), mimetypes.guess_type(filename)[0] or "application/octet-stream"

        url = str(media.get("url") or "").strip()
        if not url:
            return None

        try:
            raw, headers = await self._download_remote_bytes(url, max_bytes=ABSOLUTE_MAX_BYTES)
        except Exception as exc:
            logger.debug("[%s] Failed to download %s from %s: %s", self.name, kind, url, exc)
            return None

        aes_key = str(media.get("aeskey") or "").strip()
        if aes_key:
            try:
                raw = self._decrypt_file_bytes(raw, aes_key)
            except Exception as exc:
                logger.debug("[%s] Failed to decrypt %s from %s: %s", self.name, kind, url, exc)
                return None

        content_type = str(headers.get("content-type") or "").split(";", 1)[0].strip() or "application/octet-stream"
        if kind == "image":
            ext = self._guess_extension(url, content_type, fallback=self._detect_image_ext(raw))
            try:
                return cache_image_from_bytes(raw, ext), content_type or self._mime_for_ext(ext, fallback="image/jpeg")
            except ValueError as exc:
                logger.warning("[%s] Rejected non-image bytes from %s: %s", self.name, url, exc)
                return None

        filename = self._guess_filename(url, headers.get("content-disposition"), content_type)
        return cache_document_from_bytes(raw, filename), content_type

    @staticmethod
    def _decode_base64(data: str) -> bytes:
        payload = data.split(",", 1)[-1].strip()
        return base64.b64decode(payload)

    @staticmethod
    def _detect_image_ext(data: bytes) -> str:
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            return ".png"
        if data.startswith(b"\xff\xd8\xff"):
            return ".jpg"
        if data.startswith((b"GIF87a", b"GIF89a")):
            return ".gif"
        if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
            return ".webp"
        return ".jpg"

    @staticmethod
    def _mime_for_ext(ext: str, fallback: str = "application/octet-stream") -> str:
        return mimetypes.types_map.get(ext.lower(), fallback)

    @staticmethod
    def _guess_extension(url: str, content_type: str, fallback: str) -> str:
        ext = mimetypes.guess_extension(content_type) if content_type else None
        if ext:
            return ext
        path_ext = Path(urlparse(url).path).suffix
        if path_ext:
            return path_ext
        return fallback

    @staticmethod
    def _guess_filename(url: str, content_disposition: Optional[str], content_type: str) -> str:
        if content_disposition:
            # RFC 5987: filename*=UTF-8''encoded-filename.ext
            rfc5987_match = re.search(
                r"filename\*\s*=\s*(?:UTF-8|utf-8)''([^;\s]+)",
                content_disposition,
                re.IGNORECASE,
            )
            if rfc5987_match:
                try:
                    from urllib.parse import unquote
                    return unquote(rfc5987_match.group(1))
                except Exception:
                    return rfc5987_match.group(1)

            # Fallback: bare filename="..."
            match = re.search(r'filename="?([^";]+)"?', content_disposition)
            if match:
                return match.group(1)

        name = Path(urlparse(url).path).name or "document"
        if "." not in name:
            ext = mimetypes.guess_extension(content_type) or ".bin"
            name = f"{name}{ext}"
        return name

    @staticmethod
    def _derive_message_type(body: Dict[str, Any], text: str, media_types: List[str]) -> MessageType:
        """Choose the normalized inbound message type."""
        if any(mtype.startswith(("application/", "text/")) for mtype in media_types):
            return MessageType.DOCUMENT
        if any(mtype.startswith("image/") for mtype in media_types):
            return MessageType.TEXT if text else MessageType.PHOTO
        if str(body.get("msgtype") or "").lower() == "voice":
            return MessageType.VOICE
        return MessageType.TEXT

    # ------------------------------------------------------------------
    # Policy helpers
    # ------------------------------------------------------------------

    def _is_dm_allowed(self, sender_id: str) -> bool:
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "allowlist":
            return _entry_matches(self._allow_from, sender_id)
        return True

    def _is_group_allowed(self, chat_id: str, sender_id: str) -> bool:
        if self._group_policy == "disabled":
            return False
        if self._group_policy == "allowlist" and not _entry_matches(self._group_allow_from, chat_id):
            return False

        group_cfg = self._resolve_group_cfg(chat_id)
        sender_allow = _coerce_list(group_cfg.get("allow_from") or group_cfg.get("allowFrom"))
        if sender_allow:
            return _entry_matches(sender_allow, sender_id)
        return True

    def _resolve_group_cfg(self, chat_id: str) -> Dict[str, Any]:
        if not isinstance(self._groups, dict):
            return {}
        if chat_id in self._groups and isinstance(self._groups[chat_id], dict):
            return self._groups[chat_id]
        lowered = chat_id.lower()
        for key, value in self._groups.items():
            if isinstance(key, str) and key.lower() == lowered and isinstance(value, dict):
                return value
        wildcard = self._groups.get("*")
        return wildcard if isinstance(wildcard, dict) else {}

    def _remember_reply_req_id(self, message_id: str, req_id: str) -> None:
        normalized_message_id = str(message_id or "").strip()
        normalized_req_id = str(req_id or "").strip()
        if not normalized_message_id or not normalized_req_id:
            return
        self._reply_req_ids[normalized_message_id] = normalized_req_id
        while len(self._reply_req_ids) > DEDUP_MAX_SIZE:
            self._reply_req_ids.pop(next(iter(self._reply_req_ids)))

    def _remember_chat_req_id(self, chat_id: str, req_id: str) -> None:
        """Cache the most recent inbound req_id per chat.

        Used as a fallback reply target when we need to send into a group
        without an explicit ``reply_to`` — WeCom AI Bots are blocked from
        APP_CMD_SEND in groups and must use APP_CMD_RESPONSE bound to some
        inbound req_id to reply passively.

        The per-chat cache has a maximum size (DEDUP_MAX_SIZE) so we
        don't leak memory across many chats.

        Also clears stale streaming state for this chat — when a new
        inbound message arrives, any previous stream is done and the
        next send_typing() / send() must open a fresh one.
        """
        normalized_chat_id = str(chat_id or "").strip()
        normalized_req_id = str(req_id or "").strip()
        if not normalized_chat_id or not normalized_req_id:
            return
        # Clear any stale active stream entry for this chat so the next
        # send_typing() / send() can open a fresh one.  In normal flow
        # finalize_stream() already pops the entry; this is a safety net
        # for edge cases (interrupted turns, gateway restarts).
        self._active_streams.pop(normalized_chat_id, None)
        self._finalized_streams.discard(normalized_chat_id)
        self._last_chat_req_ids[normalized_chat_id] = normalized_req_id
        while len(self._last_chat_req_ids) > DEDUP_MAX_SIZE:
            self._last_chat_req_ids.pop(next(iter(self._last_chat_req_ids)))

    def _reply_req_id_for_message(self, reply_to: Optional[str]) -> Optional[str]:
        normalized = str(reply_to or "").strip()
        if not normalized or normalized.startswith("quote:"):
            return None
        return self._reply_req_ids.get(normalized)

    # ------------------------------------------------------------------
    # Outbound messaging
    # ------------------------------------------------------------------

    @staticmethod
    def _guess_mime_type(filename: str) -> str:
        mime_type = mimetypes.guess_type(filename)[0]
        if mime_type:
            return mime_type
        if Path(filename).suffix.lower() == ".amr":
            return "audio/amr"
        return "application/octet-stream"

    @staticmethod
    def _normalize_content_type(content_type: str, filename: str) -> str:
        normalized = str(content_type or "").split(";", 1)[0].strip().lower()
        guessed = WeComAdapter._guess_mime_type(filename)
        if not normalized:
            return guessed
        if normalized in {"application/octet-stream", "text/plain"}:
            return guessed
        return normalized

    @staticmethod
    def _detect_wecom_media_type(content_type: str) -> str:
        mime_type = str(content_type or "").strip().lower()
        if mime_type.startswith("image/"):
            return "image"
        if mime_type.startswith("video/"):
            return "video"
        if mime_type.startswith("audio/") or mime_type == "application/ogg":
            return "voice"
        return "file"

    @staticmethod
    def _apply_file_size_limits(file_size: int, detected_type: str, content_type: Optional[str] = None) -> Dict[str, Any]:
        file_size_mb = file_size / (1024 * 1024)
        normalized_type = str(detected_type or "file").lower()
        normalized_content_type = str(content_type or "").strip().lower()

        if file_size > ABSOLUTE_MAX_BYTES:
            return {
                "final_type": normalized_type,
                "rejected": True,
                "reject_reason": (
                    f"文件大小 {file_size_mb:.2f}MB 超过了企业微信允许的最大限制 20MB，无法发送。"
                    "请尝试压缩文件或减小文件大小。"
                ),
                "downgraded": False,
                "downgrade_note": None,
            }

        if normalized_type == "image" and file_size > IMAGE_MAX_BYTES:
            return {
                "final_type": "file",
                "rejected": False,
                "reject_reason": None,
                "downgraded": True,
                "downgrade_note": f"图片大小 {file_size_mb:.2f}MB 超过 10MB 限制，已转为文件格式发送",
            }

        if normalized_type == "video" and file_size > VIDEO_MAX_BYTES:
            return {
                "final_type": "file",
                "rejected": False,
                "reject_reason": None,
                "downgraded": True,
                "downgrade_note": f"视频大小 {file_size_mb:.2f}MB 超过 10MB 限制，已转为文件格式发送",
            }

        if normalized_type == "voice":
            if normalized_content_type and normalized_content_type not in VOICE_SUPPORTED_MIMES:
                return {
                    "final_type": "file",
                    "rejected": False,
                    "reject_reason": None,
                    "downgraded": True,
                    "downgrade_note": (
                        f"语音格式 {normalized_content_type} 不支持，企微仅支持 AMR 格式，已转为文件格式发送"
                    ),
                }
            if file_size > VOICE_MAX_BYTES:
                return {
                    "final_type": "file",
                    "rejected": False,
                    "reject_reason": None,
                    "downgraded": True,
                    "downgrade_note": f"语音大小 {file_size_mb:.2f}MB 超过 2MB 限制，已转为文件格式发送",
                }

        return {
            "final_type": normalized_type,
            "rejected": False,
            "reject_reason": None,
            "downgraded": False,
            "downgrade_note": None,
        }

    @staticmethod
    def _response_error(response: Dict[str, Any]) -> Optional[str]:
        errcode = response.get("errcode", 0)
        if errcode in (0, None):
            return None
        errmsg = str(response.get("errmsg") or "unknown error")
        return f"WeCom errcode {errcode}: {errmsg}"

    @classmethod
    def _raise_for_wecom_error(cls, response: Dict[str, Any], operation: str) -> None:
        error = cls._response_error(response)
        if error:
            raise RuntimeError(f"{operation} failed: {error}")

    @staticmethod
    def _decrypt_file_bytes(encrypted_data: bytes, aes_key: str) -> bytes:
        if not encrypted_data:
            raise ValueError("encrypted_data is empty")
        if not aes_key:
            raise ValueError("aes_key is required")

        # WeCom doesn't pad base64 keys; add padding if needed
        aes_key = aes_key + '=' * ((4 - len(aes_key) % 4) % 4)
        key = base64.b64decode(aes_key)
        if len(key) != 32:
            raise ValueError(f"Invalid WeCom AES key length: expected 32 bytes, got {len(key)}")

        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        except ImportError as exc:  # pragma: no cover - dependency is environment-specific
            raise RuntimeError("cryptography is required for WeCom media decryption") from exc

        cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16]))
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(encrypted_data) + decryptor.finalize()

        pad_len = decrypted[-1]
        if pad_len < 1 or pad_len > 32 or pad_len > len(decrypted):
            raise ValueError(f"Invalid PKCS#7 padding value: {pad_len}")
        if any(byte != pad_len for byte in decrypted[-pad_len:]):
            raise ValueError("Invalid PKCS#7 padding: padding bytes mismatch")

        return decrypted[:-pad_len]

    async def _download_remote_bytes(
        self,
        url: str,
        max_bytes: int,
    ) -> Tuple[bytes, Dict[str, str]]:
        from tools.url_safety import is_safe_url
        if not is_safe_url(url):
            raise ValueError(f"Blocked unsafe URL (SSRF protection): {url[:80]}")

        if not HTTPX_AVAILABLE:
            raise RuntimeError("httpx is required for WeCom media download")

        client = self._http_client or httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        created_client = client is not self._http_client
        try:
            async with client.stream(
                "GET",
                url,
                headers={
                    "User-Agent": "HermesAgent/1.0",
                    "Accept": "*/*",
                },
            ) as response:
                response.raise_for_status()
                headers = {key.lower(): value for key, value in response.headers.items()}
                content_length = headers.get("content-length")
                if content_length and content_length.isdigit() and int(content_length) > max_bytes:
                    raise ValueError(
                        f"Remote media exceeds WeCom limit: {int(content_length)} bytes > {max_bytes} bytes"
                    )

                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data.extend(chunk)
                    if len(data) > max_bytes:
                        raise ValueError(
                            f"Remote media exceeds WeCom limit while downloading: {len(data)} bytes > {max_bytes} bytes"
                        )

                return bytes(data), headers
        finally:
            if created_client:
                await client.aclose()

    @staticmethod
    def _looks_like_url(media_source: str) -> bool:
        parsed = urlparse(str(media_source or ""))
        return parsed.scheme in {"http", "https"}

    async def _load_outbound_media(
        self,
        media_source: str,
        file_name: Optional[str] = None,
    ) -> Tuple[bytes, str, str]:
        source = str(media_source or "").strip()
        if not source:
            raise ValueError("media source is required")
        if re.fullmatch(r"<[^>\n]+>", source):
            raise ValueError(f"Media placeholder was not replaced with a real file path: {source}")

        parsed = urlparse(source)
        if parsed.scheme in {"http", "https"}:
            data, headers = await self._download_remote_bytes(source, max_bytes=ABSOLUTE_MAX_BYTES)
            content_disposition = headers.get("content-disposition")
            resolved_name = file_name or self._guess_filename(source, content_disposition, headers.get("content-type", ""))
            content_type = self._normalize_content_type(headers.get("content-type", ""), resolved_name)
            return data, content_type, resolved_name

        if parsed.scheme == "file":
            local_path = Path(unquote(parsed.path)).expanduser()
        else:
            local_path = Path(source).expanduser()

        if not local_path.is_absolute():
            local_path = (Path.cwd() / local_path).resolve()

        if not local_path.exists() or not local_path.is_file():
            raise FileNotFoundError(f"Media file not found: {local_path}")

        data = local_path.read_bytes()
        resolved_name = file_name or local_path.name
        content_type = self._normalize_content_type("", resolved_name)
        return data, content_type, resolved_name

    async def _prepare_outbound_media(
        self,
        media_source: str,
        file_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        data, content_type, resolved_name = await self._load_outbound_media(media_source, file_name=file_name)
        detected_type = self._detect_wecom_media_type(content_type)
        size_check = self._apply_file_size_limits(len(data), detected_type, content_type)
        return {
            "data": data,
            "content_type": content_type,
            "file_name": resolved_name,
            "detected_type": detected_type,
            **size_check,
        }

    async def _upload_media_bytes(self, data: bytes, media_type: str, filename: str) -> Dict[str, Any]:
        if not data:
            raise ValueError("Cannot upload empty media")

        total_size = len(data)
        total_chunks = (total_size + UPLOAD_CHUNK_SIZE - 1) // UPLOAD_CHUNK_SIZE
        if total_chunks > MAX_UPLOAD_CHUNKS:
            raise ValueError(
                f"File too large: {total_chunks} chunks exceeds maximum of {MAX_UPLOAD_CHUNKS} chunks"
            )

        init_response = await self._send_request(
            APP_CMD_UPLOAD_MEDIA_INIT,
            {
                "type": media_type,
                "filename": filename,
                "total_size": total_size,
                "total_chunks": total_chunks,
                "md5": hashlib.md5(data).hexdigest(),
            },
        )
        self._raise_for_wecom_error(init_response, "media upload init")

        init_body = init_response.get("body") if isinstance(init_response.get("body"), dict) else {}
        upload_id = str(init_body.get("upload_id") or "").strip()
        if not upload_id:
            raise RuntimeError(f"media upload init failed: missing upload_id in response {init_response}")

        for chunk_index, start in enumerate(range(0, total_size, UPLOAD_CHUNK_SIZE)):
            chunk = data[start : start + UPLOAD_CHUNK_SIZE]
            chunk_response = await self._send_request(
                APP_CMD_UPLOAD_MEDIA_CHUNK,
                {
                    "upload_id": upload_id,
                    # Match the official SDK implementation, which currently uses 0-based chunk indexes.
                    "chunk_index": chunk_index,
                    "base64_data": base64.b64encode(chunk).decode("ascii"),
                },
            )
            self._raise_for_wecom_error(chunk_response, f"media upload chunk {chunk_index}")

        finish_response = await self._send_request(
            APP_CMD_UPLOAD_MEDIA_FINISH,
            {"upload_id": upload_id},
        )
        self._raise_for_wecom_error(finish_response, "media upload finish")

        finish_body = finish_response.get("body") if isinstance(finish_response.get("body"), dict) else {}
        media_id = str(finish_body.get("media_id") or "").strip()
        if not media_id:
            raise RuntimeError(f"media upload finish failed: missing media_id in response {finish_response}")

        return {
            "type": str(finish_body.get("type") or media_type),
            "media_id": media_id,
            "created_at": finish_body.get("created_at"),
        }

    async def _send_media_message(self, chat_id: str, media_type: str, media_id: str) -> Dict[str, Any]:
        response = await self._send_request(
            APP_CMD_SEND,
            {
                "chatid": chat_id,
                "msgtype": media_type,
                media_type: {"media_id": media_id},
            },
        )
        self._raise_for_wecom_error(response, "send media message")
        return response

    # ------------------------------------------------------------------
    # 模板卡片（template_card）支持
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_template_cards(text: str) -> Optional[Tuple[List[Dict[str, Any]], str]]:
        """从 LLM 回复文本中提取模板卡片 JSON 代码块。

        匹配规则：
        1. 扫描所有 ```json ... ``` 或 ``` ... ``` 代码块
        2. 尝试 JSON.parse，检查是否包含合法的 card_type
        3. 合法卡片从原文移除并返回；不合法保留原文

        Returns:
            (cards, remaining_text) — 卡片列表和去除卡片代码块后的剩余文本。
            若未找到任何卡片则返回 None。
        """
        if not text or not text.strip():
            return None

        cards: List[Dict[str, Any]] = []
        blocks_to_remove: List[str] = []

        for match in _TEMPLATE_CARD_BLOCK_RE.finditer(text):
            full_match = match.group(0)
            json_content = match.group(1).strip()
            if not json_content:
                continue

            try:
                parsed = json.loads(json_content)
            except (json.JSONDecodeError, ValueError, TypeError):
                continue

            if not isinstance(parsed, dict):
                continue

            card_type = parsed.get("card_type")
            if not isinstance(card_type, str) or card_type not in VALID_CARD_TYPES:
                continue

            # 确保主要字段存在
            if "template_card" not in parsed:
                # 兼容两种格式：直接 card_type 在外层或 template_card 嵌套
                parsed = {"template_card": parsed}

            cards.append(parsed)
            blocks_to_remove.append(full_match)

        if not cards:
            return None

        # 从原文中移除已提取的代码块
        remaining_text = text
        for block in blocks_to_remove:
            remaining_text = remaining_text.replace(block, "", 1)

        # 清理多余空行
        remaining_text = re.sub(r"\n{3,}", "\n\n", remaining_text).strip()

        logger.info(
            "[WeCom] Extracted %d template card(s) from response "
            "(original=%d chars, remaining=%d chars)",
            len(cards), len(text), len(remaining_text),
        )
        return cards, remaining_text

    async def _send_template_card_message(
        self,
        chat_id: str,
        card: Dict[str, Any],
        reply_req_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """通过 WebSocket 发送一条模板卡片消息。

        优先使用 reply channel（有 reply_req_id），否则用 proactive send。
        """
        template_card_body = card.get("template_card", card)
        body = {
            "msgtype": "template_card",
            "template_card": template_card_body,
        }

        try:
            if reply_req_id:
                response = await self._send_reply_request(
                    reply_req_id,
                    body,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            else:
                response = await self._send_request(
                    APP_CMD_SEND,
                    {"chatid": chat_id, **body},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            errcode = response.get("errcode", 0)
            if errcode not in (0, None):
                logger.warning(
                    "[%s] Template card send failed (errcode=%s): %s",
                    self.name, errcode, response.get("errmsg", ""),
                )
                return None
            logger.info("[%s] Template card sent: card_type=%s", self.name, template_card_body.get("card_type"))
            return response
        except Exception as exc:
            logger.warning("[%s] Template card send error: %s", self.name, exc)
            return None

    async def _detect_and_send_template_cards(
        self,
        content: str,
        chat_id: str,
        reply_req_id: Optional[str] = None,
    ) -> str:
        """从内容中检测模板卡片 JSON 代码块，发送后返回剩余文本。

        如果未检测到卡片，原样返回 content。
        """
        result = self._extract_template_cards(content)
        if result is None:
            return content

        cards, remaining_text = result
        for card in cards:
            await self._send_template_card_message(
                chat_id, card, reply_req_id=reply_req_id,
            )

        return remaining_text

    @staticmethod
    def mask_template_card_blocks(text: str) -> str:
        """遮罩流式中间帧中的模板卡片 JSON 代码块。

        已闭合的代码块（含 card_type）→ "📋 *正在生成卡片消息...*"
        未闭合的代码块尾部 → 截断
        非模板卡片代码块 → 保留
        """
        if not text:
            return text

        masked = text

        # 处理已闭合的代码块
        masked = _TEMPLATE_CARD_BLOCK_RE.sub(
            lambda m: "\n\n📋 *正在生成卡片消息...*\n\n"
            if '"card_type"' in m.group(1) or "'card_type'" in m.group(1)
            else m.group(0),
            masked,
        )

        # 处理未闭合的代码块尾部
        unclosed_match = _TEMPLATE_CARD_UNCLOSED_RE.search(masked)
        if unclosed_match:
            unclosed_content = unclosed_match.group(0)
            if '"card_type"' in unclosed_content or "'card_type'" in unclosed_content:
                masked = masked[:unclosed_match.start()] + "\n\n📋 *正在生成卡片消息...*"

        return masked

    @staticmethod
    def _format_template_card_event_text(body: Dict[str, Any], card_event: Dict[str, Any]) -> str:
        """将模板卡片事件回调格式化为可继续路由给大模型的文本。"""
        if not isinstance(card_event, dict):
            return ""
        selected_items = card_event.get("selected_items") if isinstance(card_event.get("selected_items"), dict) else {}
        raw_selected = selected_items.get("selected_item") if isinstance(selected_items.get("selected_item"), list) else []
        selected_lines = []
        for item in raw_selected:
            if not isinstance(item, dict):
                continue
            qk = str(item.get("question_key") or "").strip() or "unknown_question"
            raw_ids = item.get("option_ids") if isinstance(item.get("option_ids"), dict) else {}
            ids = raw_ids.get("option_id") if isinstance(raw_ids.get("option_id"), list) else []
            selected_lines.append(
                f"- {qk}: {', '.join(str(i) for i in ids if i) if ids else '(未选择)'}"
            )

        sender = body.get("from") if isinstance(body.get("from"), dict) else {}
        sender_userid = str(sender.get("userid") or "")
        sender_corpid = str(sender.get("corpid") or "")
        chatid = str(body.get("chatid") or sender_userid)

        lines = [
            "[企业微信模板卡片回调]",
            f"event_type(事件类型): template_card_event",
            f"msgid(消息 id): {body.get('msgid')}" if body.get("msgid") else None,
            f"chat_type(会话类型): {body.get('chattype')}" if body.get("chattype") else None,
            f"chat_id(会话 id): {chatid}" if chatid else None,
            f"from.corpid(企业 id): {sender_corpid}" if sender_corpid else None,
            f"from.userid(发送人 id): {sender_userid}" if sender_userid else None,
            f"card_type(卡片类型): {card_event.get('card_type')}" if card_event.get("card_type") else None,
            f"event_key(事件 key): {card_event.get('event_key')}" if card_event.get("event_key") else None,
            f"task_id(任务 id): {card_event.get('task_id')}" if card_event.get("task_id") else None,
            "selected_items(选择项):" if selected_lines else "selected_items(选择项): []",
            *selected_lines,
        ]
        return "\n".join(line for line in lines if line is not None)

    # ------------------------------------------------------------------
    # Agent HTTP API — 双通道回退
    # ------------------------------------------------------------------

    _API_GET_TOKEN = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    _API_SEND_MESSAGE = "https://qyapi.weixin.qq.com/cgi-bin/message/send"
    _API_SEND_APPCHAT = "https://qyapi.weixin.qq.com/cgi-bin/appchat/send"
    _API_UPLOAD_MEDIA = "https://qyapi.weixin.qq.com/cgi-bin/media/upload"

    async def _get_agent_token(self) -> str:
        """获取 Agent API AccessToken，带缓存和自动刷新。"""
        now = asyncio.get_running_loop().time()
        if self._agent_token and self._agent_token_expires_at > now + 60:
            return self._agent_token

        async with self._agent_token_refresh_lock:
            # 双重检查
            if self._agent_token and self._agent_token_expires_at > now + 60:
                return self._agent_token

            url = (
                f"{self._API_GET_TOKEN}?corpid={_urlesc(self._agent_corp_id)}"
                f"&corpsecret={_urlesc(self._agent_corp_secret)}"
            )
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
                data = resp.json()

            errcode = data.get("errcode", -1)
            if errcode != 0 or not data.get("access_token"):
                raise RuntimeError(
                    f"Agent gettoken failed: errcode={errcode} errmsg={data.get('errmsg', '?')}"
                )
            self._agent_token = data["access_token"]
            expires_in = data.get("expires_in", 7200)
            self._agent_token_expires_at = now + expires_in
            logger.info("[%s] Agent token refreshed (expires in %ds)", self.name, expires_in)
            return self._agent_token

    @staticmethod
    def _resolve_agent_target(chat_id: str) -> Dict[str, str]:
        """解析 chat_id 为 Agent API 的接收目标参数。

        支持的格式：
        - user:xxx → {"touser": "xxx"}
        - party:xxx → {"toparty": "xxx"}
        - tag:xxx → {"totag": "xxx"}
        - group:xxx / chat:xxx → {"chatid": "xxx"}
        - 默认 → {"touser": chat_id}
        """
        chat_id = chat_id.strip()
        if chat_id.startswith("party:") or chat_id.startswith("dept:"):
            return {"toparty": chat_id.split(":", 1)[1].strip()}
        if chat_id.startswith("tag:"):
            return {"totag": chat_id.split(":", 1)[1].strip()}
        if chat_id.startswith("group:") or chat_id.startswith("chat:"):
            return {"chatid": chat_id.split(":", 1)[1].strip()}
        if chat_id.startswith("user:"):
            return {"touser": chat_id.split(":", 1)[1].strip()}
        # 启发式：以 wr/wc 开头视为群聊
        if re.match(r"^(wr|wc)", chat_id, re.IGNORECASE):
            return {"chatid": chat_id}
        # 默认视为用户ID
        return {"touser": chat_id}

    async def _send_agent_text(self, chat_id: str, content: str) -> Dict[str, Any]:
        """通过 Agent HTTP API 发送文本消息。"""
        token = await self._get_agent_token()
        target = self._resolve_agent_target(chat_id)
        is_chat = "chatid" in target

        if is_chat:
            body = {
                **target,
                "msgtype": "text",
                "text": {"content": content},
            }
            url = f"{self._API_SEND_APPCHAT}?access_token={_urlesc(token)}"
        else:
            body = {
                **target,
                "msgtype": "text",
                "agentid": self._agent_id,
                "text": {"content": content},
            }
            url = f"{self._API_SEND_MESSAGE}?access_token={_urlesc(token)}"

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=body)
            data = resp.json()

        errcode = data.get("errcode", -1)
        if errcode != 0:
            raise RuntimeError(
                f"Agent send text failed: errcode={errcode} errmsg={data.get('errmsg', '?')}"
            )
        # 检查部分失败
        invalid = []
        for key in ("invaliduser", "invalidparty", "invalidtag"):
            val = data.get(key)
            if val:
                invalid.append(f"{key}={val}")
        if invalid:
            logger.warning(
                "[%s] Agent send partial failure: %s", self.name, ", ".join(invalid)
            )
        logger.info("[%s] Agent text sent via HTTP API to %s", self.name, chat_id)
        return data

    async def _upload_agent_media(
        self, data: bytes, media_type: str, filename: str,
    ) -> str:
        """通过 Agent HTTP API 上传临时素材，返回 media_id。"""
        token = await self._get_agent_token()
        url = f"{self._API_UPLOAD_MEDIA}?access_token={_urlesc(token)}&type={_urlesc(media_type)}"
        mime_map = {
            "image": "image/png",
            "voice": "audio/amr",
            "video": "video/mp4",
            "file": "application/octet-stream",
        }
        content_type = mime_map.get(media_type, "application/octet-stream")

        # 用 httpx 的 multipart 上传
        files = {"media": (filename, data, content_type)}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, files=files)
            result = resp.json()

        if not result.get("media_id"):
            raise RuntimeError(
                f"Agent upload failed: errcode={result.get('errcode')} "
                f"errmsg={result.get('errmsg', '?')}"
            )
        logger.info(
            "[%s] Agent media uploaded: type=%s filename=%s media_id=%s",
            self.name, media_type, filename, result["media_id"][:16],
        )
        return result["media_id"]

    async def _send_agent_media(
        self, chat_id: str, media_type: str, media_id: str,
    ) -> Dict[str, Any]:
        """通过 Agent HTTP API 发送已上传的媒体消息。"""
        token = await self._get_agent_token()
        target = self._resolve_agent_target(chat_id)
        is_chat = "chatid" in target

        media_payload = {"media_id": media_id}
        if is_chat:
            body = {
                **target,
                "msgtype": media_type,
                media_type: media_payload,
            }
            url = f"{self._API_SEND_APPCHAT}?access_token={_urlesc(token)}"
        else:
            body = {
                **target,
                "msgtype": media_type,
                "agentid": self._agent_id,
                media_type: media_payload,
            }
            url = f"{self._API_SEND_MESSAGE}?access_token={_urlesc(token)}"

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=body)
            data = resp.json()

        errcode = data.get("errcode", -1)
        if errcode != 0:
            raise RuntimeError(
                f"Agent send {media_type} failed: errcode={errcode} errmsg={data.get('errmsg', '?')}"
            )
        logger.info("[%s] Agent %s sent via HTTP to %s", self.name, media_type, chat_id)
        return data

    async def _agent_markdown_unsupported(self, chat_id: str, content: str) -> Dict[str, Any]:
        """Agent API 不支持 markdown（仅支持 text），转成纯文本发送。"""
        # 去除 markdown 格式字符，保留核心内容
        plain = re.sub(r"[*_~`#>]", "", content)
        plain = re.sub(r"\n{3,}", "\n\n", plain).strip()
        return await self._send_agent_text(chat_id, plain[:self.MAX_MESSAGE_LENGTH])

    # ------------------------------------------------------------------
    # MCP (Model Context Protocol) — 企业微信内置工具集成
    # ------------------------------------------------------------------

    _MCP_GET_CONFIG_CMD = "aibot_get_mcp_config"
    _MCP_PROTOCOL_VERSION = "2025-03-26"
    _MCP_CLIENT_NAME = "hermes_wecom_mcp"
    _MCP_CLIENT_VERSION = "1.0.0"
    _MCP_REQUEST_TIMEOUT = 30.0
    _MCP_INIT_TIMEOUT = 15.0

    @staticmethod
    def _mcp_cache_key(account_id: str, category: str) -> str:
        return f"{account_id}:{category}"

    async def _fetch_mcp_config(self, category: str) -> Dict[str, Any]:
        """通过 WS 拉取指定品类的 MCP 配置（Server URL）。"""
        if not self._ws or self._ws.closed:
            raise RuntimeError("WebSocket not connected, cannot fetch MCP config")
        req_id = self._new_req_id("mcp_config")
        future = asyncio.get_running_loop().create_future()
        self._pending_responses[req_id] = future
        try:
            await self._send_json({
                "cmd": self._MCP_GET_CONFIG_CMD,
                "headers": {"req_id": req_id},
                "body": {
                    "biz_type": category,
                    "plugin_version": "1.0.0",
                },
            })
            response = await asyncio.wait_for(future, timeout=REQUEST_TIMEOUT_SECONDS)
            errcode = response.get("errcode", -1)
            if errcode not in (0, None):
                raise RuntimeError(
                    f"MCP config fetch failed: errcode={errcode} errmsg={response.get('errmsg', '?')}"
                )
            body = response.get("body") if isinstance(response.get("body"), dict) else {}
            url = str(body.get("url") or "").strip()
            if not url:
                raise RuntimeError(f"MCP config response missing url field (category={category})")
            logger.info(
                "[%s] MCP config fetched: category=%s url=%s",
                self.name, category, url,
            )
            return body
        except asyncio.TimeoutError:
            raise RuntimeError(f"MCP config fetch timed out for category={category}")
        finally:
            self._pending_responses.pop(req_id, None)

    async def _mcp_http_request(
        self,
        url: str,
        rpc_body: Dict[str, Any],
        session_id: Optional[str] = None,
        timeout: float = _MCP_REQUEST_TIMEOUT,
        requester_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """发送 JSON-RPC 请求到 MCP Server（Streamable HTTP）。"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "User-Agent": "HermesWeCom/1.0",
        }
        if session_id:
            headers["Mcp-Session-Id"] = session_id
        if requester_user_id:
            headers["x-openclaw-wecom-userid"] = requester_user_id

        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.post(url, json=rpc_body, headers=headers)

        # 提取新的 session ID
        new_session_id = resp.headers.get("mcp-session-id")

        if resp.status_code == 204:
            return {"result": None, "new_session_id": new_session_id}

        content_type = (resp.headers.get("content-type") or "").lower()

        # SSE 响应
        if "text/event-stream" in content_type:
            text = resp.text
            result = self._parse_mcp_sse(text)
            return {"result": result, "new_session_id": new_session_id}

        # 普通 JSON 响应
        text = resp.text.strip()
        if not text:
            return {"result": None, "new_session_id": new_session_id}

        try:
            rpc = resp.json()
        except (json.JSONDecodeError, ValueError):
            raise RuntimeError(f"MCP non-JSON response (HTTP {resp.status_code})")

        if not isinstance(rpc, dict):
            raise RuntimeError(f"MCP unexpected response type: {type(rpc).__name__}")

        if "error" in rpc:
            err = rpc["error"]
            code = err.get("code", -1)
            msg = err.get("message", "unknown error")
            raise RuntimeError(f"MCP RPC error [{code}]: {msg}")

        return {"result": rpc.get("result"), "new_session_id": new_session_id}

    @staticmethod
    def _parse_mcp_sse(text: str) -> Optional[Any]:
        """解析 SSE 流式响应，取最后一个事件的数据。"""
        lines = text.split("\n")
        current_parts: List[str] = []
        last_data = ""
        for line in lines:
            if line.startswith("data: "):
                current_parts.append(line[6:])
            elif line.startswith("data:"):
                current_parts.append(line[5:])
            elif line.strip() == "" and current_parts:
                last_data = "\n".join(current_parts).strip()
                current_parts = []
        if current_parts:
            last_data = "\n".join(current_parts).strip()
        if not last_data:
            raise RuntimeError("SSE response contains no valid data")
        rpc = json.loads(last_data)
        if isinstance(rpc, dict) and "error" in rpc:
            err = rpc["error"]
            raise RuntimeError(f"MCP SSE RPC error [{err.get('code')}]: {err.get('message')}")
        if isinstance(rpc, dict):
            return rpc.get("result")
        return rpc

    async def _mcp_initialize(
        self,
        url: str,
        account_id: str,
        category: str,
        requester_user_id: Optional[str] = None,
    ) -> Optional[str]:
        """执行 Streamable HTTP initialize 握手，返回 session_id（无状态则返回 None）。"""
        init_body = {
            "jsonrpc": "2.0",
            "id": self._new_req_id("mcp_init"),
            "method": "initialize",
            "params": {
                "protocolVersion": self._MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": self._MCP_CLIENT_NAME,
                    "version": self._MCP_CLIENT_VERSION,
                },
            },
        }
        result = await self._mcp_http_request(
            url, init_body, timeout=self._MCP_INIT_TIMEOUT,
            requester_user_id=requester_user_id,
        )
        session_id = result.get("new_session_id")

        # 发送 initialized 通知（无状态 server 没有 session_id 也要发）
        notify_body = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }
        await self._mcp_http_request(
            url, notify_body, session_id=session_id,
            timeout=self._MCP_INIT_TIMEOUT,
            requester_user_id=requester_user_id,
        )

        logger.info(
            "[%s] MCP initialized: category=%s session_id=%s",
            self.name, category, session_id or "(stateless)",
        )
        return session_id

    _mcp_config_cache: Dict[str, Dict[str, Any]] = {}
    _mcp_session_cache: Dict[str, Optional[str]] = {}  # category → session_id or None
    _mcp_init_locks: Dict[str, asyncio.Lock] = {}

    async def _ensure_mcp_session(
        self,
        category: str,
        requester_user_id: Optional[str] = None,
    ) -> Tuple[str, Optional[str]]:
        """获取 MCP Server URL 和有效 session。自动初始化/复用会话。"""
        account_id = self._bot_id  # 用 bot_id 作为账户标识
        key = self._mcp_cache_key(account_id, category)

        # 1. 获取/缓存配置（URL）
        if key not in self._mcp_config_cache:
            config = await self._fetch_mcp_config(category)
            self._mcp_config_cache[key] = config
        url = str(self._mcp_config_cache[key].get("url") or "").strip()

        # 2. 获取/初始化会话
        existing_session = self._mcp_session_cache.get(key)
        if existing_session is not None:
            return url, existing_session or None  # None = 无状态

        if key not in self._mcp_init_locks:
            self._mcp_init_locks[key] = asyncio.Lock()
        async with self._mcp_init_locks[key]:
            if key in self._mcp_session_cache:
                return url, self._mcp_session_cache[key] or None
            session_id = await self._mcp_initialize(url, account_id, category, requester_user_id)
            self._mcp_session_cache[key] = session_id or ""  # "" = 无状态
            return url, session_id

    async def send_mcp_list(
        self,
        category: str,
        requester_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """列出指定品类的所有 MCP 工具。"""
        url, session_id = await self._ensure_mcp_session(category, requester_user_id)
        rpc_body = {
            "jsonrpc": "2.0",
            "id": self._new_req_id("mcp_list"),
            "method": "tools/list",
        }
        result = await self._mcp_http_request(url, rpc_body, session_id, requester_user_id=requester_user_id)
        return result.get("result") or {}

    async def send_mcp_call(
        self,
        category: str,
        method: str,
        args: Dict[str, Any],
        requester_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """调用指定品类的某个 MCP 工具。"""
        url, session_id = await self._ensure_mcp_session(category, requester_user_id)
        rpc_body = {
            "jsonrpc": "2.0",
            "id": self._new_req_id("mcp_call"),
            "method": "tools/call",
            "params": {
                "name": method,
                "arguments": args or {},
            },
        }
        result = await self._mcp_http_request(url, rpc_body, session_id, requester_user_id=requester_user_id)
        return result.get("result") or {}

    async def _send_reply_markdown(self, reply_req_id: str, content: str) -> Dict[str, Any]:
        response = await self._send_reply_request(
            reply_req_id,
            {
                "msgtype": "markdown",
                "markdown": {"content": content[:self.MAX_MESSAGE_LENGTH]},
            },
        )
        self._raise_for_wecom_error(response, "send reply markdown")
        return response

    async def _send_reply_stream(
        self,
        reply_req_id: str,
        content: str,
        stream_id: Optional[str] = None,
        finish: bool = True,
    ) -> Dict[str, Any]:
        """Send one chunk of WeCom's native reply-mode stream.

        Final chunks (``finish=True``) go through :meth:`_send_reply_request`
        so the caller gets an ACK and any WeCom error is raised. Intermediate
        chunks (``finish=False``) are fire-and-forget via :meth:`_send_json`
        — WeCom AI Bot does not send per-chunk ACKs, and waiting for one
        would stall the stream.

        When the WeCom server returns errcode 846608 (stream expired, usually
        after ~6 minutes of inactivity), falls back to sending the content as
        a final markdown reply via :meth:`_send_reply_markdown`.
        """
        stream_id = stream_id or self._new_req_id("stream")
        logger.info(
            "[%s] _send_reply_stream: len=%d finish=%s stream_id=%s reply_req_id=%s",
            self.name, len(content), finish, stream_id, reply_req_id,
        )
        stream_payload = {
            "msgtype": "stream",
            "stream": {
                "id": stream_id,
                "finish": finish,
                "content": content[:self.MAX_MESSAGE_LENGTH],
            },
        }
        if finish:
            response = await self._send_reply_request(reply_req_id, stream_payload)
            errcode = response.get("errcode", 0)
            if errcode == STREAM_EXPIRED_ERRCODE:
                logger.warning(
                    "[%s] Stream expired (errcode=%d) for reply_req_id=%s, "
                    "falling back to markdown reply",
                    self.name, STREAM_EXPIRED_ERRCODE, reply_req_id,
                )
                await asyncio.sleep(_STREAM_EXPIRED_RETRY_GRACE)
                response = await self._send_reply_markdown(reply_req_id, content)
                return response
            self._raise_for_wecom_error(response, "send reply stream")
            return response

        await self._send_json(
            {
                "cmd": APP_CMD_RESPONSE,
                "headers": {"req_id": str(reply_req_id).strip()},
                "body": stream_payload,
            }
        )
        return {"headers": {"req_id": reply_req_id}}

    async def _send_reply_stream_non_blocking(
        self,
        reply_req_id: str,
        content: str,
        stream_id: Optional[str] = None,
        finish: bool = True,
    ) -> Dict[str, Any]:
        """Non-blocking variant of :meth:`_send_reply_stream`.

        If the previous non-final frame for this ``reply_req_id`` stream has
        not yet been acknowledged (i.e. no finish=True has been sent yet and
        a send is already in-flight), intermediate frames return a dict with
        key ``"skipped"`` set to ``True`` so the caller can avoid blocking.

        Final frames (``finish=True``) are never skipped — they always send,
        because otherwise the stream would hang open forever.
        """
        if not finish:
            # Check if there is already an in-flight stream chunk pending
            # for this reply_req_id by looking for a pending future.
            pending_future = self._pending_responses.get(reply_req_id)
            if pending_future and not pending_future.done():
                logger.info(
                    "[%s] _send_reply_stream_non_blocking: "
                    "SKIPPED non-final frame for reply_req_id=%s "
                    "(previous frame not yet acked)",
                    self.name, reply_req_id,
                )
                return {"skipped": True, "headers": {"req_id": reply_req_id}}

        return await self._send_reply_stream(
            reply_req_id=reply_req_id,
            content=content,
            stream_id=stream_id,
            finish=finish,
        )

    async def _send_reply_media_message(
        self,
        reply_req_id: str,
        media_type: str,
        media_id: str,
    ) -> Dict[str, Any]:
        response = await self._send_reply_request(
            reply_req_id,
            {
                "msgtype": media_type,
                media_type: {"media_id": media_id},
            },
        )
        self._raise_for_wecom_error(response, "send reply media message")
        return response

    async def _send_followup_markdown(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
    ) -> Optional[SendResult]:
        if not content:
            return None
        result = await self.send(chat_id=chat_id, content=content, reply_to=reply_to)
        if not result.success:
            logger.warning("[%s] Follow-up markdown send failed: %s", self.name, result.error)
        return result

    async def _send_media_source(
        self,
        chat_id: str,
        media_source: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
    ) -> SendResult:
        if not chat_id:
            return SendResult(success=False, error="chat_id is required")

        try:
            prepared = await self._prepare_outbound_media(media_source, file_name=file_name)
        except FileNotFoundError as exc:
            return SendResult(success=False, error=str(exc))
        except Exception as exc:
            logger.error("[%s] Failed to prepare outbound media %s: %s", self.name, media_source, exc)
            return SendResult(success=False, error=str(exc))

        if prepared["rejected"]:
            await self._send_followup_markdown(
                chat_id,
                f"⚠️ {prepared['reject_reason']}",
                reply_to=reply_to,
            )
            return SendResult(success=False, error=prepared["reject_reason"])

        reply_req_id = self._reply_req_id_for_message(reply_to)
        if not reply_req_id and chat_id in self._last_chat_req_ids:
            reply_req_id = self._last_chat_req_ids[chat_id]

        ws_available = self._ws is not None and not self._ws.closed
        if not ws_available and self._agent_configured:
            # WS 不可用，通过 Agent HTTP API 上传和发送媒体
            logger.warning(
                "[%s] WebSocket not available, sending media via Agent HTTP API to %s",
                self.name, chat_id,
            )
            try:
                media_id = await self._upload_agent_media(
                    prepared["data"],
                    prepared["final_type"],
                    prepared["file_name"],
                )
                await self._send_agent_media(
                    chat_id,
                    prepared["final_type"],
                    media_id,
                )
                media_response = {"errcode": 0, "media_id": media_id}
            except Exception as exc:
                logger.error("[%s] Agent media send failed: %s", self.name, exc)
                return SendResult(success=False, error=str(exc))
        else:
            try:
                upload_result = await self._upload_media_bytes(
                    prepared["data"],
                    prepared["final_type"],
                    prepared["file_name"],
                )
                if reply_req_id:
                    media_response = await self._send_reply_media_message(
                        reply_req_id,
                        prepared["final_type"],
                        upload_result["media_id"],
                    )
                else:
                    media_response = await self._send_media_message(
                        chat_id,
                        prepared["final_type"],
                        upload_result["media_id"],
                    )
            except asyncio.TimeoutError:
                return SendResult(success=False, error="Timeout sending media to WeCom")
            except Exception as exc:
                logger.error("[%s] Failed to send media %s: %s", self.name, media_source, exc)
                return SendResult(success=False, error=str(exc))

        caption_result = None
        downgrade_result = None
        if caption:
            caption_result = await self._send_followup_markdown(
                chat_id,
                caption,
                reply_to=reply_to,
            )
        if prepared["downgraded"] and prepared["downgrade_note"]:
            downgrade_result = await self._send_followup_markdown(
                chat_id,
                f"ℹ️ {prepared['downgrade_note']}",
                reply_to=reply_to,
            )

        return SendResult(
            success=True,
            message_id=self._payload_req_id(media_response) or uuid.uuid4().hex[:12],
            raw_response={
                "upload": upload_result,
                "media": media_response,
                "caption": caption_result.raw_response if caption_result else None,
                "caption_error": caption_result.error if caption_result and not caption_result.success else None,
                "downgrade": downgrade_result.raw_response if downgrade_result else None,
                "downgrade_error": downgrade_result.error if downgrade_result and not downgrade_result.success else None,
            },
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send markdown to a WeCom chat.

        Uses passive reply-mode (``aibot_respond_msg``) when a reply context
        exists, otherwise proactive ``aibot_send_msg``. When ``metadata``
        contains ``{"streaming": True}`` and a reply context is available,
        the first chunk is sent with ``finish=False`` and the stream id is
        stashed in ``self._active_streams`` so later
        :meth:`edit_message` / :meth:`finalize_stream` calls can continue
        the same native WebSocket stream (no client-visible message edits).

        Native streaming is a WeCom reply-channel feature; proactive sends
        ignore the ``streaming`` flag and always deliver as a single message.
        """
        streaming = bool(metadata.get("streaming")) if metadata else False
        # Native-streaming adapters (``native_streaming_unified``) must always
        # use the reply-channel stream when a reply context exists, even if
        # the caller didn't explicitly set ``metadata.streaming``.  Otherwise
        # the thinking bubble opened by send_typing() stays open forever:
        #   send_typing() → opens stream (bubble)
        #   send(streaming=False) → sends markdown, NOT through stream
        #   → stream never gets content nor finalize → ghost bubble
        if not streaming and getattr(self, "native_streaming_unified", False):
            has_reply_context = bool(
                self._reply_req_id_for_message(reply_to)
                or self._last_chat_req_ids.get(chat_id)
            )
            if has_reply_context:
                streaming = True
                logger.debug(
                    "[%s] send: auto-enabling streaming for native_streaming_unified adapter",
                    self.name,
                )
        logger.info(
            "[%s] send: chat_id=%s streaming=%s reply_to=%s content_len=%d",
            self.name, chat_id, streaming, reply_to, len(content),
        )

        # 检查 WebSocket 是否可用；不可用时回退到 Agent HTTP API
        ws_available = self._ws is not None and not self._ws.closed
        if not ws_available and self._agent_configured and not reply_to:
            logger.warning(
                "[%s] WebSocket not available, falling back to Agent HTTP API for send to %s",
                self.name, chat_id,
            )
            try:
                remaining = await self._detect_and_send_template_cards(content, chat_id)
                if not remaining:
                    remaining = " "
                response_data = await self._agent_markdown_unsupported(chat_id, remaining)
                return SendResult(
                    success=True,
                    message_id=f"agent-{uuid.uuid4().hex[:12]}",
                    raw_response=response_data,
                )
            except Exception as exc:
                logger.error("[%s] Agent fallback send failed: %s", self.name, exc)
                return SendResult(success=False, error=str(exc))

        if not chat_id:
            return SendResult(success=False, error="chat_id is required")

        stream_id: Optional[str] = None
        try:
            reply_req_id = self._reply_req_id_for_message(reply_to)

            if not reply_req_id and chat_id in self._last_chat_req_ids:
                # Only reuse the cached inbound req_id as reply context if:
                # 1. There's an active stream (within-stream continuation), OR
                # 2. No stream has been finalized yet (first independent reply).
                # Using the cached req_id after a previous stream was finalized
                # would cause the new message to overwrite the previous one on
                # WeCom's reply channel (same reply_req_id = same message target).
                if chat_id in self._active_streams or chat_id not in self._finalized_streams:
                    reply_req_id = self._last_chat_req_ids[chat_id]
                else:
                    logger.info(
                        "[%s] send: skipping _last_chat_req_ids fallback for %s "
                        "(stream already finalized, would overwrite previous message)",
                        self.name, chat_id,
                    )

            logger.info(
                "[%s] send: reply_req_id=%s last_req_ids_keys=%s",
                self.name, reply_req_id, list(self._last_chat_req_ids.keys()),
            )

            if reply_req_id:
                if streaming:
                    # Event callback messages cannot use reply-mode streams.
                    # Skip non-final frames entirely; finalize_stream()
                    # handles the final flush via proactive send.
                    if chat_id in self._event_callback_chats:
                        logger.info(
                            "[%s] send: skipping streaming chunk for event_callback chat %s",
                            self.name, chat_id,
                        )
                        return SendResult(
                            success=True,
                            message_id=reply_req_id or uuid.uuid4().hex[:12],
                        )
                    existing = self._active_streams.get(chat_id)
                    if existing:
                        # Stream already opened by send_typing, continue it
                        _, stream_id = existing
                    else:
                        # No typing bubble yet — open one now
                        stream_id = self._new_req_id("stream")
                        await self._send_reply_stream_non_blocking(
                            reply_req_id, "",
                            stream_id=stream_id, finish=False,
                        )
                        self._active_streams[chat_id] = (reply_req_id, stream_id)
                    response = await self._send_reply_stream_non_blocking(
                        reply_req_id,
                        content,
                        stream_id=stream_id,
                        finish=False,
                    )
                else:
                    # 检测并发送模板卡片（非流式回复路径）
                    remaining = await self._detect_and_send_template_cards(
                        content, chat_id, reply_req_id=reply_req_id,
                    )
                    if remaining:
                        response = await self._send_reply_markdown(reply_req_id, remaining)
                    else:
                        response = {"headers": {"req_id": reply_req_id}, "errcode": 0}
            else:
                # 检测并发送模板卡片（非流式主动发送路径）
                remaining = await self._detect_and_send_template_cards(content, chat_id)
                if remaining:
                    response = await self._send_request(
                        APP_CMD_SEND,
                        {
                            "chatid": chat_id,
                            "msgtype": "markdown",
                            "markdown": {"content": remaining[:self.MAX_MESSAGE_LENGTH]},
                        },
                    )
                else:
                    response = {"headers": {"req_id": self._new_req_id("tc")}, "errcode": 0}
        except asyncio.TimeoutError:
            return SendResult(success=False, error="Timeout sending message to WeCom")
        except Exception as exc:
            logger.error("[%s] Send failed: %s", self.name, exc)
            return SendResult(success=False, error=str(exc))

        error = self._response_error(response)
        if error:
            return SendResult(success=False, error=error)

        # In streaming reply-mode, reuse ``reply_req_id`` as the message_id so
        # ``edit_message`` / ``finalize_stream`` can look up the active stream.
        if streaming and stream_id and reply_req_id:
            self._active_streams[chat_id] = (reply_req_id, stream_id)
            message_id = reply_req_id
        else:
            message_id = self._payload_req_id(response) or uuid.uuid4().hex[:12]

        return SendResult(
            success=True,
            message_id=message_id,
            raw_response=response,
        )

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Continue an active native stream by sending another (non-final) chunk.

        WeCom has no "edit an already-delivered message" API; instead, its
        AI Bot protocol supports a multi-chunk stream identified by a
        ``stream_id`` on the reply channel. We map the gateway's generic
        ``edit_message`` to that protocol: each call delivers one more
        ``finish=False`` chunk under the existing ``stream_id``.

        Returns ``SendResult(success=False, ...)`` if the chat_id has no
        active stream (e.g. because the reply window has already been
        finalized, or the message was sent via the proactive path which
        does not support streaming).
        """
        stream_info = self._active_streams.get(chat_id)
        logger.info(
            "[%s] edit_message: message_id=%s stream_found=%s content_len=%d",
            self.name, message_id, bool(stream_info), len(content),
        )
        if not stream_info:
            return SendResult(success=False, error="no active stream for message")

        reply_req_id, stream_id = stream_info
        try:
            await self._send_reply_stream(
                reply_req_id, content, stream_id=stream_id, finish=False,
            )
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            logger.error("[%s] Stream edit failed: %s", self.name, exc)
            return SendResult(success=False, error=str(exc))

    async def finalize_stream(
        self,
        chat_id: str,
        message_id: str,
        content: str,
    ) -> SendResult:
        """Close an active native stream with a final (``finish=True``) chunk.

        The stream entry is removed from ``self._active_streams`` regardless
        of outcome to prevent leaks. Called by ``GatewayStreamConsumer`` at
        the end of a stream, on segment break, or on cancellation.

        For event callback chats (where streaming was skipped), sends the
        content as a proactive markdown message via APP_CMD_SEND.
        """
        # Event callback: no stream was opened; send proactively instead.
        if chat_id in self._event_callback_chats:
            self._event_callback_chats.discard(chat_id)
            logger.info(
                "[%s] finalize_stream: event_callback chat %s — sending proactively",
                self.name, chat_id,
            )
            try:
                # 先检测并发送模板卡片
                remaining = await self._detect_and_send_template_cards(content, chat_id)
                if remaining:
                    response = await self._send_request(
                        APP_CMD_SEND,
                        {
                            "chatid": chat_id,
                            "msgtype": "markdown",
                            "markdown": {"content": remaining[:self.MAX_MESSAGE_LENGTH]},
                        },
                    )
                    self._raise_for_wecom_error(response, "finalize_stream proactive send")
                else:
                    response = {"headers": {"req_id": uuid.uuid4().hex[:12]}, "errcode": 0}
                return SendResult(
                    success=True,
                    message_id=self._payload_req_id(response) or uuid.uuid4().hex[:12],
                    raw_response=response,
                )
            except Exception as exc:
                logger.error("[%s] finalize_stream proactive send failed: %s", self.name, exc)
                return SendResult(success=False, error=str(exc))

        stream_info = self._active_streams.pop(chat_id, None)
        logger.info(
            "[%s] finalize_stream: message_id=%s stream_found=%s content_len=%d",
            self.name, message_id, bool(stream_info), len(content),
        )
        if not stream_info:
            return SendResult(success=False, error="no active stream for message")

        reply_req_id, stream_id = stream_info
        # Mark as finalized so send_typing() doesn't reopen this stream
        # (e.g. from the progress-queue refresh loop).
        self._finalized_streams.add(chat_id)
        # Clear the cached inbound req_id so subsequent independent sends
        # to this chat create fresh messages via proactive APP_CMD_SEND
        # instead of reusing this stream's reply channel (which would
        # overwrite the finalized message on WeCom).  The next inbound
        # user message will repopulate _last_chat_req_ids automatically.
        self._last_chat_req_ids.pop(chat_id, None)
        if len(self._finalized_streams) > 100:
            self._finalized_streams.clear()
        # WeCom AI Bot errcode 6000 ("more than one callers at the same time")
        # fires when the finish=True frame arrives while the server is still
        # processing the previous fire-and-forget chunk. A short grace period
        # lets the server settle before we close the stream. The user has
        # already seen every chunk on their client; this only affects the
        # finalize ACK.
        await asyncio.sleep(0.25)
        try:
            # 先关闭 stream bubble（finish=True 发空内容），然后以 markdown 格式主动发送
            # 最终内容，确保可长按转发和表格左右滑动。stream 消息在 WeCom 客户端上
            # 不支持转发和表格滚动，而 markdown 消息（aibot_send_msg）支持。
            remaining = await self._detect_and_send_template_cards(
                content, chat_id, reply_req_id=reply_req_id,
            )
            if not remaining:
                remaining = " "
            # Step 1: 关闭 stream bubble（空内容 finish=True）
            try:
                await self._send_reply_stream(
                    reply_req_id, " ", stream_id=stream_id, finish=True,
                )
            except Exception as exc:
                logger.warning(
                    "[%s] finalize_stream: close stream bubble failed (non-fatal): %s",
                    self.name, exc,
                )
            # Step 2: 以 markdown 格式发送实际内容（可转发、可滚动表格）
            markdown_result = await self._send_request(
                APP_CMD_SEND,
                {
                    "chatid": chat_id,
                    "msgtype": "markdown",
                    "markdown": {"content": remaining[:self.MAX_MESSAGE_LENGTH]},
                },
            )
            errcode = markdown_result.get("errcode", 0)
            if errcode != 0:
                # Proactive send failed (e.g. group chat restriction) —
                # fall back to reply-mode markdown
                logger.warning(
                    "[%s] finalize_stream: proactive send failed (errcode=%d), "
                    "falling back to reply-mode markdown",
                    self.name, errcode,
                )
                markdown_result = await self._send_reply_markdown(reply_req_id, remaining)
            response = markdown_result
            # Notify any waiters on the ack event for this reply_req_id.
            ack_event = self._stream_ack_events.get(reply_req_id)
            if ack_event:
                ack_event.set()
            return SendResult(success=True, message_id=message_id, raw_response=response)
        except Exception as exc:
            logger.error("[%s] Stream finalize failed: %s", self.name, exc)
            ack_event = self._stream_ack_events.get(reply_req_id)
            if ack_event:
                ack_event.set()
            return SendResult(success=False, error=str(exc))

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del metadata

        result = await self._send_media_source(
            chat_id=chat_id,
            media_source=image_url,
            caption=caption,
            reply_to=reply_to,
        )
        if result.success or not self._looks_like_url(image_url):
            return result

        logger.warning("[%s] Falling back to text send for image URL %s: %s", self.name, image_url, result.error)
        fallback_text = f"{caption}\n{image_url}" if caption else image_url
        return await self.send(chat_id=chat_id, content=fallback_text, reply_to=reply_to)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        del kwargs
        return await self._send_media_source(
            chat_id=chat_id,
            media_source=image_path,
            caption=caption,
            reply_to=reply_to,
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        del kwargs
        return await self._send_media_source(
            chat_id=chat_id,
            media_source=file_path,
            caption=caption,
            file_name=file_name,
            reply_to=reply_to,
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        del kwargs
        return await self._send_media_source(
            chat_id=chat_id,
            media_source=audio_path,
            caption=caption,
            reply_to=reply_to,
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        del kwargs
        return await self._send_media_source(
            chat_id=chat_id,
            media_source=video_path,
            caption=caption,
            reply_to=reply_to,
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Open an empty native stream to trigger WeCom's thinking bubble.

        WeCom has no generic typing indicator API, but its AI Bot protocol
        shows a thinking bubble when a ``msgtype:"stream"`` response is
        opened. We send an empty first chunk here so the user sees the
        bubble immediately; the actual response content continues the same
        stream via :meth:`send` / :meth:`edit_message`.
        """
        reply_req_id = self._last_chat_req_ids.get(chat_id)
        if not reply_req_id:
            return  # No req_id available, can't open a stream

        if chat_id in self._active_streams:
            return  # Stream already open for this chat

        if chat_id in self._finalized_streams:
            # Stream was finalized for this chat; don't reopen it until
            # a new inbound message clears the flag via _remember_chat_req_id.
            # Progress queue keeps calling us ~every 0.3s — without this guard
            # we'd open a never-finalized ghost bubble on every tick.
            return

        stream_id = self._new_req_id("stream")
        try:
            await self._send_reply_stream(
                reply_req_id,
                "",
                stream_id=stream_id,
                finish=False,
            )
            self._active_streams[chat_id] = (reply_req_id, stream_id)
            logger.info(
                "[%s] send_typing: opened stream reply_req_id=%s stream_id=%s",
                self.name, reply_req_id, stream_id,
            )
        except Exception as exc:
            logger.debug("[%s] send_typing: failed to open stream: %s", self.name, exc)
            # Non-fatal — content will send normally when AI responds

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return minimal chat info."""
        return {
            "name": chat_id,
            "type": "group" if chat_id and chat_id.lower().startswith("group") else "dm",
        }


# ------------------------------------------------------------------
# QR code scan flow for obtaining bot credentials
# ------------------------------------------------------------------

_QR_GENERATE_URL = "https://work.weixin.qq.com/ai/qc/generate"
_QR_QUERY_URL = "https://work.weixin.qq.com/ai/qc/query_result"
_QR_CODE_PAGE = "https://work.weixin.qq.com/ai/qc/gen?source=hermes&scode="
_QR_POLL_INTERVAL = 3  # seconds
_QR_POLL_TIMEOUT = 300  # 5 minutes


def qr_scan_for_bot_info(
    *,
    timeout_seconds: int = _QR_POLL_TIMEOUT,
) -> Optional[Dict[str, str]]:
    """Run the WeCom QR scan flow to obtain bot_id and secret.

    Fetches a QR code from WeCom, renders it in the terminal, and polls
    until the user scans it or the timeout expires.

    Returns ``{"bot_id": ..., "secret": ...}`` on success, ``None`` on
    failure or timeout.

    Note: the ``work.weixin.qq.com/ai/qc/{generate,query_result}`` endpoints
    used here are not part of WeCom's public developer API — they back the
    admin-console web UI's bot-creation flow and may change without notice.
    The same pattern is used by the feishu/dingtalk QR setup wizards.
    """
    try:
        import urllib.request
        import urllib.parse
    except ImportError:  # pragma: no cover
        logger.error("urllib is required for WeCom QR scan")
        return None

    generate_url = f"{_QR_GENERATE_URL}?source=hermes"

    # ── Step 1: Fetch QR code ──
    print("  Connecting to WeCom...", end="", flush=True)
    try:
        req = urllib.request.Request(generate_url, headers={"User-Agent": "HermesAgent/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.error("WeCom QR: failed to fetch QR code: %s", exc)
        print(f" failed: {exc}")
        return None

    data = raw.get("data") or {}
    scode = str(data.get("scode") or "").strip()
    auth_url = str(data.get("auth_url") or "").strip()

    if not scode or not auth_url:
        logger.error("WeCom QR: unexpected response format: %s", raw)
        print(" failed: unexpected response format")
        return None

    print(" done.")

    # ── Step 2: Render QR code in terminal ──
    print()
    qr_rendered = False
    try:
        import qrcode as _qrcode
        qr = _qrcode.QRCode()
        qr.add_data(auth_url)
        qr.make(fit=True)
        qr.print_ascii(invert=True)
        qr_rendered = True
    except ImportError:
        pass
    except Exception:
        pass

    page_url = f"{_QR_CODE_PAGE}{urllib.parse.quote(scode)}"
    if qr_rendered:
        print(f"\n  Scan the QR code above, or open this URL directly:\n  {page_url}")
    else:
        print(f"  Open this URL in WeCom on your phone:\n\n  {page_url}\n")
        print("  Tip: pip install qrcode  to display a scannable QR code here next time")
    print()
    print("  Fetching configuration results...", end="", flush=True)

    # ── Step 3: Poll for result ──
    import time
    deadline = time.time() + timeout_seconds
    query_url = f"{_QR_QUERY_URL}?scode={urllib.parse.quote(scode)}"
    poll_count = 0

    while time.time() < deadline:
        try:
            req = urllib.request.Request(query_url, headers={"User-Agent": "HermesAgent/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logger.debug("WeCom QR poll error: %s", exc)
            time.sleep(_QR_POLL_INTERVAL)
            continue

        poll_count += 1
        # Print a dot on every poll so progress is visible within 3s.
        print(".", end="", flush=True)

        result_data = result.get("data") or {}
        status = str(result_data.get("status") or "").lower()

        if status == "success":
            print()  # newline after "Fetching configuration results..." dots
            bot_info = result_data.get("bot_info") or {}
            bot_id = str(bot_info.get("botid") or bot_info.get("bot_id") or "").strip()
            secret = str(bot_info.get("secret") or "").strip()
            if bot_id and secret:
                return {"bot_id": bot_id, "secret": secret}
            logger.warning(
                "WeCom QR: scan reported success but bot_info missing or incomplete: %s",
                result_data,
            )
            print(
                "  QR scan reported success but no bot credentials were returned.\n"
                "  This usually means the bot was not actually created on the WeCom side.\n"
                "  Falling back to manual credential entry."
            )
            return None

        time.sleep(_QR_POLL_INTERVAL)

    print()  # newline after dots
    print(f"  QR scan timed out ({timeout_seconds // 60} minutes). Please try again.")
    return None
