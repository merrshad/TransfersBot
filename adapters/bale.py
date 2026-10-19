"""Bale adapter: the Telegram-compatible Bale Bot API over raw ``httpx``.

Endpoint assumptions, taken from the official documentation at
``https://docs.bale.ai`` (see ``README.md`` for the full evidence list):

* ``{base_url}/bot{TOKEN}/{method}`` — the token sits in the path, like Telegram.
* ``getUpdates`` accepts ``offset``, ``limit`` (1–100) and ``timeout`` seconds.
  ``allowed_updates`` is **not** documented, so it is never sent.
* ``sendMessage`` accepts ``chat_id``, ``text`` (1–4096 chars) and
  ``reply_to_message_id``. ``parse_mode`` is **not** documented and Bale already
  formats outgoing text as Markdown, so it is never sent either.
* ``getMe`` returns the bot user; used as a startup sanity check.
* Responses use the Telegram envelope ``{"ok": true, "result": ...}`` /
  ``{"ok": false, "error_code": ..., "description": ...}``. Because the docs do
  not state whether failures also use HTTP 4xx/5xx, the JSON body is always
  inspected first, and a nested envelope (some compatible servers double-wrap
  the result) is unwrapped defensively.
* Bale does not document the error text for a blocked bot or a chat that never
  started the bot, so unreachable recipients are detected from the HTTP status,
  ``error_code`` and description keywords (``core.errors.looks_unreachable``).

The token is never interpolated into a log record, and transport failures are
re-raised without chaining the original httpx error, whose message contains the
token-bearing URL.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

import httpx
from pydantic import SecretStr

from adapters.base import (
    BaseAdapter,
    PollResult,
    clamp_message_text,
    download_url_to_file,
    guess_suffix,
    safe_file_name,
)
from adapters.telegram import token_secret
from core.errors import (
    ApiError,
    MediaTooLargeError,
    MediaUnsupportedError,
    TransportError,
    classify_messaging_error,
)
from core.media import (
    MEGABYTE,
    describe_contact,
    describe_location,
)
from core.models import (
    InlineButton,
    MediaItem,
    MediaKind,
    MessageRef,
    OutboundMedia,
    Platform,
    SentMessage,
    UnifiedMessage,
)

DEFAULT_BALE_API_BASE_URL = "https://tapi.bale.ai"
POLL_LIMIT = 100
#: Documented ``getFile`` download ceiling, used when settings say nothing.
DEFAULT_MAX_DOWNLOAD_BYTES = 20 * MEGABYTE
#: Upload timeout; a 50 MB multipart body needs more than a default read timeout.
UPLOAD_TIMEOUT_SECONDS = 300.0
#: Bale caps captions at 1024 characters, like Telegram.
CAPTION_MAX_LENGTH = 1024
#: Multipart field name per send method (from the official documentation).
_UPLOAD_FIELDS: Mapping[MediaKind, tuple[str, str]] = {
    MediaKind.PHOTO: ("sendPhoto", "photo"),
    MediaKind.VIDEO: ("sendVideo", "video"),
    MediaKind.ANIMATION: ("sendAnimation", "animation"),
    MediaKind.DOCUMENT: ("sendDocument", "document"),
    MediaKind.AUDIO: ("sendAudio", "audio"),
    MediaKind.VOICE: ("sendVoice", "voice"),
}
#: Album item type per kind (Bale documents photo/video/document/audio).
_ALBUM_ITEM_TYPES: Mapping[MediaKind, str] = {
    MediaKind.PHOTO: "photo",
    MediaKind.VIDEO: "video",
    MediaKind.ANIMATION: "video",
    MediaKind.DOCUMENT: "document",
    MediaKind.AUDIO: "audio",
}
#: Extra seconds added to the httpx read timeout so the platform can answer a
#: long poll just before its own timeout expires.
POLL_READ_GRACE_SECONDS = 15.0
CONNECT_TIMEOUT_SECONDS = 15.0
MAX_UNWRAP_DEPTH = 3


def parse_api_response(status_code: int, body: object) -> Any:
    """Return the ``result`` of a Bale API response, or raise a typed error.

    The JSON envelope is authoritative: ``ok: true`` wins, ``ok: false`` becomes
    a :class:`~core.errors.MessagingError`. HTTP status codes are only used for
    bodies that carry no envelope.

    Raises:
        core.errors.MessagingError: ``RateLimitError``, ``TransportError``,
            ``DeliveryError`` or ``ApiError``.
    """
    payload: object = body
    for _ in range(MAX_UNWRAP_DEPTH):
        if not isinstance(payload, Mapping) or "ok" not in payload:
            break
        if payload.get("ok") is not True:
            break
        result = payload.get("result")
        if isinstance(result, Mapping) and "ok" in result:
            payload = result  # double-wrapped envelope: inspect the inner one
            continue
        return result

    if isinstance(payload, Mapping) and "ok" not in payload and status_code < 400:
        return payload  # bare result without an envelope; tolerate it

    if isinstance(payload, Mapping):
        raw_description = payload.get("description")
        description = (
            raw_description
            if isinstance(raw_description, str) and raw_description.strip()
            else "Bale API error"
        )
        raw_code = payload.get("error_code")
        error_code = int(raw_code) if isinstance(raw_code, (int, float)) else None
        parameters = payload.get("parameters")
        retry_after: float | None = None
        if isinstance(parameters, Mapping):
            raw_retry = parameters.get("retry_after")
            if isinstance(raw_retry, (int, float)):
                retry_after = float(raw_retry)
        raise classify_messaging_error(
            description=description,
            status_code=status_code if status_code >= 400 else None,
            error_code=error_code,
            retry_after=retry_after,
            platform=Platform.BALE.value,
        )

    raise classify_messaging_error(
        description=f"unexpected Bale API payload (HTTP {status_code})",
        status_code=status_code if status_code >= 400 else None,
        platform=Platform.BALE.value,
    )


def _inline_keyboard(
    buttons: Sequence[Sequence[InlineButton]],
) -> dict[str, Any] | None:
    """Translate the domain button grid into Bale's inline markup."""
    rows = [
        [
            {"text": button.text, "callback_data": button.callback_data}
            for button in row
        ]
        for row in buttons
        if row
    ]
    return {"inline_keyboard": rows} if rows else None


def _message_timestamp(raw_date: object) -> datetime:
    """Convert a Bale Unix timestamp into an aware UTC datetime."""
    if isinstance(raw_date, (int, float)) and not isinstance(raw_date, bool):
        return datetime.fromtimestamp(float(raw_date), tz=UTC)
    return datetime.now(UTC)


def _as_int(value: object) -> int | None:
    """``value`` when it is an int (bools excluded), else ``None``."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _as_float(value: object) -> float | None:
    """``value`` as a float when it is numeric, else ``None``."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def _as_str(value: object) -> str | None:
    """``value`` when it is a non-blank string, else ``None``."""
    if isinstance(value, str) and value.strip():
        return value
    return None


def _mapping(value: object) -> Mapping[str, Any] | None:
    """``value`` when it is a mapping, else ``None``."""
    return value if isinstance(value, Mapping) else None


def _media_from_object(
    raw: Mapping[str, Any], kind: MediaKind, *, mime_default: str | None = None
) -> MediaItem | None:
    """Build a :class:`MediaItem` from a Bale ``Video``/``Document``/… object."""
    file_id = _as_str(raw.get("file_id"))
    if file_id is None:
        return None
    return MediaItem(
        kind=kind,
        file_id=file_id,
        file_name=_as_str(raw.get("file_name")),
        mime_type=_as_str(raw.get("mime_type")) or mime_default,
        file_size=_as_int(raw.get("file_size")),
        duration=_as_float(raw.get("duration")),
        width=_as_int(raw.get("width")),
        height=_as_int(raw.get("height")),
        emoji=_as_str(raw.get("emoji")),
    )


def _extract_media(message: Mapping[str, Any]) -> tuple[MediaItem, ...]:
    """The attachment of a Bale message, if any.

    Bale documents ``photo`` (array of sizes), ``video``, ``animation``,
    ``document``, ``audio``, ``voice`` and ``sticker``. ``video_note`` is not
    documented; if it ever appears it is still relayed as a video-file, and
    anything unrecognised falls back to a text placeholder upstream.
    """
    photos = message.get("photo")
    if isinstance(photos, list):
        sizes = [item for item in photos if isinstance(item, Mapping)]
        if sizes:
            largest = max(
                sizes,
                key=lambda size: (
                    _as_int(size.get("file_size")) or 0,
                    (_as_int(size.get("width")) or 0) * (_as_int(size.get("height")) or 0),
                ),
            )
            item = _media_from_object(largest, MediaKind.PHOTO, mime_default="image/jpeg")
            if item is not None:
                return (item,)

    for key, kind, mime_default in (
        ("animation", MediaKind.ANIMATION, "video/mp4"),
        ("video", MediaKind.VIDEO, "video/mp4"),
        ("video_note", MediaKind.VIDEO_NOTE, "video/mp4"),
        ("voice", MediaKind.VOICE, "audio/ogg"),
        ("audio", MediaKind.AUDIO, "audio/mpeg"),
        ("sticker", MediaKind.STICKER, None),
        ("document", MediaKind.DOCUMENT, None),
    ):
        raw = _mapping(message.get(key))
        if raw is None:
            continue
        item = _media_from_object(raw, kind, mime_default=mime_default)
        if item is not None:
            return (item,)
    return ()


def _textual_fallback(message: Mapping[str, Any]) -> str | None:
    """Text rendering of a location or contact (Bale documents both)."""
    location = _mapping(message.get("location"))
    if location is not None:
        latitude = _as_float(location.get("latitude"))
        longitude = _as_float(location.get("longitude"))
        if latitude is not None and longitude is not None:
            return describe_location(latitude=latitude, longitude=longitude)
    contact = _mapping(message.get("contact"))
    if contact is not None:
        phone = _as_str(contact.get("phone_number"))
        if phone is not None:
            name = " ".join(
                part
                for part in (
                    _as_str(contact.get("first_name")),
                    _as_str(contact.get("last_name")),
                )
                if part
            )
            return describe_contact(
                name=name, phone_number=phone, username=_as_str(contact.get("username"))
            )
    return None


def _unsupported_kind(message: Mapping[str, Any]) -> str | None:
    """Name of a message type this bridge cannot represent, if any."""
    for key in ("venue", "dice", "game", "invoice", "successful_payment", "story"):
        value = message.get(key)
        if isinstance(value, Mapping) or value:
            return key
    return None


def parse_update(raw: Mapping[str, Any]) -> UnifiedMessage | None:
    """Convert one raw Bale update into a :class:`UnifiedMessage`.

    Returns:
        ``None`` for update kinds this phase ignores: non-message updates,
        non-private chats, messages without a sender (Bale documents ``from`` as
        optional) and messages with nothing at all to relay.
    """
    callback = _mapping(raw.get("callback_query"))
    if callback is not None:
        callback_message = _mapping(callback.get("message"))
        sender = _mapping(callback.get("from"))
        data = _as_str(callback.get("data"))
        callback_id = _as_str(callback.get("id"))
        sender_id = _as_int(sender.get("id")) if sender is not None else None
        if sender is None or data is None or callback_id is None or sender_id is None:
            return None
        chat = _mapping(callback_message.get("chat")) if callback_message is not None else None
        callback_chat_id = (
            _as_int(chat.get("id")) if chat is not None else sender_id
        ) or sender_id
        callback_message_id = (
            _as_int(callback_message.get("message_id"))
            if callback_message is not None
            else 0
        ) or 0
        return UnifiedMessage(
            source=Platform.BALE,
            sender_id=sender_id,
            chat_id=callback_chat_id,
            message_id=callback_message_id,
            timestamp=(
                _message_timestamp(callback_message.get("date"))
                if callback_message is not None
                else datetime.now(UTC)
            ),
            callback_data=data,
            callback_id=callback_id,
        )

    message = raw.get("message")
    if not isinstance(message, Mapping):
        return None
    chat = message.get("chat")
    if not isinstance(chat, Mapping) or chat.get("type") != "private":
        return None
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    if not isinstance(chat_id, int) or not isinstance(message_id, int):
        return None
    sender = message.get("from")
    if not isinstance(sender, Mapping):
        return None
    sender_id = sender.get("id")
    if not isinstance(sender_id, int):
        return None

    media = _extract_media(message)
    text = _as_str(message.get("text")) or _as_str(message.get("caption"))
    unsupported: str | None = None
    if not media and not text:
        text = _textual_fallback(message)
        if text is None:
            unsupported = _unsupported_kind(message)
            if unsupported is None:
                return None

    reply = message.get("reply_to_message")
    reply_to: MessageRef | None = None
    if isinstance(reply, Mapping):
        reply_message_id = reply.get("message_id")
        reply_chat = reply.get("chat")
        reply_chat_id = (
            reply_chat.get("id") if isinstance(reply_chat, Mapping) else chat_id
        )
        if isinstance(reply_message_id, int) and isinstance(reply_chat_id, int):
            reply_to = MessageRef(
                platform=Platform.BALE,
                chat_id=reply_chat_id,
                message_id=reply_message_id,
            )

    raw_entities = message.get("entities") or message.get("caption_entities")
    entities: tuple[Mapping[str, Any], ...] = (
        tuple(item for item in raw_entities if isinstance(item, Mapping))
        if isinstance(raw_entities, list)
        else ()
    )
    media_group_id = _as_str(message.get("media_group_id"))

    return UnifiedMessage(
        source=Platform.BALE,
        sender_id=sender_id,
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        entities=entities,
        media=media,
        reply_to=reply_to,
        timestamp=_message_timestamp(message.get("date")),
        media_group_id=media_group_id,
        unsupported=unsupported,
    )


class BaleAdapter(BaseAdapter):
    """Long-polling Bale adapter over a shared ``httpx.AsyncClient``.

    Args:
        token: Bot token (``SecretStr`` from settings, or a plain string).
        base_url: Bale API base, normally ``settings.bale_api_base_url``.
        client: Optional pre-built ``httpx.AsyncClient`` (tests inject one with
            a ``MockTransport``); when omitted the adapter creates and owns one.
        logger: Logger override.
    """

    def __init__(
        self,
        token: SecretStr | str,
        *,
        base_url: str = DEFAULT_BALE_API_BASE_URL,
        client: httpx.AsyncClient | None = None,
        max_download_bytes: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("bridge.adapters.bale")
        self._base_url = base_url.rstrip("/")
        self._token = token_secret(token)
        self._api_root = f"{self._base_url}/bot{self._token}"
        self._owns_client = client is None
        self._client = client if client is not None else httpx.AsyncClient()
        self._max_download_bytes = int(
            max_download_bytes
            if max_download_bytes is not None
            else DEFAULT_MAX_DOWNLOAD_BYTES
        )

    @property
    def platform(self) -> Platform:
        """Always :attr:`Platform.BALE`."""
        return Platform.BALE

    @property
    def base_url(self) -> str:
        """Configured Bale API base (without token)."""
        return self._base_url

    def _url(self, method: str) -> str:
        """Build the token-bearing method URL (never log the result)."""
        return f"{self._api_root}/{method}"

    async def _call(
        self,
        method: str,
        payload: Mapping[str, Any],
        *,
        read_timeout: float | None = None,
    ) -> Any:
        """POST ``payload`` to ``method`` and return the parsed ``result``."""
        request_timeout: float | httpx.Timeout | None = None
        if read_timeout is not None:
            request_timeout = httpx.Timeout(
                CONNECT_TIMEOUT_SECONDS, read=read_timeout, write=CONNECT_TIMEOUT_SECONDS
            )
        try:
            response = await self._client.post(
                self._url(method), json=dict(payload), timeout=request_timeout
            )
        except httpx.TimeoutException:
            # Deliberately not chained: the httpx message embeds the token in the URL.
            raise TransportError(
                f"Bale {method} timed out", platform=Platform.BALE.value
            ) from None
        except httpx.HTTPError as exc:
            raise TransportError(
                f"Bale {method} transport failure ({type(exc).__name__})",
                platform=Platform.BALE.value,
            ) from None

        try:
            body: object = response.json()
        except ValueError:
            body = None
        return parse_api_response(response.status_code, body)

    async def get_me(self) -> Mapping[str, Any]:
        """Call ``getMe`` and return the bot user (startup sanity check)."""
        result = await self._call("getMe", {})
        if not isinstance(result, Mapping):
            raise ApiError("Bale getMe returned an unexpected payload", platform=Platform.BALE.value)
        return result

    async def poll_once(self, offset: int | None, *, timeout: int) -> PollResult:
        """Fetch pending Bale updates once using long polling."""
        payload: dict[str, Any] = {"limit": POLL_LIMIT, "timeout": timeout}
        if offset is not None:
            payload["offset"] = offset

        result = await self._call(
            "getUpdates", payload, read_timeout=float(timeout) + POLL_READ_GRACE_SECONDS
        )
        if not isinstance(result, Sequence) or isinstance(result, (str, bytes)):
            raise ApiError(
                "Bale getUpdates returned an unexpected payload", platform=Platform.BALE.value
            )

        raw_updates = [item for item in result if isinstance(item, Mapping)]
        messages = tuple(
            msg for msg in (parse_update(item) for item in raw_updates) if msg is not None
        )
        update_ids = [
            item["update_id"]
            for item in raw_updates
            if isinstance(item.get("update_id"), int)
        ]
        next_offset = max(update_ids) + 1 if update_ids else None
        if next_offset is not None and len(messages) != len(raw_updates):
            self._log.debug(
                "skipped %d unsupported Bale updates", len(raw_updates) - len(messages)
            )
        return PollResult(
            messages=messages, next_offset=next_offset, received=len(raw_updates)
        )

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage:
        """Send a text message, optionally as a reply and with inline buttons.

        ``parse_mode`` is intentionally omitted: Bale's ``sendMessage`` does not
        document it and Bale formats outgoing text as Markdown itself.
        """
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": clamp_message_text(text, logger=self._log),
        }
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
        markup = _inline_keyboard(buttons)
        if markup is not None:
            payload["reply_markup"] = markup

        result = await self._call("sendMessage", payload)
        message_id = result.get("message_id") if isinstance(result, Mapping) else None
        if not isinstance(message_id, int):
            raise ApiError(
                "Bale sendMessage response has no message_id", platform=Platform.BALE.value
            )
        return SentMessage(platform=Platform.BALE, chat_id=chat_id, message_id=message_id)

    async def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage | None:
        """Edit an existing text message (used by the inline menu)."""
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": clamp_message_text(text, logger=self._log),
        }
        markup = _inline_keyboard(buttons)
        if markup is not None:
            payload["reply_markup"] = markup
        result = await self._call("editMessageText", payload)
        result_id = result.get("message_id") if isinstance(result, Mapping) else None
        if isinstance(result_id, int):
            return SentMessage(
                platform=Platform.BALE, chat_id=chat_id, message_id=result_id
            )
        return None

    async def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> None:
        """Stop the button spinner, optionally showing a short toast."""
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        await self._call("answerCallbackQuery", payload)

    # ------------------------------------------------------------------ media

    async def download_media(self, item: MediaItem, destination: Path) -> int:
        """Download one Bale attachment into ``destination``.

        ``file_id`` handles are resolved with ``getFile`` and then fetched from
        ``/file/bot<TOKEN>/<file_path>`` (Bale documents one-hour link validity
        but a fresh ``getFile`` per download sidesteps that entirely).
        """
        if item.data is not None:
            destination.write_bytes(item.data)
            return len(item.data)
        if item.url is not None:
            return await download_url_to_file(
                item.url,
                destination,
                max_bytes=self._max_download_bytes,
                client=self._client,
            )
        if not item.file_id:  # pragma: no cover - MediaItem guarantees a source
            raise ApiError("media item has no Bale source")

        result = await self._call("getFile", {"file_id": item.file_id})
        if not isinstance(result, Mapping):
            raise ApiError(
                "Bale getFile returned an unexpected payload", platform=Platform.BALE.value
            )
        declared = _as_int(result.get("file_size"))
        if declared is not None and declared > self._max_download_bytes:
            raise MediaTooLargeError(
                size_bytes=declared, limit_bytes=self._max_download_bytes, kind="فایل"
            )
        file_path = _as_str(result.get("file_path"))
        if file_path is None:
            raise ApiError(
                "Bale getFile returned no file_path", platform=Platform.BALE.value
            )
        url = f"{self._base_url}/file/bot{self._token}/{file_path.lstrip('/')}"
        written = await download_url_to_file(
            url,
            destination,
            max_bytes=self._max_download_bytes,
            client=self._client,
        )
        return written

    async def send_media(
        self,
        chat_id: int,
        item: OutboundMedia,
        *,
        reply_to_message_id: int | None = None,
    ) -> SentMessage:
        """Upload one attachment with the documented multipart field.

        Raises:
            core.errors.MediaUnsupportedError: Bale has no such media method
                (stickers and video notes are not documented).
        """
        field = _UPLOAD_FIELDS.get(item.kind)
        if field is None:
            raise MediaUnsupportedError(kind=item.kind.value)
        method, field_name = field
        data: dict[str, Any] = {"chat_id": chat_id}
        caption = (
            clamp_message_text(item.caption, limit=CAPTION_MAX_LENGTH, logger=self._log)
            if item.caption
            else None
        )
        if caption:
            data["caption"] = caption
        if item.duration is not None:
            data["duration"] = int(item.duration)
        if reply_to_message_id is not None:
            data["reply_to_message_id"] = reply_to_message_id

        with item.path.open("rb") as handle:
            files = {field_name: self._upload_tuple(item, handle)}
            result = await self._call_multipart(method, data, files)

        message_id = result.get("message_id") if isinstance(result, Mapping) else None
        if not isinstance(message_id, int):
            raise ApiError(
                f"Bale {method} response has no message_id", platform=Platform.BALE.value
            )
        self._log.info(
            "bale media sent kind=%s bytes=%s", item.kind.value, item.file_size
        )
        return SentMessage(platform=Platform.BALE, chat_id=chat_id, message_id=message_id)

    async def send_media_group(
        self,
        chat_id: int,
        items: Sequence[OutboundMedia],
        *,
        reply_to_message_id: int | None = None,
    ) -> tuple[SentMessage, ...]:
        """Upload 2–10 attachments as one album using ``attach://`` references."""
        if len(items) < 2:
            raise ApiError("a Bale album needs at least 2 items")
        media: list[dict[str, Any]] = []
        opened: list[BinaryIO] = []
        try:
            data: dict[str, Any] = {"chat_id": chat_id}
            if reply_to_message_id is not None:
                data["reply_to_message_id"] = reply_to_message_id
            files: dict[str, Any] = {}
            for index, item in enumerate(items):
                item_type = _ALBUM_ITEM_TYPES.get(item.kind)
                if item_type is None:
                    raise MediaUnsupportedError(kind=item.kind.value)
                attach_name = f"item{index}"
                entry: dict[str, Any] = {
                    "type": item_type,
                    "media": f"attach://{attach_name}",
                }
                caption = (
                    clamp_message_text(
                        item.caption, limit=CAPTION_MAX_LENGTH, logger=self._log
                    )
                    if item.caption
                    else None
                )
                if caption:
                    entry["caption"] = caption
                if item.duration is not None:
                    entry["duration"] = int(item.duration)
                media.append(entry)
                handle = item.path.open("rb")
                opened.append(handle)
                files[attach_name] = self._upload_tuple(item, handle)
            data["media"] = json.dumps(media, ensure_ascii=False)
            result = await self._call_multipart("sendMediaGroup", data, files)
        finally:
            for stream in opened:
                stream.close()

        if not isinstance(result, Sequence) or isinstance(result, (str, bytes)):
            raise ApiError(
                "Bale sendMediaGroup returned an unexpected payload",
                platform=Platform.BALE.value,
            )
        sent: list[SentMessage] = []
        for entry in result:
            message_id = (
                _as_int(entry.get("message_id")) if isinstance(entry, Mapping) else None
            )
            if message_id is None:
                raise ApiError(
                    "Bale sendMediaGroup response is missing a message_id",
                    platform=Platform.BALE.value,
                )
            sent.append(
                SentMessage(platform=Platform.BALE, chat_id=chat_id, message_id=message_id)
            )
        self._log.info(
            "bale album sent items=%s bytes=%s",
            len(items),
            sum(item.file_size or 0 for item in items),
        )
        return tuple(sent)

    async def _call_multipart(
        self,
        method: str,
        data: Mapping[str, Any],
        files: Mapping[str, Any],
    ) -> Any:
        """POST a multipart body to ``method`` and return the parsed ``result``."""
        try:
            response = await self._client.post(
                self._url(method),
                data=dict(data),
                files=dict(files),
                timeout=httpx.Timeout(
                    CONNECT_TIMEOUT_SECONDS,
                    read=UPLOAD_TIMEOUT_SECONDS,
                    write=UPLOAD_TIMEOUT_SECONDS,
                ),
            )
        except httpx.TimeoutException:
            # Deliberately not chained: the httpx message embeds the token in the URL.
            raise TransportError(
                f"Bale {method} timed out", platform=Platform.BALE.value
            ) from None
        except httpx.HTTPError as exc:
            raise TransportError(
                f"Bale {method} transport failure ({type(exc).__name__})",
                platform=Platform.BALE.value,
            ) from None

        try:
            body: object = response.json()
        except ValueError:
            body = None
        return parse_api_response(response.status_code, body)

    def _upload_tuple(self, item: OutboundMedia, handle: BinaryIO) -> tuple[str, Any, str]:
        """Multipart ``(filename, fileobj, content_type)`` for one attachment."""
        name = safe_file_name(item, fallback=f"file{guess_suffix(item)}")
        return (name, handle, item.mime_type or "application/octet-stream")

    async def close(self) -> None:
        """Close the HTTP client when this adapter created it."""
        if self._owns_client:
            await self._client.aclose()


__all__ = [
    "CONNECT_TIMEOUT_SECONDS",
    "DEFAULT_BALE_API_BASE_URL",
    "MAX_UNWRAP_DEPTH",
    "POLL_LIMIT",
    "POLL_READ_GRACE_SECONDS",
    "BaleAdapter",
    "parse_api_response",
    "parse_update",
]
