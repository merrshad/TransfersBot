"""Telegram adapter.

**Library choice:** `python-telegram-bot` v21+ is used because
``Bot.get_updates(offset=..., timeout=...)`` is a first-class async API that
returns typed ``Update`` objects and raises typed errors (``Forbidden``,
``BadRequest``, ``RetryAfter``), which is exactly what an offset-persisting long
-poll needs; it also shares the httpx stack with the Bale adapter.

Scope: **text, commands and media**. Every attachment kind Telegram documents is
parsed into a :class:`~core.models.MediaItem`, downloaded with ``getFile`` when
the bot has to re-upload it elsewhere, and sent back with the matching
``send*`` method. Location, contact and poll messages become text renderings;
message types the bridge cannot represent become a text placeholder and are
logged by name (never by content).

Documented limits baked into the defaults: ``getFile`` downloads at most 20 MB,
``sendPhoto`` uploads at most 10 MB, every other media method 50 MB, stickers
must fit their format's cap (512 KB static ``.WEBP``), and albums hold 2–10
items.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO, Protocol, runtime_checkable

from pydantic import SecretStr
from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
    ReplyParameters,
    Update,
)
from telegram.error import (
    BadRequest,
    Forbidden,
    NetworkError,
    RetryAfter,
    TelegramError,
    TimedOut,
)
from telegram.request import HTTPXRequest

from adapters.base import (
    BaseAdapter,
    PollResult,
    clamp_message_text,
    download_url_to_file,
)
from core.errors import (
    ApiError,
    DeliveryError,
    MediaTooLargeError,
    MessagingError,
    RateLimitError,
    TransportError,
    classify_messaging_error,
)
from core.media import (
    MEGABYTE,
    describe_contact,
    describe_location,
    describe_poll,
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

#: New messages plus inline-button presses.
DEFAULT_ALLOWED_UPDATES: tuple[str, ...] = ("message", "callback_query")
POLL_LIMIT = 100
#: Telegram caps captions at 1024 characters (after entity parsing).
CAPTION_MAX_LENGTH = 1024
#: Documented ``getFile`` download ceiling, used when settings say nothing.
DEFAULT_MAX_DOWNLOAD_BYTES = 20 * MEGABYTE

#: PTB defaults are 5s connect/read/write and 20s for media writes. A 20–50 MB
#: upload can easily exceed 20s, so the client timed out *after* Telegram had
#: accepted the file; the outbox then retried and the recipient saw duplicates.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 30.0
DEFAULT_READ_TIMEOUT_SECONDS = 300.0
DEFAULT_WRITE_TIMEOUT_SECONDS = 600.0
DEFAULT_POOL_TIMEOUT_SECONDS = 30.0

#: Mime type assumed for each sticker flavour Telegram documents.
_STICKER_MIME_TYPES = {
    (False, False): "image/webp",  # static
    (True, False): "application/x-tgsticker",  # animated .TGS
    (False, True): "video/webm",  # video .WEBM
}

#: Message attributes that name a type this bridge cannot represent, in the
#: order they are reported when several are present.
_UNSUPPORTED_ATTRIBUTES: tuple[str, ...] = (
    "venue",
    "dice",
    "game",
    "invoice",
    "successful_payment",
    "refunded_payment",
    "paid_media",
    "story",
    "giveaway",
    "giveaway_winners",
    "video_chat_started",
    "video_chat_ended",
    "video_chat_scheduled",
    "video_chat_participants_invited",
    "proximity_alert_triggered",
    "new_chat_members",
    "left_chat_member",
    "pinned_message",
    "web_app_data",
)


def token_secret(token: SecretStr | str) -> str:
    """Return the raw token string from a :class:`SecretStr` or a plain string."""
    return token.get_secret_value() if isinstance(token, SecretStr) else str(token)


def duration_seconds(value: int | timedelta | None) -> float | None:
    """Normalise PTB's ``duration`` attributes to seconds.

    Like ``RetryAfter.retry_after``, durations are typed ``int | timedelta`` and
    flip to ``timedelta`` in a future major version, so both shapes are accepted.
    """
    if value is None:
        return None
    return value.total_seconds() if isinstance(value, timedelta) else float(value)


def retry_after_seconds(value: int | timedelta) -> float:
    """Normalise PTB's ``RetryAfter.retry_after`` to seconds.

    python-telegram-bot currently reports an ``int`` number of seconds but
    already types the attribute as ``int | timedelta`` (it flips to
    ``timedelta`` in a future major version), so both shapes are accepted.
    """
    return value.total_seconds() if isinstance(value, timedelta) else float(value)


def translate_telegram_error(error: TelegramError) -> MessagingError:
    """Map a python-telegram-bot exception onto a :class:`MessagingError`.

    ``Forbidden`` (403) is how Telegram reports a blocked bot, a deactivated
    user and missing rights, so it maps to a permanent delivery failure.
    """
    if isinstance(error, RetryAfter):
        return RateLimitError(
            str(error),
            retry_after=retry_after_seconds(error.retry_after),
            platform=Platform.TELEGRAM.value,
        )
    if isinstance(error, Forbidden):
        return DeliveryError(
            str(error), platform=Platform.TELEGRAM.value, status_code=403, error_code=403
        )
    if isinstance(error, BadRequest):
        return classify_messaging_error(
            description=str(error),
            status_code=400,
            error_code=400,
            platform=Platform.TELEGRAM.value,
        )
    if isinstance(error, (TimedOut, NetworkError)):
        return TransportError(str(error), platform=Platform.TELEGRAM.value)
    return classify_messaging_error(
        description=str(error), platform=Platform.TELEGRAM.value
    )


# ------------------------------------------------------------------- parsing


def _best_photo(message: Message) -> MediaItem | None:
    """Largest available size of a photo message (Telegram sends several)."""
    photos = message.photo or ()
    if not photos:
        return None
    largest = max(
        photos, key=lambda size: (size.file_size or 0, (size.width or 0) * (size.height or 0))
    )
    return MediaItem(
        kind=MediaKind.PHOTO,
        file_id=largest.file_id,
        file_size=largest.file_size,
        # Telegram re-encodes photos to JPEG; the size list carries no mime type.
        mime_type="image/jpeg",
        width=largest.width,
        height=largest.height,
    )


def _extract_media(message: Message) -> tuple[MediaItem, ...]:
    """The attachment of ``message``, if any (Telegram allows at most one)."""
    photo = _best_photo(message)
    if photo is not None:
        return (photo,)

    animation = message.animation
    if animation is not None:
        return (
            MediaItem(
                kind=MediaKind.ANIMATION,
                file_id=animation.file_id,
                file_name=animation.file_name,
                mime_type=animation.mime_type or "video/mp4",
                file_size=animation.file_size,
                duration=duration_seconds(animation.duration),
                width=animation.width,
                height=animation.height,
            ),
        )

    video = message.video
    if video is not None:
        return (
            MediaItem(
                kind=MediaKind.VIDEO,
                file_id=video.file_id,
                file_name=video.file_name,
                mime_type=video.mime_type or "video/mp4",
                file_size=video.file_size,
                duration=duration_seconds(video.duration),
                width=video.width,
                height=video.height,
            ),
        )

    video_note = message.video_note
    if video_note is not None:
        return (
            MediaItem(
                kind=MediaKind.VIDEO_NOTE,
                file_id=video_note.file_id,
                mime_type="video/mp4",
                file_size=video_note.file_size,
                duration=duration_seconds(video_note.duration),
                width=video_note.length,
                height=video_note.length,
            ),
        )

    voice = message.voice
    if voice is not None:
        return (
            MediaItem(
                kind=MediaKind.VOICE,
                file_id=voice.file_id,
                mime_type=voice.mime_type or "audio/ogg",
                file_size=voice.file_size,
                duration=duration_seconds(voice.duration),
            ),
        )

    audio = message.audio
    if audio is not None:
        return (
            MediaItem(
                kind=MediaKind.AUDIO,
                file_id=audio.file_id,
                file_name=audio.file_name,
                mime_type=audio.mime_type or "audio/mpeg",
                file_size=audio.file_size,
                duration=duration_seconds(audio.duration),
            ),
        )

    sticker = message.sticker
    if sticker is not None:
        return (
            MediaItem(
                kind=MediaKind.STICKER,
                file_id=sticker.file_id,
                mime_type=_STICKER_MIME_TYPES.get(
                    (bool(sticker.is_animated), bool(sticker.is_video)), "image/webp"
                ),
                file_size=sticker.file_size,
                width=sticker.width,
                height=sticker.height,
                emoji=sticker.emoji,
            ),
        )

    document = message.document
    if document is not None:
        return (
            MediaItem(
                kind=MediaKind.DOCUMENT,
                file_id=document.file_id,
                file_name=document.file_name,
                mime_type=document.mime_type,
                file_size=document.file_size,
            ),
        )
    return ()


def _unsupported_kind(message: Message) -> str | None:
    """Name of the message type this bridge cannot represent, if any."""
    for attribute in _UNSUPPORTED_ATTRIBUTES:
        value = getattr(message, attribute, None)
        # Truthiness, not ``is not None``: PTB defaults list-like service fields
        # (``new_chat_members``, ``giveaway_winners``) to empty tuples.
        if value:
            return attribute
    return None


def _textual_fallback(message: Message) -> str | None:
    """Text rendering for a message that carries no text and no attachment."""
    location = message.location
    if location is not None:
        return describe_location(
            latitude=location.latitude, longitude=location.longitude
        )
    contact = message.contact
    if contact is not None:
        full_name = " ".join(
            part for part in (contact.first_name, contact.last_name) if part
        )
        return describe_contact(name=full_name, phone_number=contact.phone_number)
    poll = message.poll
    if poll is not None:
        return describe_poll(
            question=poll.question,
            options=[option.text for option in poll.options],
            poll_type=str(getattr(poll.type, "value", poll.type)),
        )
    return None


def _sender_name(sender: object) -> str:
    """Human-readable sender label for group bridging (never a token/content)."""
    full_name = getattr(sender, "full_name", None)
    if isinstance(full_name, str) and full_name.strip():
        return full_name.strip()
    username = getattr(sender, "username", None)
    if isinstance(username, str) and username.strip():
        return username.strip()
    return "کاربر"


def to_unified_message(update: Update) -> UnifiedMessage | None:
    """Convert one Telegram update into a :class:`UnifiedMessage`.

    Returns:
        ``None`` for anything this phase does not relay: non-message updates,
        non-private chats, messages without a sender (channel posts) and
        messages that carry neither text, an attachment, a location/contact/poll
        rendering, nor an unsupported-type placeholder.
    """
    callback = update.callback_query
    if callback is not None:
        callback_message = callback.message
        if callback.from_user is None or callback.data is None:
            return None
        chat = callback_message.chat if callback_message is not None else None
        return UnifiedMessage(
            source=Platform.TELEGRAM,
            sender_id=callback.from_user.id,
            chat_id=chat.id if chat is not None else callback.from_user.id,
            message_id=(
                callback_message.message_id if callback_message is not None else 0
            ),
            timestamp=(
                getattr(callback_message, "date", None)
                if callback_message is not None
                else None
            )
            or datetime.now(UTC),
            callback_data=callback.data,
            callback_id=callback.id,
            chat_type=getattr(chat, "type", "private") if chat is not None else "private",
            chat_title=getattr(chat, "title", None) if chat is not None else None,
            sender_name=_sender_name(callback.from_user),
            is_bot_sender=bool(getattr(callback.from_user, "is_bot", False)),
        )

    message: Message | None = update.effective_message
    if message is None:
        return None
    chat = message.chat
    if chat is None or chat.type not in {"private", "group", "supergroup"}:
        return None
    sender = message.from_user
    if sender is None:
        return None

    media = _extract_media(message)
    text = message.text or message.caption
    unsupported: str | None = None
    if not media and not text:
        text = _textual_fallback(message)
        if text is None:
            unsupported = _unsupported_kind(message)
            if unsupported is None:
                return None

    reply = message.reply_to_message
    reply_to = (
        MessageRef(
            platform=Platform.TELEGRAM,
            chat_id=reply.chat.id,
            message_id=reply.message_id,
        )
        if reply is not None and reply.chat is not None
        else None
    )
    raw_entities = message.entities or message.caption_entities or ()
    entities: tuple[dict[str, Any], ...] = tuple(
        entity.to_dict() for entity in raw_entities
    )
    return UnifiedMessage(
        source=Platform.TELEGRAM,
        sender_id=sender.id,
        chat_id=chat.id,
        message_id=message.message_id,
        text=text,
        entities=entities,
        media=media,
        reply_to=reply_to,
        timestamp=message.date or datetime.now(UTC),
        media_group_id=str(message.media_group_id) if message.media_group_id else None,
        unsupported=unsupported,
        chat_type=chat.type,
        chat_title=chat.title if isinstance(chat.title, str) else None,
        sender_name=_sender_name(sender),
        is_bot_sender=bool(getattr(sender, "is_bot", False)),
    )


# -------------------------------------------------------------------- ports


@runtime_checkable
class TelegramFilePort(Protocol):
    """The slice of ``telegram.File`` the adapter downloads through."""

    file_size: int | None

    async def download_to_drive(self, custom_path: str | Path | None = None) -> Path:
        """Download the file to ``custom_path`` and return the written path."""
        ...


@runtime_checkable
class TelegramBotPort(Protocol):
    """The slice of ``telegram.Bot`` this adapter uses.

    Declaring it separately keeps the adapter testable offline and insulated
    from python-telegram-bot's version churn (v22 already moved ``session`` to
    ``_request``, for example).
    """

    async def get_updates(
        self,
        offset: int | None = None,
        limit: int | None = None,
        timeout: int | None = None,
        allowed_updates: Sequence[str] | None = None,
    ) -> tuple[Update, ...]:
        """``getUpdates`` (see the Telegram Bot API)."""
        ...

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_parameters: ReplyParameters | None = None,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> Message:
        """``sendMessage`` (see the Telegram Bot API)."""
        ...

    async def edit_message_text(
        self,
        text: str,
        chat_id: int,
        message_id: int,
        *,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> Message | bool:
        """``editMessageText`` (PTB takes the text first)."""
        ...

    async def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> bool:
        """``answerCallbackQuery``."""
        ...

    async def get_file(self, file_id: str) -> TelegramFilePort:
        """``getFile`` (see the Telegram Bot API)."""
        ...

    async def send_photo(
        self,
        chat_id: int,
        photo: BinaryIO | str | Path,
        *,
        caption: str | None = None,
        reply_parameters: ReplyParameters | None = None,
    ) -> Message:
        """``sendPhoto``."""
        ...

    async def send_video(
        self,
        chat_id: int,
        video: BinaryIO | str | Path,
        *,
        caption: str | None = None,
        duration: int | None = None,
        width: int | None = None,
        height: int | None = None,
        reply_parameters: ReplyParameters | None = None,
    ) -> Message:
        """``sendVideo``."""
        ...

    async def send_animation(
        self,
        chat_id: int,
        animation: BinaryIO | str | Path,
        *,
        caption: str | None = None,
        duration: int | None = None,
        width: int | None = None,
        height: int | None = None,
        reply_parameters: ReplyParameters | None = None,
    ) -> Message:
        """``sendAnimation``."""
        ...

    async def send_document(
        self,
        chat_id: int,
        document: BinaryIO | str | Path,
        *,
        caption: str | None = None,
        reply_parameters: ReplyParameters | None = None,
    ) -> Message:
        """``sendDocument``."""
        ...

    async def send_audio(
        self,
        chat_id: int,
        audio: BinaryIO | str | Path,
        *,
        caption: str | None = None,
        duration: int | None = None,
        reply_parameters: ReplyParameters | None = None,
    ) -> Message:
        """``sendAudio``."""
        ...

    async def send_voice(
        self,
        chat_id: int,
        voice: BinaryIO | str | Path,
        *,
        caption: str | None = None,
        duration: int | None = None,
        reply_parameters: ReplyParameters | None = None,
    ) -> Message:
        """``sendVoice``."""
        ...

    async def send_video_note(
        self,
        chat_id: int,
        video_note: BinaryIO | str | Path,
        *,
        duration: int | None = None,
        length: int | None = None,
        reply_parameters: ReplyParameters | None = None,
    ) -> Message:
        """``sendVideoNote`` (no caption, per the API)."""
        ...

    async def send_sticker(
        self,
        chat_id: int,
        sticker: BinaryIO | str | Path,
        *,
        emoji: str | None = None,
        reply_parameters: ReplyParameters | None = None,
    ) -> Message:
        """``sendSticker`` (no caption, per the API)."""
        ...

    async def send_media_group(
        self,
        chat_id: int,
        media: Sequence[Any],
        *,
        reply_parameters: ReplyParameters | None = None,
    ) -> tuple[Message, ...]:
        """``sendMediaGroup`` (2–10 items)."""
        ...


#: Kinds that accept a caption (stickers and video notes do not).
_CAPTION_KINDS = frozenset(
    {
        MediaKind.PHOTO,
        MediaKind.VIDEO,
        MediaKind.ANIMATION,
        MediaKind.DOCUMENT,
        MediaKind.AUDIO,
        MediaKind.VOICE,
    }
)


class TelegramAdapter(BaseAdapter):
    """Long-polling Telegram adapter built on python-telegram-bot.

    Args:
        token: Bot token (``SecretStr`` from settings, or a plain string).
        bot: Optional pre-built bot/client implementing
            :class:`TelegramBotPort`; injected by tests, otherwise a real
            ``telegram.Bot`` is created and owned by this adapter.
        allowed_updates: Update types to request; defaults to ``("message",
            "callback_query")``.
        max_download_bytes: ``getFile`` ceiling (from settings).
        connect_timeout: TCP/TLS connect timeout for the Bot request.
        read_timeout: Read timeout (downloads and long polls).
        write_timeout: Write timeout (uploads and other requests).
        logger: Logger override.
    """

    def __init__(
        self,
        token: SecretStr | str,
        *,
        bot: TelegramBotPort | None = None,
        allowed_updates: Sequence[str] = DEFAULT_ALLOWED_UPDATES,
        max_download_bytes: int | None = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        read_timeout: float = DEFAULT_READ_TIMEOUT_SECONDS,
        write_timeout: float = DEFAULT_WRITE_TIMEOUT_SECONDS,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("bridge.adapters.telegram")
        self._owns_bot = bot is None
        if bot is not None:
            self._bot: TelegramBotPort = bot
        else:
            request = HTTPXRequest(
                connection_pool_size=16,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                write_timeout=write_timeout,
                media_write_timeout=write_timeout,
                pool_timeout=DEFAULT_POOL_TIMEOUT_SECONDS,
            )
            self._bot = Bot(token=token_secret(token), request=request)
        self._allowed_updates = tuple(allowed_updates)
        self._max_download_bytes = int(
            max_download_bytes if max_download_bytes is not None else DEFAULT_MAX_DOWNLOAD_BYTES
        )

    @property
    def platform(self) -> Platform:
        """Always :attr:`Platform.TELEGRAM`."""
        return Platform.TELEGRAM

    # --------------------------------------------------------------- polling

    async def poll_once(self, offset: int | None, *, timeout: int) -> PollResult:
        """Fetch pending Telegram updates once."""
        try:
            updates = await self._bot.get_updates(
                offset=offset,
                limit=POLL_LIMIT,
                timeout=timeout,
                allowed_updates=list(self._allowed_updates),
            )
        except TelegramError as exc:
            raise translate_telegram_error(exc) from exc

        messages = tuple(
            msg for msg in (to_unified_message(update) for update in updates) if msg is not None
        )
        next_offset = max(update.update_id for update in updates) + 1 if updates else None
        if next_offset is not None and len(messages) != len(updates):
            self._log.debug(
                "skipped %d unsupported Telegram updates", len(updates) - len(messages)
            )
        return PollResult(
            messages=messages, next_offset=next_offset, received=len(updates)
        )

    # --------------------------------------------------------------- sending

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage:
        """Send a text message, optionally as a reply and with inline buttons."""
        body = clamp_message_text(text, logger=self._log)
        markup = _inline_markup(buttons)
        kwargs: dict[str, object] = {}
        if reply_to_message_id is not None:
            kwargs["reply_parameters"] = ReplyParameters(message_id=reply_to_message_id)
        if markup is not None:
            kwargs["reply_markup"] = markup
        try:
            message = await self._bot.send_message(chat_id, body, **kwargs)  # type: ignore[arg-type]
        except TelegramError as exc:
            raise translate_telegram_error(exc) from exc

        return SentMessage(
            platform=Platform.TELEGRAM, chat_id=chat_id, message_id=message.message_id
        )

    async def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage | None:
        """Edit an existing text message (used by the inline menu)."""
        body = clamp_message_text(text, logger=self._log)
        markup = _inline_markup(buttons)
        kwargs: dict[str, object] = {}
        if markup is not None:
            kwargs["reply_markup"] = markup
        try:
            result = await self._bot.edit_message_text(
                body, chat_id, message_id, **kwargs  # type: ignore[arg-type]
            )
        except TelegramError as exc:
            raise translate_telegram_error(exc) from exc
        if isinstance(result, Message):
            return SentMessage(
                platform=Platform.TELEGRAM,
                chat_id=chat_id,
                message_id=result.message_id,
            )
        return None

    async def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> None:
        """Stop the button spinner, optionally showing a short toast."""
        try:
            await self._bot.answer_callback_query(callback_query_id, text=text)
        except TelegramError as exc:
            raise translate_telegram_error(exc) from exc

    async def download_media(self, item: MediaItem, destination: Path) -> int:
        """Download one Telegram attachment into ``destination``.

        Memory payloads are written directly; ``url`` sources are streamed and
        ``file_id`` handles are resolved with ``getFile`` first.
        """
        if item.data is not None:
            return self._write_bytes(item.data, destination)
        if item.url is not None:
            return await download_url_to_file(
                item.url, destination, max_bytes=self._max_download_bytes
            )
        if not item.file_id:  # pragma: no cover - MediaItem guarantees a source
            raise ApiError("media item has no Telegram source")

        try:
            remote = await self._bot.get_file(item.file_id)
        except TelegramError as exc:
            raise translate_telegram_error(exc) from exc
        size = getattr(remote, "file_size", None)
        if isinstance(size, int) and size > self._max_download_bytes:
            raise MediaTooLargeError(
                size_bytes=size, limit_bytes=self._max_download_bytes, kind="فایل"
            )
        try:
            await remote.download_to_drive(custom_path=destination)
        except TelegramError as exc:
            raise translate_telegram_error(exc) from exc
        return destination.stat().st_size

    async def send_media(
        self,
        chat_id: int,
        item: OutboundMedia,
        *,
        reply_to_message_id: int | None = None,
    ) -> SentMessage:
        """Send one attachment with the method its kind requires."""
        if not isinstance(item.kind, MediaKind):  # pragma: no cover - typed upstream
            raise ApiError(f"unsupported media kind={item.kind!r}")
        reply = _reply_parameters(reply_to_message_id)
        caption = (
            clamp_message_text(item.caption, limit=CAPTION_MAX_LENGTH, logger=self._log)
            if item.caption and item.kind in _CAPTION_KINDS
            else None
        )
        try:
            with item.path.open("rb") as handle:
                message = await self._send_one(
                    chat_id, item, handle, caption=caption, reply=reply
                )
        except TelegramError as exc:
            raise translate_telegram_error(exc) from exc

        self._log.info(
            "telegram media sent kind=%s bytes=%s", item.kind.value, item.file_size
        )
        return SentMessage(
            platform=Platform.TELEGRAM, chat_id=chat_id, message_id=message.message_id
        )

    async def send_media_group(
        self,
        chat_id: int,
        items: Sequence[OutboundMedia],
        *,
        reply_to_message_id: int | None = None,
    ) -> tuple[SentMessage, ...]:
        """Send 2–10 attachments as one album, preserving order."""
        if len(items) < 2:
            raise ApiError("a Telegram album needs at least 2 items")
        reply = _reply_parameters(reply_to_message_id)
        opened: list[BinaryIO] = []
        try:
            media = []
            for item in items:
                handle = item.path.open("rb")
                opened.append(handle)
                media.append(
                    _input_media(
                        item,
                        handle,
                        caption=(
                            clamp_message_text(
                                item.caption, limit=CAPTION_MAX_LENGTH, logger=self._log
                            )
                            if item.caption
                            else None
                        ),
                    )
                )
            messages = await self._bot.send_media_group(
                chat_id, media, reply_parameters=reply
            )
        except TelegramError as exc:
            raise translate_telegram_error(exc) from exc
        finally:
            for stream in opened:
                stream.close()

        self._log.info(
            "telegram album sent items=%s bytes=%s",
            len(items),
            sum(item.file_size or 0 for item in items),
        )
        return tuple(
            SentMessage(
                platform=Platform.TELEGRAM, chat_id=chat_id, message_id=message.message_id
            )
            for message in messages
        )

    async def close(self) -> None:
        """Shut down the underlying bot when this adapter created it."""
        if not self._owns_bot:
            return
        shutdown = getattr(self._bot, "shutdown", None)
        if shutdown is not None:
            await shutdown()

    # -------------------------------------------------------------- internals

    def _write_bytes(self, data: bytes, destination: Path) -> int:
        """Write an in-memory payload to the spool file."""
        destination.write_bytes(data)
        return len(data)

    async def _send_one(
        self,
        chat_id: int,
        item: OutboundMedia,
        handle: BinaryIO,
        *,
        caption: str | None,
        reply: ReplyParameters | None,
    ) -> Message:
        """Dispatch one attachment to the matching ``send*`` method."""
        duration = int(item.duration) if item.duration else None
        if item.kind is MediaKind.PHOTO:
            return await self._bot.send_photo(
                chat_id, handle, caption=caption, reply_parameters=reply
            )
        if item.kind is MediaKind.VIDEO:
            return await self._bot.send_video(
                chat_id,
                handle,
                caption=caption,
                duration=duration,
                width=item.width,
                height=item.height,
                reply_parameters=reply,
            )
        if item.kind is MediaKind.ANIMATION:
            return await self._bot.send_animation(
                chat_id,
                handle,
                caption=caption,
                duration=duration,
                width=item.width,
                height=item.height,
                reply_parameters=reply,
            )
        if item.kind is MediaKind.DOCUMENT:
            return await self._bot.send_document(
                chat_id, handle, caption=caption, reply_parameters=reply
            )
        if item.kind is MediaKind.AUDIO:
            return await self._bot.send_audio(
                chat_id,
                handle,
                caption=caption,
                duration=duration,
                reply_parameters=reply,
            )
        if item.kind is MediaKind.VOICE:
            return await self._bot.send_voice(
                chat_id,
                handle,
                caption=caption,
                duration=duration,
                reply_parameters=reply,
            )
        if item.kind is MediaKind.VIDEO_NOTE:
            return await self._bot.send_video_note(
                chat_id,
                handle,
                duration=duration,
                length=item.width or item.height,
                reply_parameters=reply,
            )
        if item.kind is MediaKind.STICKER:
            return await self._bot.send_sticker(
                chat_id, handle, emoji=item.emoji, reply_parameters=reply
            )
        raise ApiError(f"Telegram cannot send kind={item.kind.value}")


def _reply_parameters(reply_to_message_id: int | None) -> ReplyParameters | None:
    """Reply markup for an optional anchor id."""
    if reply_to_message_id is None:
        return None
    return ReplyParameters(message_id=reply_to_message_id)


def _inline_markup(
    buttons: Sequence[Sequence[InlineButton]],
) -> InlineKeyboardMarkup | None:
    """Translate the domain button grid into PTB's inline markup."""
    rows = [
        [
            (
                InlineKeyboardButton(text=button.text, url=button.url)
                if button.url is not None
                else InlineKeyboardButton(
                    text=button.text, callback_data=button.callback_data
                )
            )
            for button in row
        ]
        for row in buttons
        if row
    ]
    return InlineKeyboardMarkup(rows) if rows else None


def _input_media(
    item: OutboundMedia, handle: BinaryIO, *, caption: str | None
) -> Any:
    """Build the ``InputMedia`` object for one album item."""
    if item.kind is MediaKind.PHOTO:
        return InputMediaPhoto(media=handle, caption=caption)
    if item.kind in {MediaKind.VIDEO, MediaKind.ANIMATION}:
        return InputMediaVideo(
            media=handle,
            caption=caption,
            duration=int(item.duration) if item.duration else None,
            width=item.width,
            height=item.height,
        )
    if item.kind is MediaKind.DOCUMENT:
        return InputMediaDocument(media=handle, caption=caption)
    if item.kind is MediaKind.AUDIO:
        return InputMediaAudio(
            media=handle, caption=caption, duration=int(item.duration) if item.duration else None
        )
    raise ApiError(f"Telegram albums cannot carry kind={item.kind.value}")


__all__ = [
    "CAPTION_MAX_LENGTH",
    "DEFAULT_ALLOWED_UPDATES",
    "DEFAULT_MAX_DOWNLOAD_BYTES",
    "POLL_LIMIT",
    "TelegramAdapter",
    "TelegramBotPort",
    "TelegramFilePort",
    "duration_seconds",
    "retry_after_seconds",
    "to_unified_message",
    "token_secret",
    "translate_telegram_error",
]
