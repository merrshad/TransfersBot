"""Platform-independent message and persistence view models.

Messenger adapters (Telegram/Bale) translate their native updates into
:class:`UnifiedMessage`; nothing downstream needs to know the origin SDK.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any


class Platform(str, Enum):
    """Messenger a user or message belongs to."""

    TELEGRAM = "telegram"
    BALE = "bale"


class MediaKind(str, Enum):
    """Normalised attachment kinds understood by both messengers."""

    PHOTO = "photo"
    VIDEO = "video"
    AUDIO = "audio"
    VOICE = "voice"
    DOCUMENT = "document"
    STICKER = "sticker"
    ANIMATION = "animation"
    VIDEO_NOTE = "video_note"


@dataclass(frozen=True, slots=True)
class MediaItem:
    """A single attachment and everything the relay needs to move it.

    Exactly one *source* must be provided:

    * ``data`` — the bytes are already in memory (tests, downloaded files);
    * ``url`` — a ready-to-use HTTP(S) URL the adapter can stream;
    * ``file_id`` — a platform handle (Telegram/Bale ``file_id``) that only the
      owning adapter can resolve with ``getFile``.

    ``file_size``/``mime_type`` may be unknown until the adapter resolves the
    handle; the metadata that matters for rendering (``caption``, ``duration``,
    ``width``/``height``, ``emoji``) is carried along so the target can receive
    a faithful message.
    """

    kind: MediaKind
    file_name: str | None = None
    mime_type: str | None = None
    file_size: int | None = None
    data: bytes | None = None
    url: str | None = None
    file_id: str | None = None
    caption: str | None = None
    duration: float | None = None
    width: int | None = None
    height: int | None = None
    emoji: str | None = None

    def __post_init__(self) -> None:
        sources = [value is not None for value in (self.data, self.url, self.file_id)]
        if sum(sources) != 1:
            raise ValueError(
                "MediaItem requires exactly one of 'data', 'url' or 'file_id'."
            )
        if self.data is not None and not self.data:
            raise ValueError("MediaItem data must not be empty.")
        if self.url is not None and not self.url.strip():
            raise ValueError("MediaItem url must not be blank.")
        if self.file_id is not None and not self.file_id.strip():
            raise ValueError("MediaItem file_id must not be blank.")
        if self.file_size is not None and self.file_size < 0:
            raise ValueError("MediaItem file_size must be non-negative.")

    @property
    def has_bytes(self) -> bool:
        """Whether the payload is already in memory."""
        return self.data is not None

    @property
    def needs_download(self) -> bool:
        """Whether an adapter must fetch the bytes before they can be relayed."""
        return self.data is None


@dataclass(frozen=True, slots=True)
class InlineButton:
    """One inline (glass) button: either a callback or an external URL.

    Exactly one of ``callback_data`` and ``url`` must be provided. URL buttons
    are used for cross-bot deep links (Telegram ↔ Bale pairing).
    """

    text: str
    callback_data: str | None = None
    url: str | None = None

    def __post_init__(self) -> None:
        if not self.text.strip():
            raise ValueError("button text must not be blank.")
        if (self.callback_data is None) == (self.url is None):
            raise ValueError("an inline button needs exactly one of callback_data/url.")
        if self.callback_data is not None:
            if not self.callback_data:
                raise ValueError("callback_data must not be empty.")
            if len(self.callback_data.encode("utf-8")) > 64:
                raise ValueError(
                    "callback_data must be at most 64 bytes (Telegram limit)."
                )
        if self.url is not None and not self.url.startswith(("http://", "https://")):
            raise ValueError("button url must be an http(s) URL.")


@dataclass(frozen=True, slots=True)
class MessageRef:
    """Reference to a message in a specific chat, used for reply threading."""

    platform: Platform
    chat_id: int
    message_id: int


@dataclass(frozen=True, slots=True)
class UnifiedMessage:
    """A messenger-neutral inbound message.

    Attributes:
        source: Messenger the message arrived from.
        sender_id: Platform-specific id of the sending user.
        chat_id: Platform-specific chat id (private chat for this bot).
        message_id: Platform-specific message id, unique per chat.
        text: Plain text body, if any.
        entities: Raw platform entities (mentions, links, formatting) passed
            through unchanged for later re-rendering.
        media: Ordered attachments.
        reply_to: Message this one replies to, if any.
        timestamp: Timezone-aware creation time (UTC recommended).
    """

    source: Platform
    sender_id: int
    chat_id: int
    message_id: int
    text: str | None = None
    entities: tuple[Mapping[str, Any], ...] = ()
    media: tuple[MediaItem, ...] = ()
    reply_to: MessageRef | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    media_group_id: str | None = None
    unsupported: str | None = None
    # An inline-button press travels through the same polling path as a
    # message, but it is a command-like event and is never relayed.
    callback_data: str | None = None
    callback_id: str | None = None
    # Group bridging needs the chat kind and a human sender label.
    chat_type: str = "private"
    chat_title: str | None = None
    sender_name: str | None = None
    is_bot_sender: bool = False

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("UnifiedMessage.timestamp must be timezone-aware.")

    @property
    def key(self) -> tuple[str, int, int]:
        """Natural key matching the ``messages`` unique index."""
        return (self.source.value, self.chat_id, self.message_id)

    @property
    def is_group(self) -> bool:
        """Whether this message came from a group/supergroup."""
        return self.chat_type in {"group", "supergroup"}

    @property
    def has_media(self) -> bool:
        """Whether the message carries at least one attachment."""
        return bool(self.media)

    @property
    def is_relayable(self) -> bool:
        """Whether this message carries anything the bridge can relay.

        A body, at least one attachment, or an unsupported-type placeholder are
        all relayable; a button press and an empty update are not.
        """
        if self.callback_data is not None:
            return False
        return bool(self.text) or bool(self.media) or self.unsupported is not None


@dataclass(frozen=True, slots=True)
class MessageRecord:
    """Persisted record of a bridged message (source and target ids)."""

    id: int
    pair_id: int
    source_platform: Platform
    source_chat_id: int
    source_message_id: int
    target_message_id: int | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class OutboundMedia:
    """One attachment resolved to a local file, ready for the target platform.

    Produced by the delivery layer (download + any conversion) and handed to an
    adapter's ``send_media``/``send_media_group``.
    """

    kind: MediaKind
    path: Path
    file_name: str | None = None
    mime_type: str | None = None
    file_size: int | None = None
    duration: float | None = None
    width: int | None = None
    height: int | None = None
    emoji: str | None = None
    caption: str | None = None


@dataclass(frozen=True, slots=True)
class SentMessage:
    """Result of a successful outbound delivery.

    Returned by a messenger adapter so the caller can correlate a delivered
    message with its platform message id (used for replies and later phases).
    """

    platform: Platform
    chat_id: int
    message_id: int


class OutboxStatus(str, Enum):
    """Lifecycle of an outbound message waiting to be delivered.

    ``SENDING`` is a short-lived lease: a worker atomically claims a pending
    entry before touching the network. If the process dies mid-send the row
    stays ``SENDING``; the next worker start turns every orphaned lease back
    into ``PENDING`` so the message is retried instead of being lost.
    """

    PENDING = "pending"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """An outbound payload scoped to one pair, with retry bookkeeping."""

    id: int
    pair_id: int
    payload: Mapping[str, Any]
    attempts: int
    next_attempt_at: datetime
    status: OutboxStatus
    created_at: datetime
    last_error: str | None = None
    locked_at: datetime | None = None
    sent_at: datetime | None = None
    last_attempt_at: datetime | None = None
    group_link_id: int | None = None


@dataclass(frozen=True, slots=True)
class OutboxStats:
    """Aggregate outbox counters used by the owner's ``/status`` command.

    Counts only — never payloads or recipient ids.
    """

    pending: int = 0
    sending: int = 0
    sent: int = 0
    failed: int = 0
    errors_24h: int = 0
    last_relay_at: datetime | None = None

    @property
    def queue_length(self) -> int:
        """Entries still waiting to be delivered (pending + in-flight)."""
        return self.pending + self.sending
