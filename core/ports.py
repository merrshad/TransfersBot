"""Repository protocols the domain layer depends on.

The domain never imports ``storage``; it only knows these protocols, so a test
double or a future Postgres implementation can be swapped in without touching
business rules. ``storage/db.py`` provides the SQLite implementation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from core.models import (
    InlineButton,
    MediaItem,
    MessageRecord,
    OutboundMedia,
    OutboxEntry,
    OutboxStats,
    OutboxStatus,
    Platform,
    SentMessage,
)


@runtime_checkable
class TransactionManager(Protocol):
    """Runs a block of repository calls atomically."""

    def transaction(self) -> AbstractContextManager[None]:
        """Open a transaction; commit on success, roll back on exception."""
        ...


@runtime_checkable
class MessageSenderPort(Protocol):
    """Moves text and media in and out of one platform.

    Implemented by ``adapters.telegram.TelegramAdapter`` and
    ``adapters.bale.BaleAdapter``; the domain/bridge layer only sees this
    protocol, so it can be driven by fakes in tests without any network.

    Media cannot be forwarded by handle — a Telegram ``file_id`` is meaningless
    to Bale — so an adapter can both *download* an attachment of its own
    platform and *send* one to it.
    """

    @property
    def platform(self) -> Platform:
        """Messenger this sender talks to."""
        ...

    async def is_chat_admin(self, chat_id: int, user_id: int) -> bool:
        """Whether ``user_id`` is an administrator/creator of ``chat_id``.

        Raises:
            MessagingError: If the platform cannot answer the membership query.
                Callers must fail closed and deny the operation.
        """
        ...

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage:
        """Send a text message and return the platform message id.

        ``buttons`` is a grid of inline buttons (rows of buttons); an empty
        sequence sends no keyboard.

        Raises:
            AdapterError: A typed adapter error when delivery fails; see
                ``adapters.errors`` for the temporary/permanent distinction.
        """
        ...

    async def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage | None:
        """Edit an existing text message and its inline keyboard."""
        ...

    async def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> None:
        """Acknowledge an inline-button press (stops the client spinner)."""
        ...

    async def download_media(self, item: MediaItem, destination: Path) -> int:
        """Fetch ``item`` of *this* platform into ``destination``.

        Returns:
            The number of bytes written.

        Raises:
            core.errors.MediaTooLargeError: The file exceeds the platform's
                documented ``getFile`` download limit.
            core.errors.MessagingError: A typed messaging error when the fetch
                fails; a partial file is the caller's to delete.
        """
        ...

    async def send_media(
        self,
        chat_id: int,
        item: OutboundMedia,
        *,
        reply_to_message_id: int | None = None,
    ) -> SentMessage:
        """Send one attachment, with its caption, to ``chat_id``."""
        ...

    async def send_media_group(
        self,
        chat_id: int,
        items: Sequence[OutboundMedia],
        *,
        reply_to_message_id: int | None = None,
    ) -> tuple[SentMessage, ...]:
        """Send 2–10 attachments as one album, in order.

        Raises:
            core.errors.MediaUnsupportedError: The platform has no album API.
        """
        ...


@runtime_checkable
class MediaDownloadPort(Protocol):
    """The download half of a messenger adapter (used by the media worker).

    Split out so the delivery layer can type-annotate “an adapter that can fetch
    its own attachments” without depending on the full sender contract.
    """

    async def download_media(self, item: MediaItem, destination: Path) -> int:
        """Fetch ``item`` into ``destination`` and return the bytes written."""
        ...


@runtime_checkable
class MessageRepositoryPort(Protocol):
    """Storage of bridged messages, always scoped to a pair."""

    def add(
        self,
        *,
        pair_id: int,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
        created_at: datetime,
        target_message_id: int | None = None,
    ) -> tuple[MessageRecord, bool]:
        """Insert a message record; returns ``(record, created)``."""
        ...

    def get_for_pair(self, pair_id: int, message_id: int) -> MessageRecord | None:
        """Fetch a stored message by id, restricted to ``pair_id``."""
        ...

    def get_by_source_for_pair(
        self,
        pair_id: int,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
    ) -> MessageRecord | None:
        """Fetch a stored message by its natural source key within a pair."""
        ...

    def find_by_source(
        self,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
    ) -> MessageRecord | None:
        """Fetch a stored message by its natural source key across all pairs.

        Only used to decide whether an inbound message was already relayed
        (loop/idempotency guard); the result is never shown to users.
        """
        ...

    def find_by_target_in_pair(
        self,
        pair_id: int,
        *,
        source_platform: Platform,
        source_chat_id: int,
        target_message_id: int,
    ) -> MessageRecord | None:
        """Fetch the record a bot-delivered message id belongs to, per pair.

        ``source_platform``/``source_chat_id`` identify the chat the record was
        *sent from* (the partner's), so the lookup stays unambiguous even when
        both members are on the same platform or chat ids collide numerically
        across platforms. Used to map a reply back to the original message.
        """
        ...

    def attach_target_message(
        self, pair_id: int, message_id: int, target_message_id: int
    ) -> MessageRecord | None:
        """Record the id a message was delivered as on the target platform."""
        ...

    def list_for_pair(
        self, pair_id: int, *, limit: int = 50, after_id: int | None = None
    ) -> tuple[MessageRecord, ...]:
        """List recent messages of a single pair, oldest first."""
        ...


@runtime_checkable
class OutboxRepositoryPort(Protocol):
    """Durable queue of outbound payloads, each bound to one pair."""

    def enqueue(
        self,
        *,
        pair_id: int,
        payload: Mapping[str, Any],
        next_attempt_at: datetime,
        created_at: datetime,
    ) -> OutboxEntry:
        """Append a pending delivery for a pair."""
        ...

    def get_for_pair(self, pair_id: int, entry_id: int) -> OutboxEntry | None:
        """Fetch one entry, restricted to ``pair_id``."""
        ...

    def list_for_pair(
        self, pair_id: int, *, status: OutboxStatus | None = None
    ) -> tuple[OutboxEntry, ...]:
        """List a pair's outbound entries, oldest first."""
        ...

    def list_due(self, now: datetime, *, limit: int = 100) -> tuple[OutboxEntry, ...]:
        """List pending entries whose ``next_attempt_at`` has passed.

        This is the single deliberately global query: the delivery worker has no
        pair/user context when it polls. Callers must treat results as a queue,
        not as a way to read another pair's content.
        """
        ...

    def claim_due(self, now: datetime, *, limit: int = 100) -> tuple[OutboxEntry, ...]:
        """Lease due pending entries by atomically moving them to ``sending``."""
        ...

    def recover_orphaned(self, now: datetime | None = None) -> int:
        """Move every ``sending`` row back to ``pending`` and return the count."""
        ...

    def recover_stale(self, now: datetime, *, max_age: timedelta) -> int:
        """Move only ``sending`` leases older than ``max_age`` back to pending."""
        ...

    def mark_sent(
        self,
        pair_id: int,
        entry_id: int,
        *,
        sent_at: datetime | None = None,
        scrub_payload: bool = True,
    ) -> OutboxEntry | None:
        """Mark an entry delivered and (by default) drop its payload content."""
        ...

    def purge_sent(self, before: datetime) -> int:
        """Delete content-free ``sent`` tombstones older than ``before``."""
        ...

    def mark_failed(
        self,
        pair_id: int,
        entry_id: int,
        *,
        error: str,
        next_attempt_at: datetime,
        terminal: bool = False,
        now: datetime | None = None,
    ) -> OutboxEntry | None:
        """Record a failed attempt and its backoff schedule."""
        ...

    def defer(
        self,
        pair_id: int,
        entry_id: int,
        *,
        next_attempt_at: datetime,
        reason: str | None = None,
        now: datetime | None = None,
    ) -> OutboxEntry | None:
        """Return a lease to pending without counting a delivery attempt."""
        ...

    def queue_stats(self, now: datetime | None = None) -> OutboxStats:
        """Aggregate queue length, 24-hour errors and the last relay time."""
        ...


__all__ = [
    "MediaDownloadPort",
    "MessageRepositoryPort",
    "MessageSenderPort",
    "OutboxRepositoryPort",
    "TransactionManager",
]
