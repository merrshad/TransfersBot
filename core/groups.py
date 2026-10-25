"""Group-to-group bridging models and repository contracts.

A *group link* binds one Telegram group/supergroup to one Bale group. Group
messages use their own message mapping and outbox, so the existing user-pair
flow (``pairs``/``messages``) stays untouched.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from core.errors import DomainError
from core.models import OutboxEntry, Platform


class GroupOwnerOnlyError(DomainError):
    """Raised when someone other than the configured owner tries to link groups."""

    code = "group_owner_only"

    def __init__(
        self, message: str = "⛔️ فقط مالک ربات می‌تواند گروه‌ها را جفت کند."
    ) -> None:
        super().__init__(message)


class GroupAlreadyLinkedError(DomainError):
    """Raised when a group already belongs to an active link."""

    code = "group_already_linked"

    def __init__(
        self,
        message: str = (
            "این گروه قبلاً به یک گروه دیگر وصل شده است.\n"
            "برای قطع سینک: /groupsync off"
        ),
    ) -> None:
        super().__init__(message)


class GroupInviteInvalidError(DomainError):
    """Raised when a group pairing code is unknown, used or expired."""

    code = "group_invite_invalid"

    def __init__(
        self,
        message: str = (
            "کد اتصال نامعتبر یا منقضی شده است.\n"
            "دوباره /groupsync را در گروه اول بفرستید."
        ),
    ) -> None:
        super().__init__(message)


class GroupLinkNotFoundError(DomainError):
    """Raised when an operation needs an active link that does not exist."""

    code = "group_link_not_found"

    def __init__(
        self, message: str = "این گروه لینک فعالی ندارد."
    ) -> None:
        super().__init__(message)


class GroupLinkStatus(str, Enum):
    """Lifecycle of a group-to-group bridge."""

    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclass(frozen=True, slots=True)
class GroupLink:
    """One active/inactive bridge between a Telegram and a Bale group."""

    id: int
    telegram_chat_id: int
    bale_chat_id: int
    status: GroupLinkStatus
    created_at: datetime
    updated_at: datetime

    @property
    def is_active(self) -> bool:
        """Whether messages should be relayed."""
        return self.status is GroupLinkStatus.ACTIVE

    def target_for(self, source: Platform) -> tuple[Platform, int]:
        """Return ``(target_platform, target_chat_id)`` for a source group."""
        if source is Platform.TELEGRAM:
            return Platform.BALE, self.bale_chat_id
        return Platform.TELEGRAM, self.telegram_chat_id


@dataclass(frozen=True, slots=True)
class GroupLinkInvite:
    """A single-use code that lets the owner link two groups."""

    id: int
    token: str
    source_platform: Platform
    source_chat_id: int
    created_by: int
    created_at: datetime
    expires_at: datetime
    used_at: datetime | None
    used_chat_id: int | None

    @property
    def is_used(self) -> bool:
        """Whether the code has already completed a link."""
        return self.used_at is not None

    def is_pending(self, now: datetime) -> bool:
        """Whether the code can still be used at ``now``."""
        return not self.is_used and self.expires_at > now


@dataclass(frozen=True, slots=True)
class KnownGroup:
    """A group the bot has seen, even before it is linked."""

    id: int
    platform: Platform
    chat_id: int
    title: str | None
    discovered_by: int
    created_at: datetime
    last_seen_at: datetime

    @property
    def display_name(self) -> str:
        """A safe, human-friendly label for inline buttons."""
        raw = " ".join((self.title or "").split())
        return raw or str(self.chat_id)


@dataclass(frozen=True, slots=True)
class GroupMessage:
    """Mapping between one source group message and its delivered copy."""

    id: int
    group_link_id: int
    source_platform: Platform
    source_chat_id: int
    source_message_id: int
    target_message_id: int | None
    created_at: datetime


@runtime_checkable
class GroupLinkRepositoryPort(Protocol):
    """Persistence required by the group relay."""

    def link(
        self, *, telegram_chat_id: int, bale_chat_id: int, now: datetime
    ) -> GroupLink:
        """Create one active link, preserving other independent links.

        Raises:
            GroupAlreadyLinkedError: If either group already belongs to an
                active link.
        """
        ...

    def seed_if_empty(
        self, *, telegram_chat_id: int, bale_chat_id: int, now: datetime
    ) -> GroupLink | None:
        """Create the legacy env-configured link only when no link exists."""
        ...

    def get_active_for_source(
        self, platform: Platform, chat_id: int
    ) -> GroupLink | None:
        """Return the active link that contains this source group."""
        ...

    def get(self, link_id: int) -> GroupLink | None:
        """Fetch a link by id."""
        ...

    def deactivate(self, link_id: int, *, now: datetime) -> GroupLink | None:
        """Move one link to inactive; other links are untouched."""
        ...

    def list_active(self) -> tuple[GroupLink, ...]:
        """Return every active link, oldest first."""
        ...


@runtime_checkable
class GroupOutboxRepositoryPort(Protocol):
    """The enqueue slice used by the group bridge."""

    def enqueue(
        self,
        *,
        group_link_id: int,
        payload: Mapping[str, Any],
        next_attempt_at: datetime,
        created_at: datetime,
    ) -> OutboxEntry:
        """Append a pending group delivery."""
        ...

    def cancel_pending(self, group_link_id: int) -> int:
        """Delete queued-but-not-sent group deliveries for one link."""
        ...


@runtime_checkable
class KnownGroupRepositoryPort(Protocol):
    """Groups discovered from messages so the owner can pick one in a button."""

    def upsert(
        self,
        *,
        platform: Platform,
        chat_id: int,
        title: str | None,
        discovered_by: int,
        now: datetime,
    ) -> KnownGroup:
        """Insert/update a group's metadata and last-seen time."""
        ...

    def get(self, platform: Platform, chat_id: int) -> KnownGroup | None:
        """Fetch a known group."""
        ...

    def list_by_platform(self, platform: Platform) -> tuple[KnownGroup, ...]:
        """All known groups for one platform, oldest first."""
        ...


@runtime_checkable
class GroupLinkInviteRepositoryPort(Protocol):
    """Persistence for single-use, owner-created group pairing codes."""

    def create(
        self,
        *,
        token: str,
        source_platform: Platform,
        source_chat_id: int,
        created_by: int,
        created_at: datetime,
        expires_at: datetime,
    ) -> GroupLinkInvite:
        """Store a new pending code."""
        ...

    def get_pending(
        self, token: str, *, now: datetime
    ) -> GroupLinkInvite | None:
        """Return an unused, unexpired code by its token."""
        ...

    def find_pending_for_source(
        self, platform: Platform, chat_id: int, *, now: datetime
    ) -> GroupLinkInvite | None:
        """Return the newest pending code for a source group."""
        ...

    def mark_used(
        self, token: str, *, used_at: datetime, used_chat_id: int
    ) -> GroupLinkInvite | None:
        """Consume a code atomically; ``None`` if already used/invalid."""
        ...

    def revoke_pending_for_source(
        self, platform: Platform, chat_id: int
    ) -> int:
        """Delete pending codes for a source group (re-issue flow)."""
        ...


@runtime_checkable
class GroupMessageRepositoryPort(Protocol):
    """Persistence for group message ids, deduplication and reply mapping."""

    def add(
        self,
        *,
        group_link_id: int,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
        created_at: datetime,
    ) -> tuple[GroupMessage, bool]:
        """Insert idempotently; returns ``(record, created)``."""
        ...

    def find_by_source(
        self,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
    ) -> GroupMessage | None:
        """Look up a message by its global source key (dedup guard)."""
        ...

    def find_by_target_in_link(
        self,
        group_link_id: int,
        *,
        source_platform: Platform,
        source_chat_id: int,
        target_message_id: int,
    ) -> GroupMessage | None:
        """Find the record a delivered target message id belongs to."""
        ...

    def attach_target_message(
        self, group_message_id: int, target_message_id: int
    ) -> GroupMessage | None:
        """Store the platform id the group copy was delivered as."""
        ...


__all__ = [
    "GroupAlreadyLinkedError",
    "GroupInviteInvalidError",
    "GroupLink",
    "GroupLinkInvite",
    "GroupLinkInviteRepositoryPort",
    "GroupLinkNotFoundError",
    "GroupLinkRepositoryPort",
    "GroupLinkStatus",
    "GroupMessage",
    "GroupMessageRepositoryPort",
    "GroupOutboxRepositoryPort",
    "GroupOwnerOnlyError",
    "KnownGroup",
    "KnownGroupRepositoryPort",
]
