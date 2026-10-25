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

from core.models import OutboxEntry, Platform


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

    def ensure_active(
        self, *, telegram_chat_id: int, bale_chat_id: int, now: datetime
    ) -> GroupLink:
        """Create/reactivate the configured link and deactivate other links."""
        ...

    def get_active_for_source(
        self, platform: Platform, chat_id: int
    ) -> GroupLink | None:
        """Return the active link that contains this source group."""
        ...

    def get(self, link_id: int) -> GroupLink | None:
        """Fetch a link by id."""
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
    "GroupLink",
    "GroupLinkRepositoryPort",
    "GroupLinkStatus",
    "GroupMessage",
    "GroupMessageRepositoryPort",
    "GroupOutboxRepositoryPort",
]
