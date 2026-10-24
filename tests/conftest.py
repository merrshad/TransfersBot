"""Shared pytest fixtures. No fixture performs network or real-token work."""

from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from core.bridge import Bridge
from core.commands import CommandRouter
from core.delivery import OutboxWorker
from core.errors import MessagingError
from core.media import MEGABYTE, MediaPolicy, TempMediaStore
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
from core.pairing import PairingService
from core.users import User, UserRole, UserService
from storage.db import Repositories

UTC = UTC
FIXED_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


class FakeClock:
    """Deterministic, advanceable UTC clock for service-level date logic."""

    def __init__(self, now: datetime | None = None) -> None:
        self._now = now or FIXED_NOW

    def __call__(self) -> datetime:
        return self._now

    @property
    def now(self) -> datetime:
        return self._now

    def advance(self, **delta: float) -> datetime:
        """Move the clock forward (``clock.advance(hours=3)``)."""
        self._now += timedelta(**delta)
        return self._now

    def set(self, moment: datetime) -> datetime:
        """Jump to an absolute time."""
        self._now = moment
        return self._now


@pytest.fixture
def clock() -> FakeClock:
    """A fixed, controllable clock."""
    return FakeClock()


@pytest.fixture
def repos(tmp_path: Path) -> Iterator[Repositories]:
    """Repositories backed by a real (temporary) SQLite file."""
    bundle = Repositories.open(tmp_path / "nested" / "bridge.db")
    try:
        yield bundle
    finally:
        bundle.close()


@pytest.fixture
def memory_repos() -> Iterator[Repositories]:
    """Repositories backed by an in-memory database."""
    bundle = Repositories.open(":memory:")
    try:
        yield bundle
    finally:
        bundle.close()


@pytest.fixture
def users(repos: Repositories) -> UserService:
    """User service with no registration cap."""
    return UserService(repos.users, repos.db, max_users=0)


@pytest.fixture
def pairing(repos: Repositories, clock: FakeClock) -> PairingService:
    """Pairing service with default limits (1 pair/user, 24h TTL)."""
    return PairingService(
        repos.users,
        repos.pairs,
        repos.requests,
        repos.db,
        max_pairs_per_user=1,
        request_ttl=timedelta(hours=24),
        invites=repos.invites,
        invite_ttl=timedelta(minutes=15),
        clock=clock,
    )


@pytest.fixture
def router(users: UserService, pairing: PairingService) -> CommandRouter:
    """Command router wired to the default services."""
    return CommandRouter(
        users,
        pairing,
        telegram_bot_username="TestTelegramBot",
        bale_bot_username="TestBaleBot",
    )


@dataclass(frozen=True, slots=True)
class SentCall:
    """One recorded delivery from a fake sender."""

    chat_id: int
    text: str
    reply_to_message_id: int | None = None
    buttons: tuple[tuple[InlineButton, ...], ...] = ()


@dataclass(frozen=True, slots=True)
class EditedCall:
    """One recorded in-place menu edit."""

    chat_id: int
    message_id: int
    text: str
    buttons: tuple[tuple[InlineButton, ...], ...] = ()


@dataclass(frozen=True, slots=True)
class SentMediaCall:
    """One recorded media delivery (a single attachment or one album)."""

    chat_id: int
    items: tuple[OutboundMedia, ...]
    reply_to_message_id: int | None = None
    album: bool = False

    @property
    def kinds(self) -> tuple[MediaKind, ...]:
        return tuple(item.kind for item in self.items)

    @property
    def captions(self) -> tuple[str | None, ...]:
        return tuple(item.caption for item in self.items)


#: Bytes written by the fake downloader when an item carries no payload.
FAKE_MEDIA_BYTES = b"fake-media-bytes"


class RecordingSender:
    """In-memory :class:`core.ports.MessageSenderPort` for offline tests.

    Deliveries are recorded instead of sent; ``failures`` maps a chat id to the
    error that sending to it must raise, which is how blocked/down recipients
    are simulated without any network. Media is faked the same way: downloads
    write :data:`FAKE_MEDIA_BYTES` to the spool path (or ``download_data``), and
    uploads are recorded with their resolved paths and captions.
    """

    def __init__(self, platform: Platform) -> None:
        self._platform = platform
        self.sent: list[SentCall] = []
        self.edited: list[EditedCall] = []
        self.answered_callbacks: list[str] = []
        self.media: list[SentMediaCall] = []
        self.downloads: list[tuple[MediaItem, Path]] = []
        self.failures: dict[int, MessagingError] = {}
        self.download_data: bytes = FAKE_MEDIA_BYTES
        self.download_error: MessagingError | None = None
        self.media_error: MessagingError | None = None
        self.album_error: MessagingError | None = None
        self._next_id = 1000

    @property
    def platform(self) -> Platform:
        return self._platform

    @property
    def texts(self) -> list[str]:
        return [call.text for call in self.sent]

    def texts_to(self, chat_id: int) -> list[str]:
        """Every text delivered to ``chat_id``, in order."""
        return [call.text for call in self.sent if call.chat_id == chat_id]

    @property
    def media_to(self) -> list[SentMediaCall]:
        """Every media delivery, in order."""
        return list(self.media)

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage:
        error = self.failures.get(chat_id)
        if error is not None:
            raise error
        self.sent.append(
            SentCall(
                chat_id,
                text,
                reply_to_message_id,
                tuple(tuple(row) for row in buttons),
            )
        )
        return SentMessage(
            platform=self._platform, chat_id=chat_id, message_id=len(self.sent)
        )

    async def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage:
        self.edited.append(
            EditedCall(
                chat_id,
                message_id,
                text,
                tuple(tuple(row) for row in buttons),
            )
        )
        return SentMessage(
            platform=self._platform, chat_id=chat_id, message_id=message_id
        )

    async def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> None:
        self.answered_callbacks.append(callback_query_id)

    async def download_media(self, item: MediaItem, destination: Path) -> int:
        """Pretend to fetch ``item`` from this platform into ``destination``."""
        if self.download_error is not None:
            raise self.download_error
        data = item.data if item.data is not None else self.download_data
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        self.downloads.append((item, destination))
        return len(data)

    async def send_media(
        self,
        chat_id: int,
        item: OutboundMedia,
        *,
        reply_to_message_id: int | None = None,
    ) -> SentMessage:
        if self.media_error is not None:
            raise self.media_error
        self.media.append(SentMediaCall(chat_id, (item,), reply_to_message_id))
        self._next_id += 1
        return SentMessage(
            platform=self._platform, chat_id=chat_id, message_id=self._next_id
        )

    async def send_media_group(
        self,
        chat_id: int,
        items: Sequence[OutboundMedia],
        *,
        reply_to_message_id: int | None = None,
    ) -> tuple[SentMessage, ...]:
        if self.album_error is not None:
            raise self.album_error
        self.media.append(
            SentMediaCall(chat_id, tuple(items), reply_to_message_id, album=True)
        )
        sent: list[SentMessage] = []
        for _ in items:
            self._next_id += 1
            sent.append(
                SentMessage(
                    platform=self._platform, chat_id=chat_id, message_id=self._next_id
                )
            )
        return tuple(sent)


@pytest.fixture
def senders() -> dict[Platform, RecordingSender]:
    """Fake senders for both platforms."""
    return {
        Platform.TELEGRAM: RecordingSender(Platform.TELEGRAM),
        Platform.BALE: RecordingSender(Platform.BALE),
    }


@pytest.fixture
def owner_platform_ids() -> dict[Platform, int]:
    """Configured owner identities used by the bridge."""
    return {Platform.TELEGRAM: 9001, Platform.BALE: 9002}


@pytest.fixture
def bridge(
    users: UserService,
    pairing: PairingService,
    router: CommandRouter,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    owner_platform_ids: dict[Platform, int],
    clock: FakeClock,
) -> Bridge:
    """Bridge wired to the real services, repositories and the fake senders."""
    return Bridge(
        users=users,
        pairing=pairing,
        router=router,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        owner_platform_ids=owner_platform_ids,
        clock=clock,
    )


@pytest.fixture
def media_store(tmp_path: Path) -> TempMediaStore:
    """Spool store in a temporary directory (200 MB per-user quota)."""
    return TempMediaStore(tmp_path / "spool", quota_bytes=200 * MEGABYTE)


@pytest.fixture
def media_policy() -> MediaPolicy:
    """Documented limits with sticker conversion disabled (deterministic)."""
    return MediaPolicy(can_convert_stickers=False)


@pytest.fixture
def media_worker(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_store: TempMediaStore,
    media_policy: MediaPolicy,
) -> OutboxWorker:
    """Delivery worker wired for media: real spool, fake senders, fixed clock."""
    return OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        retry_delay=timedelta(seconds=10),
        store=media_store,
        policy=media_policy,
    )


@pytest.fixture
def bridge_deps(
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> dict[str, object]:
    """Service bundle for tests that build their own :class:`Bridge`."""
    return {
        "users": users,
        "pairing": pairing,
        "messages": repos.messages,
        "outbox": repos.outbox,
        "tx": repos.db,
        "senders": senders,
    }


@pytest.fixture
def incoming() -> Callable[..., UnifiedMessage]:
    """Factory building inbound :class:`UnifiedMessage` objects."""
    counter = itertools.count(1)

    def _make(
        *,
        platform: Platform = Platform.TELEGRAM,
        sender_id: int = 5001,
        text: str | None = "/start",
        chat_id: int | None = None,
        message_id: int | None = None,
        entities: tuple[Mapping[str, Any], ...] = (),
        reply_to: MessageRef | None = None,
    ) -> UnifiedMessage:
        return UnifiedMessage(
            source=platform,
            sender_id=sender_id,
            chat_id=chat_id if chat_id is not None else sender_id,
            message_id=message_id if message_id is not None else next(counter),
            text=text,
            entities=entities,
            reply_to=reply_to,
            timestamp=FIXED_NOW,
        )

    return _make


@pytest.fixture
def register(users: UserService) -> Callable[..., User]:
    """Factory registering users with automatic platform ids."""
    counter = itertools.count(1000)

    def _register(
        *,
        platform: Platform = Platform.TELEGRAM,
        platform_user_id: int | None = None,
        role: UserRole = UserRole.USER,
    ) -> User:
        return users.get_or_register(
            platform,
            platform_user_id if platform_user_id is not None else next(counter),
            role=role,
        )

    return _register
