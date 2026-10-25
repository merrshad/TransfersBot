"""Group-to-group relaying: links, dedupe, sender labels and delivery."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.bridge import Bridge
from core.commands import CommandRouter
from core.delivery import GROUP_MEDIA_PAYLOAD_KIND, GROUP_TEXT_PAYLOAD_KIND, OutboxWorker
from core.groups import GroupLinkStatus
from core.media import MediaPolicy, TempMediaStore
from core.models import MediaItem, MediaKind, MessageRef, OutboxStatus, Platform, UnifiedMessage
from core.pairing import PairingService
from core.users import UserService
from storage.db import Repositories
from tests.conftest import FIXED_NOW, RecordingSender

TG_GROUP = -1001234567890
BALE_GROUP = 4497595490


@pytest.fixture
def link(repos: Repositories):
    """The configured Telegram ↔ Bale group link."""
    return repos.group_links.ensure_active(
        telegram_chat_id=TG_GROUP,
        bale_chat_id=BALE_GROUP,
        now=FIXED_NOW,
    )


@pytest.fixture
def group_bridge(
    users: UserService,
    pairing: PairingService,
    router: CommandRouter,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    owner_platform_ids,
) -> Bridge:
    """Bridge wired to the group repositories (no user registration needed)."""
    return Bridge(
        users=users,
        pairing=pairing,
        router=router,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        owner_platform_ids=owner_platform_ids,
        group_links=repos.group_links,
        group_messages=repos.group_messages,
        group_outbox=repos.group_outbox,
        clock=lambda: FIXED_NOW,
    )


@pytest.fixture
def group_worker(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_store: TempMediaStore,
    media_policy: MediaPolicy,
) -> OutboxWorker:
    """Worker for the group outbox, with the real media spool."""
    return OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.group_outbox,
        group_messages=repos.group_messages,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        retry_delay=timedelta(seconds=10),
        max_attempts=3,
        random_source=lambda: 0.5,
        store=media_store,
        policy=media_policy,
    )


def group_message(
    *,
    platform: Platform = Platform.TELEGRAM,
    chat_id: int = TG_GROUP,
    message_id: int = 10,
    text: str | None = "سلام",
    sender_id: int | None = None,
    sender_name: str = "Ali",
    is_bot: bool = False,
    media: tuple[MediaItem, ...] = (),
    reply_to: MessageRef | None = None,
) -> UnifiedMessage:
    """A group UnifiedMessage helper."""
    if sender_id is None:
        sender_id = 7001 if platform is Platform.TELEGRAM else 8001
    return UnifiedMessage(
        source=platform,
        sender_id=sender_id,
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        media=media,
        reply_to=reply_to,
        timestamp=FIXED_NOW,
        chat_type="supergroup" if platform is Platform.TELEGRAM else "group",
        sender_name=sender_name,
        is_bot_sender=is_bot,
    )


def test_group_link_ensure_is_idempotent(repos: Repositories) -> None:
    first = repos.group_links.ensure_active(
        telegram_chat_id=TG_GROUP, bale_chat_id=BALE_GROUP, now=FIXED_NOW
    )
    second = repos.group_links.ensure_active(
        telegram_chat_id=TG_GROUP, bale_chat_id=BALE_GROUP, now=FIXED_NOW
    )

    assert first.id == second.id
    assert first.status is GroupLinkStatus.ACTIVE
    assert repos.group_links.get_active_for_source(Platform.TELEGRAM, TG_GROUP) == first
    assert repos.group_links.get_active_for_source(Platform.BALE, BALE_GROUP) == first


async def test_group_text_is_queued_with_sender_prefix(
    group_bridge: Bridge, repos: Repositories, link
) -> None:
    await group_bridge.handle_message(group_message(text="سلام گروه", sender_name="علی"))

    record = repos.group_messages.find_by_source(Platform.TELEGRAM, TG_GROUP, 10)
    assert record is not None
    (entry,) = repos.group_outbox.list_for_link(link.id)
    assert entry.status is OutboxStatus.PENDING
    assert entry.payload["kind"] == GROUP_TEXT_PAYLOAD_KIND
    assert entry.payload["target_platform"] == "bale"
    assert entry.payload["target_chat_id"] == BALE_GROUP
    assert "علی" in entry.payload["text"]
    assert "سلام گروه" in entry.payload["text"]


async def test_group_commands_and_bot_messages_are_not_relayed(
    group_bridge: Bridge, repos: Repositories, link
) -> None:
    await group_bridge.handle_message(group_message(text="/start"))
    await group_bridge.handle_message(
        group_message(message_id=11, text="ربات", is_bot=True)
    )

    assert repos.group_outbox.list_for_link(link.id) == ()
    assert repos.group_messages.find_by_source(Platform.TELEGRAM, TG_GROUP, 10) is None


async def test_group_messages_are_deduplicated(
    group_bridge: Bridge, repos: Repositories, link
) -> None:
    message = group_message(message_id=12, text="تکراری")
    await group_bridge.handle_message(message)
    await group_bridge.handle_message(message)

    assert len(repos.group_outbox.list_for_link(link.id)) == 1
    assert repos.group_messages.find_by_source(Platform.TELEGRAM, TG_GROUP, 12)


async def test_group_reply_maps_to_the_other_groups_message(
    group_bridge: Bridge, repos: Repositories, link
) -> None:
    await group_bridge.handle_message(group_message(message_id=20, text="اصل"))
    record = repos.group_messages.find_by_source(Platform.TELEGRAM, TG_GROUP, 20)
    assert record is not None
    repos.group_messages.attach_target_message(record.id, 99)

    # A Bale user replies to the bot-delivered copy (id 99) in the Bale group.
    await group_bridge.handle_message(
        group_message(
            platform=Platform.BALE,
            chat_id=BALE_GROUP,
            message_id=30,
            text="پاسخ",
            sender_id=8002,
            sender_name="Sara",
            reply_to=MessageRef(platform=Platform.BALE, chat_id=BALE_GROUP, message_id=99),
        )
    )

    replies = [
        entry
        for entry in repos.group_outbox.list_for_link(link.id)
        if entry.payload.get("source_message_id") == 30
    ]
    assert replies and replies[0].payload["reply_to_message_id"] == 20


async def test_group_text_worker_delivers_and_attaches_target_id(
    group_bridge: Bridge,
    group_worker: OutboxWorker,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    link,
) -> None:
    await group_bridge.handle_message(group_message(message_id=40, text="تحویل", sender_name="Ali"))

    stats = await group_worker.process_due()

    assert stats.delivered == 1
    assert senders[Platform.BALE].texts_to(BALE_GROUP) == ["👤 Ali:\nتحویل"]
    record = repos.group_messages.find_by_source(Platform.TELEGRAM, TG_GROUP, 40)
    assert record is not None and record.target_message_id == 1
    (entry,) = repos.group_outbox.list_for_link(link.id)
    assert entry.status is OutboxStatus.SENT
    assert entry.payload == {}, "delivered group content is scrubbed"


async def test_group_media_is_queued_and_delivered(
    group_bridge: Bridge,
    group_worker: OutboxWorker,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    link,
) -> None:
    photo = MediaItem(kind=MediaKind.PHOTO, data=b"jpeg-bytes", mime_type="image/jpeg")
    await group_bridge.handle_message(
        group_message(message_id=50, text="کپشن", media=(photo,), sender_name="Ali")
    )

    (entry,) = repos.group_outbox.list_for_link(link.id)
    assert entry.payload["kind"] == GROUP_MEDIA_PAYLOAD_KIND
    assert entry.payload["items"]

    stats = await group_worker.process_due()

    assert stats.delivered == 1
    assert len(senders[Platform.BALE].media) == 1
    assert senders[Platform.BALE].media[0].chat_id == BALE_GROUP
    assert senders[Platform.BALE].media[0].items[0].caption.startswith("👤 Ali:")
