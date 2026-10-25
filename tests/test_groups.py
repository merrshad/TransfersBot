"""Group-to-group relaying: links, dedupe, sender labels and delivery."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.bridge import Bridge
from core.commands import CommandRouter
from core.delivery import GROUP_MEDIA_PAYLOAD_KIND, GROUP_TEXT_PAYLOAD_KIND, OutboxWorker
from core.groups import GroupLinkStatus
from core.ids import new_group_token
from core.media import MediaPolicy, TempMediaStore
from core.models import MediaItem, MediaKind, MessageRef, OutboxStatus, Platform, UnifiedMessage
from core.pairing import PairingService
from core.users import UserService
from storage.db import Repositories
from tests.conftest import FIXED_NOW, RecordingSender

TG_GROUP = -1001234567890
BALE_GROUP = 4497595490
TG_GROUP_2 = -1009876543210
BALE_GROUP_2 = 4497595491


@pytest.fixture
def link(repos: Repositories):
    """The configured Telegram ↔ Bale group link."""
    return repos.group_links.link(
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
        group_invites=repos.group_invites,
        known_groups=repos.known_groups,
        group_invite_ttl=timedelta(minutes=15),
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
    callback_data: str | None = None,
    callback_id: str | None = None,
    chat_title: str | None = None,
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
        chat_title=chat_title,
        sender_name=sender_name,
        is_bot_sender=is_bot,
        callback_data=callback_data,
        callback_id=callback_id,
    )


def test_multiple_group_links_are_independent(repos: Repositories) -> None:
    first = repos.group_links.link(
        telegram_chat_id=TG_GROUP, bale_chat_id=BALE_GROUP, now=FIXED_NOW
    )
    second = repos.group_links.link(
        telegram_chat_id=TG_GROUP_2, bale_chat_id=BALE_GROUP_2, now=FIXED_NOW
    )

    assert first.status is GroupLinkStatus.ACTIVE
    assert second.status is GroupLinkStatus.ACTIVE
    assert first.id != second.id
    assert repos.group_links.get_active_for_source(Platform.TELEGRAM, TG_GROUP) == first
    assert repos.group_links.get_active_for_source(Platform.BALE, BALE_GROUP_2) == second
    assert repos.group_links.list_active() == (first, second)
    assert (
        repos.group_links.seed_if_empty(
            telegram_chat_id=TG_GROUP, bale_chat_id=BALE_GROUP, now=FIXED_NOW
        )
        is None
    )


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

async def test_owner_opens_the_inline_group_menu(
    group_bridge: Bridge, repos: Repositories, senders
) -> None:
    await group_bridge.handle_message(
        group_message(
            text="/groupsync",
            sender_id=9001,
            chat_title="گروه تست تلگرام",
        )
    )

    call = senders[Platform.TELEGRAM].sent[-1]
    assert "سینک گروه" in call.text
    assert any(
        button.callback_data == "groupsync:pick"
        for row in call.buttons
        for button in row
    )
    assert (
        repos.group_invites.find_pending_for_source(
            Platform.TELEGRAM, TG_GROUP, now=FIXED_NOW
        )
        is None
    )


async def test_non_owner_cannot_create_a_group_invite(
    group_bridge: Bridge, repos: Repositories, senders
) -> None:
    await group_bridge.handle_message(
        group_message(text="/groupsync", sender_id=7001)
    )

    assert (
        repos.group_invites.find_pending_for_source(
            Platform.TELEGRAM, TG_GROUP, now=FIXED_NOW
        )
        is None
    )
    assert any("مالک" in text for text in senders[Platform.TELEGRAM].texts_to(TG_GROUP))


async def test_owner_completes_link_in_the_opposite_group(
    group_bridge: Bridge, repos: Repositories, senders
) -> None:
    token = new_group_token()
    repos.group_invites.create(
        token=token,
        source_platform=Platform.TELEGRAM,
        source_chat_id=TG_GROUP,
        created_by=9001,
        created_at=FIXED_NOW,
        expires_at=FIXED_NOW + timedelta(minutes=15),
    )
    invite = repos.group_invites.find_pending_for_source(
        Platform.TELEGRAM, TG_GROUP, now=FIXED_NOW
    )
    assert invite is not None

    await group_bridge.handle_message(
        group_message(
            platform=Platform.BALE,
            chat_id=BALE_GROUP_2,
            message_id=101,
            text=f"/groupsync {invite.token}",
            sender_id=9002,
            sender_name="مالک",
        )
    )

    linked = repos.group_links.get_active_for_source(Platform.BALE, BALE_GROUP_2)
    assert linked is not None
    assert linked.telegram_chat_id == TG_GROUP
    assert linked.bale_chat_id == BALE_GROUP_2
    assert (
        repos.group_invites.find_pending_for_source(
            Platform.TELEGRAM, TG_GROUP, now=FIXED_NOW
        )
        is None
    )
    assert any("فعال" in text for text in senders[Platform.BALE].texts_to(BALE_GROUP_2))


async def test_group_invite_cannot_be_used_twice(
    group_bridge: Bridge, repos: Repositories, senders
) -> None:
    token = new_group_token()
    repos.group_invites.create(
        token=token,
        source_platform=Platform.TELEGRAM,
        source_chat_id=TG_GROUP,
        created_by=9001,
        created_at=FIXED_NOW,
        expires_at=FIXED_NOW + timedelta(minutes=15),
    )
    invite = repos.group_invites.find_pending_for_source(
        Platform.TELEGRAM, TG_GROUP, now=FIXED_NOW
    )
    assert invite is not None
    await group_bridge.handle_message(
        group_message(
            platform=Platform.BALE,
            chat_id=BALE_GROUP_2,
            message_id=102,
            text=f"/groupsync {invite.token}",
            sender_id=9002,
        )
    )
    await group_bridge.handle_message(
        group_message(
            platform=Platform.BALE,
            chat_id=4497595492,
            message_id=103,
            text=f"/groupsync {invite.token}",
            sender_id=9002,
        )
    )

    assert (
        repos.group_links.get_active_for_source(Platform.BALE, 4497595492) is None
    )
    assert any(
        "نامعتبر" in text for text in senders[Platform.BALE].texts_to(4497595492)
    )


async def test_groupsync_off_deactivates_and_cancels_pending(
    group_bridge: Bridge, repos: Repositories, senders, link
) -> None:
    await group_bridge.handle_message(
        group_message(message_id=110, text="پیام در صف")
    )
    assert repos.group_outbox.list_for_link(link.id)

    await group_bridge.handle_message(
        group_message(
            message_id=111, text="/groupsync off", sender_id=9001
        )
    )

    assert repos.group_links.get_active_for_source(Platform.TELEGRAM, TG_GROUP) is None
    assert repos.group_links.get(link.id).status is GroupLinkStatus.INACTIVE
    assert repos.group_outbox.list_for_link(link.id) == ()
    assert any("قطع" in text for text in senders[Platform.TELEGRAM].texts_to(TG_GROUP))


async def test_groupsync_list_shows_all_active_links(
    group_bridge: Bridge, repos: Repositories, senders, link
) -> None:
    repos.group_links.link(
        telegram_chat_id=TG_GROUP_2,
        bale_chat_id=BALE_GROUP_2,
        now=FIXED_NOW,
    )

    await group_bridge.handle_message(
        group_message(message_id=120, text="/groupsync list", sender_id=9001)
    )

    reply = senders[Platform.TELEGRAM].texts_to(TG_GROUP)[-1]
    assert str(TG_GROUP) in reply
    assert str(BALE_GROUP) in reply
    assert str(TG_GROUP_2) in reply
    assert str(BALE_GROUP_2) in reply
    assert repos.group_outbox.list_for_link(link.id) == ()


async def test_each_pair_routes_to_its_own_target(group_bridge: Bridge, repos: Repositories, link) -> None:
    second = repos.group_links.link(
        telegram_chat_id=TG_GROUP_2,
        bale_chat_id=BALE_GROUP_2,
        now=FIXED_NOW,
    )
    await group_bridge.handle_message(
        group_message(chat_id=TG_GROUP_2, message_id=130, text="دومی")
    )

    first_entries = repos.group_outbox.list_for_link(link.id)
    second_entries = repos.group_outbox.list_for_link(second.id)
    assert first_entries == ()
    assert len(second_entries) == 1
    assert second_entries[0].payload["target_chat_id"] == BALE_GROUP_2

async def test_bale_owner_can_start_a_link(
    group_bridge: Bridge, repos: Repositories, senders
) -> None:
    token = new_group_token()
    repos.group_invites.create(
        token=token,
        source_platform=Platform.BALE,
        source_chat_id=BALE_GROUP_2,
        created_by=9002,
        created_at=FIXED_NOW,
        expires_at=FIXED_NOW + timedelta(minutes=15),
    )
    invite = repos.group_invites.find_pending_for_source(
        Platform.BALE, BALE_GROUP_2, now=FIXED_NOW
    )
    assert invite is not None

    await group_bridge.handle_message(
        group_message(
            chat_id=TG_GROUP_2,
            message_id=141,
            text=f"/groupsync {invite.token}",
            sender_id=9001,
        )
    )

    linked = repos.group_links.get_active_for_source(Platform.TELEGRAM, TG_GROUP_2)
    assert linked is not None
    assert linked.telegram_chat_id == TG_GROUP_2
    assert linked.bale_chat_id == BALE_GROUP_2
    assert any("فعال" in text for text in senders[Platform.TELEGRAM].texts_to(TG_GROUP_2))


async def test_group_invite_must_be_used_on_the_opposite_platform(
    group_bridge: Bridge, repos: Repositories, senders
) -> None:
    token = new_group_token()
    repos.group_invites.create(
        token=token,
        source_platform=Platform.TELEGRAM,
        source_chat_id=TG_GROUP,
        created_by=9001,
        created_at=FIXED_NOW,
        expires_at=FIXED_NOW + timedelta(minutes=15),
    )
    invite = repos.group_invites.find_pending_for_source(
        Platform.TELEGRAM, TG_GROUP, now=FIXED_NOW
    )
    assert invite is not None

    await group_bridge.handle_message(
        group_message(
            chat_id=TG_GROUP_2,
            message_id=150,
            text=f"/groupsync {invite.token}",
            sender_id=9001,
        )
    )

    assert repos.group_links.get_active_for_source(Platform.TELEGRAM, TG_GROUP_2) is None
    assert any("بله" in text for text in senders[Platform.TELEGRAM].texts_to(TG_GROUP_2))


async def test_groupsync_in_a_linked_group_reports_existing_link(
    group_bridge: Bridge, repos: Repositories, senders, link
) -> None:
    await group_bridge.handle_message(
        group_message(message_id=160, text="/groupsync", sender_id=9001)
    )

    assert (
        repos.group_invites.find_pending_for_source(
            Platform.TELEGRAM, TG_GROUP, now=FIXED_NOW
        )
        is None
    )
    assert any("وصل" in text for text in senders[Platform.TELEGRAM].texts_to(TG_GROUP))

async def test_inline_buttons_list_targets_and_create_link(
    group_bridge: Bridge, repos: Repositories, senders
) -> None:
    repos.known_groups.upsert(
        platform=Platform.TELEGRAM,
        chat_id=TG_GROUP,
        title="گروه تلگرام",
        discovered_by=9001,
        now=FIXED_NOW,
    )
    repos.known_groups.upsert(
        platform=Platform.BALE,
        chat_id=BALE_GROUP,
        title="گروه بله",
        discovered_by=9002,
        now=FIXED_NOW,
    )
    await group_bridge.handle_message(
        group_message(text="/groupsync", sender_id=9001, chat_title="گروه تلگرام")
    )

    await group_bridge.handle_message(
        group_message(
            text=None,
            message_id=501,
            callback_data="groupsync:pick",
            callback_id="cb-pick",
            sender_id=9001,
            chat_title="گروه تلگرام",
        )
    )
    pick = senders[Platform.TELEGRAM].edited[-1]
    assert "گروه بله" in pick.text
    assert any(
        button.callback_data == f"groupsync:link:{BALE_GROUP}"
        for row in pick.buttons
        for button in row
    )

    await group_bridge.handle_message(
        group_message(
            text=None,
            message_id=501,
            callback_data=f"groupsync:link:{BALE_GROUP}",
            callback_id="cb-link",
            sender_id=9001,
            chat_title="گروه تلگرام",
        )
    )

    linked = repos.group_links.get_active_for_source(Platform.TELEGRAM, TG_GROUP)
    assert linked is not None
    assert linked.bale_chat_id == BALE_GROUP
    assert "فعال شد" in senders[Platform.TELEGRAM].edited[-1].text


async def test_inline_off_button_disconnects(
    group_bridge: Bridge, repos: Repositories, senders, link
) -> None:
    await group_bridge.handle_message(
        group_message(
            text=None,
            message_id=502,
            callback_data="groupsync:off",
            callback_id="cb-off",
            sender_id=9001,
        )
    )

    assert repos.group_links.get_active_for_source(Platform.TELEGRAM, TG_GROUP) is None
    assert any("قطع" in edit.text for edit in senders[Platform.TELEGRAM].edited)


async def test_non_owner_button_does_not_change_links(
    group_bridge: Bridge, repos: Repositories
) -> None:
    await group_bridge.handle_message(
        group_message(
            text=None,
            message_id=503,
            callback_data="groupsync:pick",
            callback_id="cb-non-owner",
            sender_id=7001,
        )
    )

    assert repos.group_links.list_active() == ()
