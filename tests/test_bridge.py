"""Tests for :mod:`core.bridge` — the orchestrator, fully offline."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pytest

from core.bridge import Bridge
from core.commands import Action, CommandRouter, SendMessageAction
from core.errors import DeliveryError, TransportError, UserLimitReachedError
from core.models import OutboxStatus, Platform, UnifiedMessage
from core.pairing import PairingService, PairingStatus
from core.users import User, UserRole, UserService
from storage.db import Repositories
from tests.conftest import FIXED_NOW, RecordingSender

OWNER_TELEGRAM = 9001
OWNER_BALE = 9002


def inbound(
    platform: Platform = Platform.TELEGRAM,
    sender_id: int = 5001,
    text: str | None = "/start",
    *,
    chat_id: int | None = None,
    message_id: int = 1,
) -> UnifiedMessage:
    """Inbound message helper (the shared fixture stays available too)."""
    return UnifiedMessage(
        source=platform,
        sender_id=sender_id,
        chat_id=chat_id if chat_id is not None else sender_id,
        message_id=message_id,
        text=text,
        timestamp=FIXED_NOW,
    )


async def register_users(
    bridge: Bridge, users: UserService, *specs: tuple[Platform, int]
) -> dict[int, User]:
    """Register users through the bridge and return them keyed by platform id."""
    registered: dict[int, User] = {}
    for index, (platform, platform_user_id) in enumerate(specs, start=1):
        await bridge.handle_message(
            inbound(platform, platform_user_id, "/start", message_id=index)
        )
        user = users.get_by_platform(platform, platform_user_id)
        assert user is not None
        registered[platform_user_id] = user
    return registered


def only_request(repos: Repositories) -> Any:
    """The single stored pairing request (tests always create exactly one)."""
    rows = repos.db.connection.execute("SELECT * FROM pair_requests").fetchall()
    assert len(rows) == 1, f"expected exactly one request, found {len(rows)}"
    return rows[0]


def callback(
    *,
    sender_id: int = 5001,
    data: str = "menu:main",
    message_id: int = 77,
    platform: Platform = Platform.TELEGRAM,
) -> UnifiedMessage:
    """Inbound inline-button press helper."""
    return UnifiedMessage(
        source=platform,
        sender_id=sender_id,
        chat_id=sender_id,
        message_id=message_id,
        timestamp=FIXED_NOW,
        callback_data=data,
        callback_id="callback-1",
    )


# ------------------------------------------------------------- registration


async def test_callback_edits_the_menu_and_answers_the_query(
    bridge: Bridge, users: UserService, senders
) -> None:
    await bridge.handle_message(inbound(sender_id=5001, text="/start"))
    telegram = senders[Platform.TELEGRAM]

    await bridge.handle_message(callback(sender_id=5001, data="menu:status"))

    assert telegram.answered_callbacks == ["callback-1"]
    assert telegram.edited, "the menu must be updated in place"
    assert telegram.edited[-1].message_id == 77
    assert "وضعیت" in telegram.edited[-1].text
    assert "menu:main" in [
        button.callback_data for row in telegram.edited[-1].buttons for button in row
    ]


async def test_start_registers_user_and_replies_with_internal_id(
    bridge: Bridge, users: UserService, senders, incoming: Callable[..., UnifiedMessage]
) -> None:
    await bridge.handle_message(incoming(sender_id=5001, text="/start"))

    user = users.get_by_platform(Platform.TELEGRAM, 5001)
    assert user is not None
    assert user.role is UserRole.USER
    assert user.last_chat_id == 5001

    telegram = senders[Platform.TELEGRAM]
    assert len(telegram.sent) == 1
    assert telegram.sent[0].chat_id == 5001
    assert str(user.public_id) in telegram.sent[0].text


async def test_configured_owner_is_registered_as_owner(
    bridge: Bridge, users: UserService
) -> None:
    await bridge.handle_message(inbound(sender_id=OWNER_TELEGRAM, text="/help"))

    owner = users.get_by_platform(Platform.TELEGRAM, OWNER_TELEGRAM)
    assert owner is not None and owner.role is UserRole.OWNER


async def test_repeated_messages_reuse_one_user_row(
    bridge: Bridge, users: UserService, repos: Repositories
) -> None:
    await bridge.handle_message(inbound(sender_id=5001, text="/start"))
    await bridge.handle_message(inbound(sender_id=5001, text="/whoami", message_id=2))

    assert users.count() == 1
    assert repos.users.count() == 1


async def test_telegram_and_bale_identities_are_distinct_rows(
    bridge: Bridge, users: UserService, repos: Repositories
) -> None:
    await bridge.handle_message(inbound(Platform.TELEGRAM, 555, "/start"))
    await bridge.handle_message(inbound(Platform.BALE, 555, "/start", message_id=2))

    telegram = users.get_by_platform(Platform.TELEGRAM, 555)
    bale = users.get_by_platform(Platform.BALE, 555)
    assert telegram is not None and bale is not None
    assert telegram.id != bale.id, "the same numeric id on two platforms is two users"
    assert (telegram.platform, bale.platform) == (Platform.TELEGRAM, Platform.BALE)
    assert repos.users.count() == 2

    # The mapping key stays (platform, platform_user_id) and is stable.
    assert users.get_by_platform(Platform.TELEGRAM, 555).id == telegram.id


async def test_chat_id_is_remembered_for_delivery(
    bridge: Bridge, users: UserService, senders
) -> None:
    # Bale never documents chat.id == user.id, so the observed chat id wins.
    await bridge.handle_message(
        inbound(Platform.BALE, 7001, "/whoami", chat_id=7777)
    )

    assert senders[Platform.BALE].sent[0].chat_id == 7777
    user = users.get_by_platform(Platform.BALE, 7001)
    assert user is not None and user.last_chat_id == 7777


async def test_registration_limit_is_reported_to_the_sender(
    repos: Repositories, senders
) -> None:
    users = UserService(repos.users, repos.db, max_users=1)
    pairing = PairingService(repos.users, repos.pairs, repos.requests, repos.db)
    bridge = Bridge(
        users=users,
        pairing=pairing,
        router=CommandRouter(users, pairing),
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        owner_platform_ids={Platform.TELEGRAM: OWNER_TELEGRAM},
    )

    await bridge.handle_message(inbound(sender_id=1, text="/start"))
    await bridge.handle_message(inbound(sender_id=2, text="/start", message_id=2))

    assert users.count() == 1
    assert senders[Platform.TELEGRAM].texts[-1] == UserLimitReachedError().message


# ------------------------------------------------------------------ commands


async def test_pair_command_notifies_both_sides(
    bridge: Bridge, users: UserService, senders, repos: Repositories
) -> None:
    registered = await register_users(
        bridge, users, (Platform.TELEGRAM, 5001), (Platform.BALE, 7001)
    )
    alice, bob = registered[5001], registered[7001]

    await bridge.handle_message(inbound(text=f"/pair {bob.public_id}", sender_id=5001))

    assert any(
        "درخواست جفت‌سازی" in text
        for text in senders[Platform.TELEGRAM].texts_to(alice.platform_user_id)
    )
    assert any("/accept" in text for text in senders[Platform.BALE].texts_to(7001))
    assert only_request(repos)["status"] == PairingStatus.PENDING.value


async def test_cross_platform_pairing_and_notifications(
    bridge: Bridge, users: UserService, repos: Repositories, pairing: PairingService, senders
) -> None:
    registered = await register_users(
        bridge, users, (Platform.TELEGRAM, 5001), (Platform.BALE, 7001)
    )
    alice, bob = registered[5001], registered[7001]
    await bridge.handle_message(inbound(text=f"/pair {bob.public_id}", sender_id=5001))
    request = repos.requests.list_pending_incoming(bob.id)[0]

    await bridge.handle_message(
        inbound(Platform.BALE, 7001, f"/accept {request.id}")
    )

    pair = pairing.active_pair(alice.id)
    assert pair is not None and pair.includes(bob.id)
    assert any("فعال شد" in text for text in senders[Platform.BALE].texts_to(7001))
    assert any("پذیرفته شد" in text for text in senders[Platform.TELEGRAM].texts_to(5001))


# -------------------------------------------------------- delivery failures


async def test_blocked_target_cancels_request_and_warns_requester_and_owner(
    bridge: Bridge, users: UserService, repos: Repositories, senders: dict[Platform, RecordingSender]
) -> None:
    registered = await register_users(
        bridge,
        users,
        (Platform.TELEGRAM, 5001),
        (Platform.BALE, 7001),
        (Platform.BALE, OWNER_BALE),
    )
    alice, bob = registered[5001], registered[7001]
    senders[Platform.BALE].failures[7001] = DeliveryError(
        "Forbidden: bot was blocked by the user", platform="bale", status_code=403
    )

    await bridge.handle_message(inbound(text=f"/pair {bob.public_id}", sender_id=5001))

    assert repos.requests.list_pending_outgoing(alice.id) == ()
    assert only_request(repos)["status"] == PairingStatus.CANCELLED.value
    assert any("لغو شد" in text for text in senders[Platform.TELEGRAM].texts_to(5001))
    assert any("هشدار" in text for text in senders[Platform.BALE].texts_to(OWNER_BALE))


async def test_temporary_delivery_failure_keeps_the_request_pending(
    bridge: Bridge, users: UserService, repos: Repositories, senders: dict[Platform, RecordingSender]
) -> None:
    registered = await register_users(
        bridge, users, (Platform.TELEGRAM, 5001), (Platform.BALE, 7001)
    )
    bob = registered[7001]
    senders[Platform.BALE].failures[7001] = TransportError("gateway timeout")

    await bridge.handle_message(inbound(text=f"/pair {bob.public_id}", sender_id=5001))

    assert only_request(repos)["status"] == PairingStatus.PENDING.value
    assert not any("لغو شد" in text for text in senders[Platform.TELEGRAM].texts_to(5001))


async def test_owner_warning_is_skipped_when_the_owner_is_not_registered(
    bridge: Bridge,
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    caplog: pytest.LogCaptureFixture,
) -> None:
    registered = await register_users(
        bridge, users, (Platform.TELEGRAM, 5001), (Platform.BALE, 7001)
    )
    bob = registered[7001]
    senders[Platform.BALE].failures[7001] = DeliveryError("bot was blocked by the user")
    bale_deliveries_before = len(senders[Platform.BALE].sent)

    with caplog.at_level(logging.INFO, logger="bridge.core"):
        await bridge.handle_message(inbound(text=f"/pair {bob.public_id}", sender_id=5001))

    assert only_request(repos)["status"] == PairingStatus.CANCELLED.value
    assert "owner is not registered" in caplog.text
    assert len(senders[Platform.BALE].sent) == bale_deliveries_before, (
        "no owner warning can be delivered on Bale"
    )


async def test_failure_without_a_related_request_is_only_logged(
    bridge: Bridge,
    users: UserService,
    senders: dict[Platform, RecordingSender],
    repos: Repositories,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await register_users(bridge, users, (Platform.TELEGRAM, 5001))
    senders[Platform.TELEGRAM].failures[5001] = DeliveryError("blocked")

    with caplog.at_level(logging.WARNING, logger="bridge.core"):
        await bridge.handle_message(inbound(sender_id=5001, text="/status"))

    assert "delivery failed" in caplog.text
    assert repos.pairs.count_active() == 0


async def test_unsupported_action_type_is_logged(bridge_deps: dict[str, Any], caplog) -> None:
    class Weird(Action):
        """An action the bridge does not know how to deliver."""

    class StubRouter:
        def handle(self, actor_id: int, text: str | None) -> tuple[Action, ...]:
            return (Weird(),)

    bridge = Bridge(
        users=bridge_deps["users"],
        pairing=bridge_deps["pairing"],
        router=StubRouter(),  # type: ignore[arg-type]
        messages=bridge_deps["messages"],  # type: ignore[arg-type]
        outbox=bridge_deps["outbox"],  # type: ignore[arg-type]
        tx=bridge_deps["tx"],  # type: ignore[arg-type]
        senders=bridge_deps["senders"],
        owner_platform_ids={Platform.TELEGRAM: OWNER_TELEGRAM},
    )

    with caplog.at_level(logging.ERROR, logger="bridge.core"):
        await bridge.handle_message(inbound(sender_id=5001, text="/start"))

    assert "unsupported action type" in caplog.text


async def test_action_for_unknown_recipient_is_logged(
    bridge_deps: dict[str, Any], caplog
) -> None:
    class StubRouter:
        def handle(self, actor_id: int, text: str | None) -> tuple[Action, ...]:
            return (SendMessageAction(recipient_id=999_999, text="hi"),)

    bridge = Bridge(
        users=bridge_deps["users"],
        pairing=bridge_deps["pairing"],
        router=StubRouter(),  # type: ignore[arg-type]
        messages=bridge_deps["messages"],  # type: ignore[arg-type]
        outbox=bridge_deps["outbox"],  # type: ignore[arg-type]
        tx=bridge_deps["tx"],  # type: ignore[arg-type]
        senders=bridge_deps["senders"],
        owner_platform_ids={Platform.TELEGRAM: OWNER_TELEGRAM},
    )

    with caplog.at_level(logging.WARNING, logger="bridge.core"):
        await bridge.handle_message(inbound(sender_id=5001, text="/start"))

    assert "unknown recipient_id" in caplog.text


async def test_missing_sender_for_a_platform_is_logged(
    users: UserService, pairing: PairingService, repos: Repositories, caplog
) -> None:
    bridge = Bridge(
        users=users,
        pairing=pairing,
        router=CommandRouter(users, pairing),
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders={},
        owner_platform_ids={},
    )

    with caplog.at_level(logging.WARNING, logger="bridge.core"):
        await bridge.handle_message(inbound(sender_id=5001, text="/start"))

    assert users.count() == 1, "registration still happens without a sender"
    assert "no sender configured" in caplog.text


# -------------------------------------------------------------- plain messages


async def test_plain_text_without_a_pair_gets_a_short_guide(
    bridge: Bridge, users: UserService, senders, repos: Repositories
) -> None:
    await register_users(bridge, users, (Platform.TELEGRAM, 5001))

    await bridge.handle_message(inbound(sender_id=5001, text="سلام", message_id=9))

    assert any("/pair" in text for text in senders[Platform.TELEGRAM].texts_to(5001))
    assert repos.pairs.count_active() == 0


async def test_plain_text_with_an_active_pair_is_queued_for_relay(
    bridge: Bridge, users: UserService, repos: Repositories, pairing: PairingService, senders
) -> None:
    registered = await register_users(
        bridge, users, (Platform.TELEGRAM, 5001), (Platform.BALE, 7001)
    )
    alice, bob = registered[5001], registered[7001]
    await bridge.handle_message(inbound(text=f"/pair {bob.public_id}", sender_id=5001))
    request = repos.requests.list_pending_incoming(bob.id)[0]
    await bridge.handle_message(inbound(Platform.BALE, 7001, f"/accept {request.id}"))

    before = (
        len(senders[Platform.TELEGRAM].sent),
        len(senders[Platform.BALE].sent),
    )

    await bridge.handle_message(inbound(sender_id=5001, text="یک پیام عادی", message_id=42))

    # The relay is recorded + queued, never sent inline.
    entries = repos.outbox.list_for_pair(pairing.active_pair(alice.id).id)  # type: ignore[union-attr]
    assert len(entries) == 1 and entries[0].status == OutboxStatus.PENDING
    assert repos.messages.list_for_pair(entries[0].pair_id) != ()
    assert (
        len(senders[Platform.TELEGRAM].sent),
        len(senders[Platform.BALE].sent),
    ) == before, "the actual send belongs to the outbox worker, not the bridge"
    assert pairing.active_pair(alice.id) is not None


async def test_message_without_text_is_ignored(
    bridge: Bridge, users: UserService, senders
) -> None:
    await bridge.handle_message(inbound(sender_id=5001, text=None))

    assert users.get_by_platform(Platform.TELEGRAM, 5001) is not None
    assert senders[Platform.TELEGRAM].sent == []


# ------------------------------------------------------------- logging hygiene


async def test_info_logs_never_contain_message_content(
    bridge: Bridge, users: UserService, repos: Repositories, caplog
) -> None:
    registered = await register_users(
        bridge, users, (Platform.TELEGRAM, 5001), (Platform.BALE, 7001)
    )
    bob = registered[7001]
    secret_text = "این متن نباید در لاگ بیاید"

    with caplog.at_level(logging.INFO, logger="bridge.core"):
        await bridge.handle_message(inbound(text=f"/pair {bob.public_id}", sender_id=5001))
        await bridge.handle_message(
            inbound(sender_id=5001, text=secret_text, message_id=5)
        )

    assert "command=pair" in caplog.text, "the command name is logged"
    assert secret_text not in caplog.text
    assert f"/pair {bob.public_id}" not in caplog.text
    assert repos.requests.count_pending() == 1
