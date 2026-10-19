"""Relay tests: text relaying between the two members of an active pair.

Everything runs offline through :class:`core.bridge.Bridge` and
:class:`core.delivery.OutboxWorker` with recording senders; "delivery" means a
recorded call, never a network request. Sender recordings are cleared after
pairing so every assertion sees only relay traffic.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

import pytest

from core.bridge import Bridge
from core.commands import CommandRouter
from core.delivery import RELAY_PAYLOAD_KIND, OutboxWorker
from core.models import MessageRef, OutboxEntry, Platform, UnifiedMessage
from core.pairing import Pair, PairingService, PairingStatus
from core.users import User, UserService, UserStatus
from storage.db import Database, OutboxRepository, Repositories
from tests.conftest import FIXED_NOW, RecordingSender

TELEGRAM_USER = 5001
BALE_USER = 7001


class ExplodingOutbox(OutboxRepository):
    """Outbox whose first enqueue fails, simulating a write error mid-relay."""

    def __init__(self, db: Database) -> None:
        super().__init__(db)
        self.armed = True

    def enqueue(
        self,
        *,
        pair_id: int,
        payload: Mapping[str, Any],
        next_attempt_at: datetime,
        created_at: datetime,
    ) -> OutboxEntry:
        if self.armed:
            self.armed = False
            raise RuntimeError("simulated outbox write failure")
        return super().enqueue(
            pair_id=pair_id,
            payload=payload,
            next_attempt_at=next_attempt_at,
            created_at=created_at,
        )


def inbound_msg(
    platform: Platform,
    sender_id: int,
    text: str,
    *,
    message_id: int,
    chat_id: int | None = None,
    entities: tuple[dict, ...] = (),
    reply_to: MessageRef | None = None,
) -> UnifiedMessage:
    """An inbound message (plain text plus optional entities/reply)."""
    return UnifiedMessage(
        source=platform,
        sender_id=sender_id,
        chat_id=chat_id if chat_id is not None else sender_id,
        message_id=message_id,
        text=text,
        entities=entities,  # type: ignore[arg-type]
        reply_to=reply_to,
        timestamp=FIXED_NOW,
    )


async def make_pair(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    *,
    telegram_id: int = TELEGRAM_USER,
    bale_id: int = BALE_USER,
) -> tuple[User, User, Pair]:
    """Register a Telegram and a Bale user via the bridge and pair them."""
    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, telegram_id, "/start", message_id=1)
    )
    await bridge.handle_message(
        inbound_msg(Platform.BALE, bale_id, "/start", message_id=1)
    )
    alice = users.get_by_platform(Platform.TELEGRAM, telegram_id)
    bob = users.get_by_platform(Platform.BALE, bale_id)
    assert alice is not None and bob is not None

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, telegram_id, f"/pair {bob.public_id}", message_id=2)
    )
    request = repos.requests.list_pending_incoming(bob.id)[0]
    await bridge.handle_message(
        inbound_msg(Platform.BALE, bale_id, f"/accept {request.id}", message_id=2)
    )
    pair = pairing.active_pair(alice.id)
    assert pair is not None
    return alice, bob, pair


def clear_senders(senders: dict[Platform, RecordingSender]) -> None:
    """Drop every recorded system/command delivery made before the relays."""
    for sender in senders.values():
        sender.sent.clear()


def pending_entries(repos: Repositories) -> tuple:
    return repos.outbox.list_due(FIXED_NOW, limit=100)


@pytest.fixture
def worker(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> OutboxWorker:
    """Delivery worker wired to the shared repos, senders and fixed clock."""
    return OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        retry_delay=timedelta(seconds=10),
    )


# ------------------------------------------------------------------ happy path


async def test_relay_reaches_only_the_partner(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "سلام از تلگرام", message_id=10)
    )

    entries = repos.outbox.list_for_pair(pair.id)
    assert len(entries) == 1
    payload = entries[0].payload
    assert payload["kind"] == RELAY_PAYLOAD_KIND
    assert payload["target_user_id"] == bob.id
    assert payload["target_chat_id"] == BALE_USER
    assert payload["text"] == "سلام از تلگرام"
    assert payload["reply_to_message_id"] is None

    record = repos.messages.get_by_source_for_pair(
        pair.id, Platform.TELEGRAM, TELEGRAM_USER, 10
    )
    assert record is not None, "the relay is recorded before delivery"

    stats = await worker.process_due()
    assert stats.delivered == 1
    assert senders[Platform.BALE].texts_to(BALE_USER) == ["سلام از تلگرام"]
    assert senders[Platform.TELEGRAM].sent == [], "the sender never gets it back"

    delivered = repos.messages.get_by_source_for_pair(
        pair.id, Platform.TELEGRAM, TELEGRAM_USER, 10
    )
    assert delivered is not None and delivered.target_message_id is not None


async def test_relay_is_bidirectional(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "از تلگرام", message_id=10)
    )
    await worker.process_due()
    await bridge.handle_message(
        inbound_msg(Platform.BALE, BALE_USER, "از بله", message_id=20)
    )
    await worker.process_due()

    assert senders[Platform.BALE].texts_to(BALE_USER) == ["از تلگرام"]
    assert senders[Platform.TELEGRAM].texts_to(TELEGRAM_USER) == ["از بله"]
    assert len(repos.messages.list_for_pair(pair.id)) == 2


# ------------------------------------------------------------------ isolation


async def test_two_pairs_never_exchange_each_others_messages(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
) -> None:
    _, _, pair_a = await make_pair(bridge, users, pairing, repos)  # tg5001 ↔ bale7001
    _, _, pair_b = await make_pair(
        bridge, users, pairing, repos, telegram_id=5002, bale_id=7002
    )
    clear_senders(senders)

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, 5001, "پیام جفت A", message_id=10)
    )

    assert [entry.pair_id for entry in pending_entries(repos)] == [pair_a.id]
    await worker.process_due()

    # Pair B's members never receive anything, and no record leaks into pair B.
    assert senders[Platform.BALE].texts_to(7002) == []
    assert senders[Platform.TELEGRAM].texts_to(5002) == []
    assert repos.messages.list_for_pair(pair_b.id) == ()

    # Pair B's own traffic stays confined to pair B.
    await bridge.handle_message(
        inbound_msg(Platform.BALE, 7002, "پیام جفت B", message_id=10)
    )
    assert [entry.pair_id for entry in pending_entries(repos)] == [pair_b.id]
    await worker.process_due()

    # Each partner received exactly their own pair's message, nothing else.
    assert senders[Platform.BALE].texts_to(7002) == []
    assert senders[Platform.TELEGRAM].texts_to(5001) == []
    assert senders[Platform.TELEGRAM].texts_to(5002) == ["پیام جفت B"]
    assert senders[Platform.BALE].texts_to(7001) == ["پیام جفت A"], (
        "pair B's traffic never reached pair A's partner"
    )


# ------------------------------------------------- loops, duplicates, idempotency


async def test_duplicate_delivery_of_one_message_is_relayed_once(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)

    message = inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "فقط یک‌بار", message_id=10)
    await bridge.handle_message(message)
    await bridge.handle_message(message)  # same platform/chat/message id again
    with caplog.at_level(logging.INFO, logger="bridge.core"):
        await bridge.handle_message(message)  # and a third time, now logged

    assert len(repos.outbox.list_for_pair(pair.id)) == 1
    assert len(repos.messages.list_for_pair(pair.id)) == 1
    assert "relay skipped" in caplog.text
    assert "فقط یک‌بار" not in caplog.text


async def test_unique_index_prevents_relaying_into_another_pair(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _, _, pair_a = await make_pair(bridge, users, pairing, repos)
    _, _, pair_b = await make_pair(
        bridge, users, pairing, repos, telegram_id=5002, bale_id=7002
    )

    # The original message is recorded under pair A first.
    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, 5001, "پیام اصلی", message_id=10)
    )
    assert len(repos.messages.list_for_pair(pair_a.id)) == 1

    # The same platform message key arriving under a different pair is a
    # conflict: the unique index backs the first record and the bridge refuses
    # to relay it for the second pair.
    await bridge.handle_message(
        inbound_msg(
            Platform.TELEGRAM,
            5002,
            "کلید تکراری",
            message_id=10,
            chat_id=TELEGRAM_USER,  # force the exact natural key of pair A
        )
    )

    assert repos.outbox.list_for_pair(pair_b.id) == ()
    assert len(repos.messages.list_for_pair(pair_a.id)) == 1
    assert "message_belongs_to_other_pair" in caplog.text


# ---------------------------------------------------------------- access control


async def test_banned_sender_is_not_relayed(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    repos.users.set_status(alice.id, UserStatus.BANNED)

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "پیام کاربر مسدود", message_id=10)
    )

    assert repos.outbox.list_for_pair(pair.id) == ()
    assert any(
        "مسدود" in text for text in senders[Platform.TELEGRAM].texts_to(TELEGRAM_USER)
    )


async def test_user_without_an_active_pair_is_not_relayed(
    bridge: Bridge,
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, 5001, "/start", message_id=1)
    )

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, 5001, "بدون جفت", message_id=2)
    )

    assert repos.outbox.list_due(FIXED_NOW) == ()
    assert any("/pair" in text for text in senders[Platform.TELEGRAM].texts_to(5001))


async def test_banned_partner_blocks_the_relay(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    repos.users.set_status(bob.id, UserStatus.BANNED)

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "به مقصد مسدود", message_id=10)
    )

    assert repos.outbox.list_for_pair(pair.id) == ()
    assert any(
        "مسدود" in text for text in senders[Platform.TELEGRAM].texts_to(TELEGRAM_USER)
    ), "the sender is told the relay did not happen"


# ------------------------------------------------------------------------ replies


async def test_reply_is_anchored_to_the_original_in_the_destination(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "پیام اول", message_id=42)
    )
    await worker.process_due()
    delivered = repos.messages.get_by_source_for_pair(
        pair.id, Platform.TELEGRAM, TELEGRAM_USER, 42
    )
    assert delivered is not None and delivered.target_message_id is not None
    bale_side_id = delivered.target_message_id

    # Bob replies to the bot-delivered message in his Bale chat.
    await bridge.handle_message(
        inbound_msg(
            Platform.BALE,
            BALE_USER,
            "پاسخ",
            message_id=50,
            reply_to=MessageRef(
                platform=Platform.BALE, chat_id=BALE_USER, message_id=bale_side_id
            ),
        )
    )

    payload = repos.outbox.list_for_pair(pair.id)[-1].payload
    assert payload["reply_to_message_id"] == 42, (
        "the reply anchors to the original message id in the destination chat"
    )

    await worker.process_due()
    reply_calls = [
        call for call in senders[Platform.TELEGRAM].sent if call.text == "پاسخ"
    ]
    assert reply_calls and reply_calls[0].reply_to_message_id == 42


async def test_reply_anchor_works_in_the_other_direction(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    # Bob (Bale) starts, Alice (Telegram) replies to the delivered message.
    await bridge.handle_message(
        inbound_msg(Platform.BALE, BALE_USER, "پیام اول از بله", message_id=77)
    )
    await worker.process_due()
    delivered = repos.messages.get_by_source_for_pair(
        pair.id, Platform.BALE, BALE_USER, 77
    )
    assert delivered is not None and delivered.target_message_id is not None

    await bridge.handle_message(
        inbound_msg(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            "پاسخ از تلگرام",
            message_id=88,
            reply_to=MessageRef(
                platform=Platform.TELEGRAM,
                chat_id=TELEGRAM_USER,
                message_id=delivered.target_message_id,
            ),
        )
    )

    payload = repos.outbox.list_for_pair(pair.id)[-1].payload
    assert payload["reply_to_message_id"] == 77

    await worker.process_due()
    replies = [
        call for call in senders[Platform.BALE].sent if call.text == "پاسخ از تلگرام"
    ]
    assert replies and replies[0].reply_to_message_id == 77


async def test_reply_to_an_unmapped_message_has_no_anchor(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    worker: OutboxWorker,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)

    await bridge.handle_message(
        inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "پیام اول", message_id=42)
    )
    await worker.process_due()
    await bridge.handle_message(
        inbound_msg(
            Platform.BALE,
            BALE_USER,
            "ریپلای به ناشناخته",
            message_id=50,
            reply_to=MessageRef(
                platform=Platform.BALE, chat_id=BALE_USER, message_id=9999
            ),
        )
    )

    payload = repos.outbox.list_for_pair(pair.id)[-1].payload
    assert payload["reply_to_message_id"] is None


# ------------------------------------------------------------ entity conversion


async def test_entities_are_rendered_for_the_target_platform(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)

    await bridge.handle_message(
        inbound_msg(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            "متن مهم",
            message_id=10,
            entities=({"type": "bold", "offset": 4, "length": 3},),
        )
    )

    payload = repos.outbox.list_for_pair(pair.id)[-1].payload
    assert payload["text"] == "متن **مهم**", "Bale-bound relays carry Markdown"


async def test_unsupported_entities_fall_back_to_plain_text(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)

    await bridge.handle_message(
        inbound_msg(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            "منشن @user",
            message_id=10,
            entities=({"type": "mention", "offset": 5, "length": 5},),
        )
    )

    payload = repos.outbox.list_for_pair(pair.id)[-1].payload
    assert payload["text"] == "منشن @user"


async def test_markdown_significant_text_is_relayed_untouched(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)

    await bridge.handle_message(
        inbound_msg(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            "کد `x*y` را ببین",
            message_id=10,
            entities=({"type": "code", "offset": 0, "length": 2},),
        )
    )

    payload = repos.outbox.list_for_pair(pair.id)[-1].payload
    assert payload["text"] == "کد `x*y` را ببین"


async def test_bale_to_telegram_relay_stays_plain(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)

    await bridge.handle_message(
        inbound_msg(
            Platform.BALE,
            BALE_USER,
            "متن پررنگ",
            message_id=10,
            entities=({"type": "bold", "offset": 0, "length": 3},),
        )
    )

    payload = repos.outbox.list_for_pair(pair.id)[-1].payload
    assert payload["text"] == "متن پررنگ", "Telegram-bound relays are plain text"


# ------------------------------------------------------------ logging hygiene


async def test_relay_logs_never_contain_message_content(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await make_pair(bridge, users, pairing, repos)
    secret = "این متن محرمانه است"

    with caplog.at_level(logging.INFO, logger="bridge.core"):
        await bridge.handle_message(
            inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, secret, message_id=10)
        )

    assert "relay queued" in caplog.text
    assert secret not in caplog.text


async def test_a_failed_queue_write_leaves_no_record_and_can_be_retried(
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    clock,
) -> None:
    """The relay record and its outbox entry are written atomically.

    A queue write that fails must roll the record back: otherwise the dedup
    guard would see "already relayed" forever and the message would be lost.
    """
    alice = users.get_or_register(Platform.TELEGRAM, TELEGRAM_USER)
    bob = users.get_or_register(Platform.BALE, BALE_USER)
    pairing.accept(pairing.request(alice.id, bob.id).id, bob.id)

    outbox = ExplodingOutbox(repos.db)
    broken = Bridge(
        users=users,
        pairing=pairing,
        router=CommandRouter(users, pairing),
        messages=repos.messages,
        outbox=outbox,
        tx=repos.db,
        senders=senders,
        owner_platform_ids={},
        clock=clock,
    )

    with pytest.raises(RuntimeError):
        await broken.handle_message(
            inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "پیام", message_id=10)
        )
    assert repos.messages.find_by_source(Platform.TELEGRAM, TELEGRAM_USER, 10) is None

    # Nothing was recorded, so the same update still relays on the next attempt.
    await broken.handle_message(
        inbound_msg(Platform.TELEGRAM, TELEGRAM_USER, "پیام", message_id=10)
    )
    assert len(repos.outbox.list_due(FIXED_NOW)) == 1
    assert repos.messages.find_by_source(Platform.TELEGRAM, TELEGRAM_USER, 10) is not None


async def test_worker_logs_only_ids_and_statuses(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        retry_delay=timedelta(seconds=10),
    )
    alice = users.get_or_register(Platform.TELEGRAM, 4001)
    bob = users.get_or_register(Platform.BALE, 4002)
    pair = repos.pairs.create(
        user_a_id=min(alice.id, bob.id),
        user_b_id=max(alice.id, bob.id),
        status=PairingStatus.ACTIVE,
        created_at=FIXED_NOW,
    )
    repos.outbox.enqueue(
        pair_id=pair.id,
        payload={
            "kind": RELAY_PAYLOAD_KIND,
            "message_id": 1,
            "target_user_id": bob.id,
            "target_chat_id": 4002,
            "text": "متن محرمانه برای لاگ",
        },
        next_attempt_at=FIXED_NOW,
        created_at=FIXED_NOW,
    )

    with caplog.at_level(logging.INFO, logger="bridge.delivery"):
        await worker.process_due()

    assert "outbox delivered" in caplog.text
    assert "متن محرمانه برای لاگ" not in caplog.text
