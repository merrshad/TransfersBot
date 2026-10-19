"""Tests for :mod:`core.delivery` — the outbox delivery loop, fully offline."""

from __future__ import annotations

import logging
from datetime import timedelta

import pytest

from core.delivery import RELAY_PAYLOAD_KIND, OutboxRunStats, OutboxWorker
from core.errors import DeliveryError, TransportError
from core.models import OutboxStatus, Platform
from core.pairing import PairingStatus
from core.users import UserService
from storage.db import Repositories
from tests.conftest import FIXED_NOW, RecordingSender


@pytest.fixture
def pair_id(users: UserService, repos: Repositories) -> int:
    """A real active pair row backing every queued entry (FK target)."""
    alice = users.get_or_register(Platform.TELEGRAM, 1)
    bob = users.get_or_register(Platform.BALE, 2)
    pair = repos.pairs.create(
        user_a_id=min(alice.id, bob.id),
        user_b_id=max(alice.id, bob.id),
        status=PairingStatus.ACTIVE,
        created_at=FIXED_NOW,
    )
    return pair.id


@pytest.fixture
def worker(
    users: UserService, repos: Repositories, senders: dict[Platform, RecordingSender]
) -> OutboxWorker:
    """Worker with a fixed clock, a 10s retry delay and 3 attempts at most."""
    return OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        retry_delay=timedelta(seconds=10),
        max_attempts=3,
        random_source=lambda: 0.5,
    )


def relay_payload(
    *,
    target_user_id: int = 1,
    target_chat_id: int = 100,
    message_id: int = 1,
    text: str = "متن رله",
    reply_to: int | None = None,
    kind: str = RELAY_PAYLOAD_KIND,
) -> dict:
    """A well-formed relay payload (shape owned by the bridge)."""
    return {
        "kind": kind,
        "message_id": message_id,
        "target_user_id": target_user_id,
        "target_chat_id": target_chat_id,
        "text": text,
        "reply_to_message_id": reply_to,
    }


def enqueue(repos: Repositories, payload: dict, *, pair_id: int) -> int:
    entry = repos.outbox.enqueue(
        pair_id=pair_id,
        payload=payload,
        next_attempt_at=FIXED_NOW,
        created_at=FIXED_NOW,
    )
    return entry.id


# ------------------------------------------------------------------ happy path


async def test_delivers_pending_entry_and_marks_it_sent(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
) -> None:
    user = users.get_or_register(Platform.BALE, 100)
    entry_id = enqueue(repos, relay_payload(target_user_id=user.id), pair_id=pair_id)

    stats = await worker.process_due()

    assert stats == OutboxRunStats(delivered=1)
    assert len(senders[Platform.BALE].sent) == 1
    call = senders[Platform.BALE].sent[0]
    assert call.chat_id == 100
    assert call.text == "متن رله"
    assert call.reply_to_message_id is None
    assert repos.outbox.get_for_pair(pair_id, entry_id).status == OutboxStatus.SENT


async def test_delivery_records_the_target_message_id_on_the_source_record(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
) -> None:
    user = users.get_or_register(Platform.BALE, 100)
    record, _ = repos.messages.add(
        pair_id=pair_id,
        source_platform=Platform.TELEGRAM,
        source_chat_id=7,
        source_message_id=42,
        created_at=FIXED_NOW,
    )
    enqueue(
        repos,
        relay_payload(target_user_id=user.id, message_id=record.id),
        pair_id=pair_id,
    )

    await worker.process_due()

    updated = repos.messages.get_for_pair(pair_id, record.id)
    assert updated is not None
    assert updated.target_message_id == 1, "the delivered platform id is attached"


async def test_delivers_with_reply_anchor_when_present(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
) -> None:
    user = users.get_or_register(Platform.TELEGRAM, 100)
    enqueue(
        repos,
        relay_payload(target_user_id=user.id, reply_to=42),
        pair_id=pair_id,
    )

    await worker.process_due()

    assert senders[Platform.TELEGRAM].sent[0].reply_to_message_id == 42


async def test_not_due_entries_are_left_alone(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
) -> None:
    user = users.get_or_register(Platform.BALE, 100)
    enqueue(
        repos,
        relay_payload(target_user_id=user.id),
        pair_id=pair_id,
    )
    repos.outbox.mark_failed(
        pair_id,
        repos.outbox.list_for_pair(pair_id)[0].id,
        error="reschedule",
        next_attempt_at=FIXED_NOW + timedelta(hours=1),
    )

    stats = await worker.process_due()

    assert stats.total == 0
    assert senders[Platform.BALE].sent == []


# -------------------------------------------------------------- transient errors


async def test_transient_failure_is_scheduled_for_retry(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
) -> None:
    user = users.get_or_register(Platform.BALE, 100)
    entry_id = enqueue(repos, relay_payload(target_user_id=user.id), pair_id=pair_id)
    senders[Platform.BALE].failures[100] = TransportError("gateway timeout")

    stats = await worker.process_due()

    assert stats == OutboxRunStats(retried=1)
    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert entry.status == OutboxStatus.PENDING
    assert entry.attempts == 1
    assert entry.next_attempt_at == FIXED_NOW + timedelta(seconds=10)
    assert senders[Platform.BALE].sent == []


async def test_retries_until_it_succeeds(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
) -> None:
    user = users.get_or_register(Platform.BALE, 100)
    entry_id = enqueue(repos, relay_payload(target_user_id=user.id), pair_id=pair_id)
    senders[Platform.BALE].failures[100] = TransportError("gateway timeout")

    await worker.process_due()
    senders[Platform.BALE].failures.clear()

    # A worker whose clock moved past the retry delay sees the entry again.
    later = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW + timedelta(seconds=11),
        retry_delay=timedelta(seconds=10),
        max_attempts=3,
        random_source=lambda: 0.5,
    )
    final = await later.process_due()

    assert final == OutboxRunStats(delivered=1)
    assert repos.outbox.get_for_pair(pair_id, entry_id).status == OutboxStatus.SENT


# ------------------------------------------------------------- permanent errors


async def test_permanent_failure_marks_the_entry_failed(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
) -> None:
    user = users.get_or_register(Platform.BALE, 100)
    entry_id = enqueue(repos, relay_payload(target_user_id=user.id), pair_id=pair_id)
    senders[Platform.BALE].failures[100] = DeliveryError(
        "Forbidden: bot was blocked", platform="bale", status_code=403
    )

    stats = await worker.process_due()

    assert stats == OutboxRunStats(failed=1)
    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert entry.status == OutboxStatus.FAILED
    assert entry.attempts == 1


async def test_exhausted_attempts_become_terminal(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    pair_id: int,
) -> None:
    def build_worker(clock_now) -> OutboxWorker:
        return OutboxWorker(
            users=users,
            messages=repos.messages,
            outbox=repos.outbox,
            tx=repos.db,
            senders=senders,
            clock=clock_now,
            retry_delay=timedelta(seconds=10),
            max_attempts=2,
            random_source=lambda: 0.5,
        )

    user = users.get_or_register(Platform.BALE, 100)
    entry_id = enqueue(repos, relay_payload(target_user_id=user.id), pair_id=pair_id)
    senders[Platform.BALE].failures[100] = TransportError("down")

    await build_worker(lambda: FIXED_NOW).process_due()  # attempt 1 -> retried
    first = repos.outbox.get_for_pair(pair_id, entry_id)
    assert first.status == OutboxStatus.PENDING

    await build_worker(
        lambda: FIXED_NOW + timedelta(seconds=11)
    ).process_due()  # attempt 2 == max -> terminal

    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert entry.status == OutboxStatus.FAILED
    assert entry.attempts == 2


# ------------------------------------------------------------ malformed payloads


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "other"},  # unsupported kind
        {"kind": RELAY_PAYLOAD_KIND},  # no fields at all
        {
            "kind": RELAY_PAYLOAD_KIND,
            "target_user_id": "not-an-int",
            "target_chat_id": 1,
            "text": "x",
        },
        {
            "kind": RELAY_PAYLOAD_KIND,
            "target_user_id": 1,
            "target_chat_id": 1,
            "text": 123,  # text must be a string
        },
    ],
)
async def test_malformed_payloads_fail_terminally_without_sending(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
    payload: dict,
) -> None:
    users.get_or_register(Platform.BALE, 100)
    entry_id = enqueue(repos, payload, pair_id=pair_id)

    stats = await worker.process_due()

    assert stats == OutboxRunStats(failed=1)
    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert entry.status == OutboxStatus.FAILED
    assert entry.last_error is not None
    assert senders[Platform.BALE].sent == []
    assert senders[Platform.TELEGRAM].sent == []


async def test_unknown_target_user_fails_terminally(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    worker: OutboxWorker,
    pair_id: int,
) -> None:
    entry_id = enqueue(
        repos, relay_payload(target_user_id=9999), pair_id=pair_id
    )

    stats = await worker.process_due()

    assert stats == OutboxRunStats(failed=1)
    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert entry.status == OutboxStatus.FAILED
    assert "unknown target" in entry.last_error


async def test_missing_sender_for_the_target_platform_fails_terminally(
    users: UserService,
    repos: Repositories,
    pair_id: int,
) -> None:
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders={},  # no senders configured at all
        clock=lambda: FIXED_NOW,
    )
    user = users.get_or_register(Platform.BALE, 100)
    entry_id = enqueue(repos, relay_payload(target_user_id=user.id), pair_id=pair_id)

    stats = await worker.process_due()

    assert stats == OutboxRunStats(failed=1)
    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert entry.status == OutboxStatus.FAILED
    assert "no sender configured" in entry.last_error


# --------------------------------------------------------------------- lifecycle


async def test_unexpected_sender_error_is_retried_and_never_stalls_the_queue(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    pair_id: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An adapter bug on one entry must not stop the entries behind it.

    Only the exception *type* may be recorded: a stray exception message can
    carry user content, which the INFO/WARNING logs must never show.
    """

    class ExplodingSender(RecordingSender):
        """Sender that raises a non-MessagingError, with content in the message."""

        async def send_message(
            self, chat_id: int, text: str, *, reply_to_message_id: int | None = None
        ):
            raise RuntimeError(f"boom: {text}")

    secret = "متن محرمانه در استثنا"
    broken = ExplodingSender(Platform.BALE)
    good = users.get_or_register(Platform.TELEGRAM, 200)
    target = users.get_or_register(Platform.BALE, 100)
    first_id = enqueue(
        repos, relay_payload(target_user_id=target.id, text=secret), pair_id=pair_id
    )
    second_id = enqueue(
        repos,
        relay_payload(target_user_id=good.id, target_chat_id=200, message_id=2),
        pair_id=pair_id,
    )
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders={Platform.BALE: broken, Platform.TELEGRAM: senders[Platform.TELEGRAM]},
        clock=lambda: FIXED_NOW,
        retry_delay=timedelta(seconds=10),
        max_attempts=3,
        random_source=lambda: 0.5,
    )

    with caplog.at_level(logging.INFO, logger="bridge.delivery"):
        stats = await worker.process_due()

    assert stats == OutboxRunStats(delivered=1, retried=1)
    failed = repos.outbox.get_for_pair(pair_id, first_id)
    assert failed.status == OutboxStatus.PENDING
    assert failed.attempts == 1
    assert "unexpected RuntimeError" in (failed.last_error or "")
    assert secret not in caplog.text
    assert "boom" not in caplog.text
    assert repos.outbox.get_for_pair(pair_id, second_id).status == OutboxStatus.SENT


async def test_run_survives_a_crashing_pass(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    caplog: pytest.LogCaptureFixture,
) -> None:
    import asyncio

    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
    )
    calls: list[int] = []

    async def crashing_pass(*, limit: int = 100) -> OutboxRunStats:
        calls.append(limit)
        raise RuntimeError("database is gone")

    worker.process_due = crashing_pass  # type: ignore[method-assign]
    stop = asyncio.Event()

    async def fake_sleep(delay: float) -> None:
        if len(calls) >= 2:
            stop.set()

    with caplog.at_level(logging.ERROR, logger="bridge.delivery"):
        await worker.run(stop=stop, interval=0.5, sleep=fake_sleep)

    assert len(calls) == 2, "the worker kept looping after the failed pass"
    assert "outbox pass aborted (RuntimeError)" in caplog.text


async def test_run_processes_until_stop_is_set(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    pair_id: int,
) -> None:
    import asyncio

    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
    )
    user = users.get_or_register(Platform.BALE, 100)
    enqueue(repos, relay_payload(target_user_id=user.id), pair_id=pair_id)

    stop = asyncio.Event()
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        stop.set()  # stop after the first pass

    await worker.run(stop=stop, interval=2.0, sleep=fake_sleep)

    assert len(senders[Platform.BALE].sent) == 1
    assert sleeps == [2.0]


async def test_invalid_construction_is_rejected(
    users: UserService, repos: Repositories, senders: dict[Platform, RecordingSender]
) -> None:
    with pytest.raises(ValueError):
        OutboxWorker(
            users=users,
            messages=repos.messages,
            outbox=repos.outbox,
            tx=repos.db,
            senders=senders,
            retry_delay=timedelta(0),
        )
    with pytest.raises(ValueError):
        OutboxWorker(
            users=users,
            messages=repos.messages,
            outbox=repos.outbox,
            tx=repos.db,
            senders=senders,
            max_attempts=0,
        )
