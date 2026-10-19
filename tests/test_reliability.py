"""Phase 4 tests: durable queue, retry/backoff, rate limits, clean shutdown.

Every test drives the clock, the random sample or the sleeper explicitly, so the
suite never waits on real time. No test touches the network.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import Any

import pytest

from core.commands import CommandRouter
from core.delivery import (
    RATE_LIMIT_QUEUED_NOTICE,
    RATE_LIMIT_REJECTED_NOTICE,
    OutboxRunStats,
    OutboxWorker,
)
from core.errors import (
    ApiError,
    DeliveryError,
    RateLimitError,
    TransportError,
)
from core.health import Heartbeat
from core.models import OutboxStatus, Platform
from core.pairing import PairingService, PairingStatus
from core.ratelimit import RateLimiter
from core.retry import RetryPolicy, is_retryable
from core.users import UserRole, UserService
from storage.db import Repositories
from tests.conftest import FIXED_NOW, FakeClock, RecordingSender

# ------------------------------------------------------------------ helpers


def make_pair(users: UserService, repos: Repositories) -> int:
    """A real active pair so outbox rows satisfy their foreign key."""
    alice = users.get_or_register(Platform.TELEGRAM, 1)
    bob = users.get_or_register(Platform.BALE, 2)
    pair = repos.pairs.create(
        user_a_id=min(alice.id, bob.id),
        user_b_id=max(alice.id, bob.id),
        status=PairingStatus.ACTIVE,
        created_at=FIXED_NOW,
    )
    return pair.id


def payload(
    *,
    target_user_id: int,
    target_chat_id: int = 100,
    text: str = "متن",
    source_user_id: int | None = None,
) -> dict[str, Any]:
    """A well-formed text relay payload."""
    body: dict[str, Any] = {
        "kind": "relay_text",
        "message_id": 1,
        "target_user_id": target_user_id,
        "target_chat_id": target_chat_id,
        "text": text,
        "reply_to_message_id": None,
    }
    if source_user_id is not None:
        body["source_user_id"] = source_user_id
    return body


def enqueue(
    repos: Repositories, pair_id: int, body: dict[str, Any], *, at=None
) -> int:
    entry = repos.outbox.enqueue(
        pair_id=pair_id,
        payload=body,
        next_attempt_at=at or FIXED_NOW,
        created_at=FIXED_NOW,
    )
    return entry.id


def worker(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    *,
    clock=None,
    retry_policy: RetryPolicy | None = None,
    rate_limiter: RateLimiter | None = None,
    user_rate_limit_action: str = "queue",
    max_attempts: int = 5,
    sent_retention: timedelta = timedelta(days=7),
) -> OutboxWorker:
    """Worker with deterministic jitter and an optional fake clock/limiter."""
    return OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=clock or (lambda: FIXED_NOW),
        retry_policy=retry_policy,
        rate_limiter=rate_limiter,
        user_rate_limit_action=user_rate_limit_action,
        max_attempts=max_attempts,
        sent_retention=sent_retention,
        random_source=lambda: 0.5,
    )


# ------------------------------------------------------- durable queue states


def test_claim_moves_a_due_entry_to_sending_without_counting_an_attempt(
    repos: Repositories,
) -> None:
    pair_id = make_pair(UserService(repos.users, repos.db, max_users=0), repos)
    entry_id = enqueue(repos, pair_id, payload(target_user_id=1))

    claimed = repos.outbox.claim_due(FIXED_NOW)

    assert [entry.id for entry in claimed] == [entry_id]
    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert entry.status is OutboxStatus.SENDING
    assert entry.locked_at == FIXED_NOW
    assert entry.attempts == 0
    assert repos.outbox.claim_due(FIXED_NOW) == (), "a lease is not claimable twice"


async def test_orphaned_sending_entry_is_recovered_and_sent_exactly_once(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    pair_id = make_pair(users, repos)
    target = users.get_or_register(Platform.BALE, 500)
    entry_id = enqueue(
        repos, pair_id, payload(target_user_id=target.id, target_chat_id=500)
    )
    repos.outbox.claim_due(FIXED_NOW)
    assert repos.outbox.get_for_pair(pair_id, entry_id).status is OutboxStatus.SENDING

    # A new worker represents the process after a crash/restart.
    recovered = await worker(users, repos, senders).process_due()

    assert recovered == OutboxRunStats(delivered=1)
    assert len(senders[Platform.BALE].sent) == 1, "the recovered entry is sent once"
    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert entry.status is OutboxStatus.SENT
    assert entry.locked_at is None
    assert entry.sent_at == FIXED_NOW


async def test_worker_purges_old_sent_tombstones_on_start(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    pair_id = make_pair(users, repos)
    target = users.get_or_register(Platform.BALE, 500)
    entry_id = enqueue(
        repos, pair_id, payload(target_user_id=target.id, target_chat_id=500)
    )
    repos.outbox.mark_sent(
        pair_id, entry_id, sent_at=FIXED_NOW - timedelta(days=10)
    )

    await worker(
        users, repos, senders, sent_retention=timedelta(days=1)
    ).process_due()

    assert repos.outbox.get_for_pair(pair_id, entry_id) is None


async def test_stale_lease_is_recovered_inside_a_long_running_worker(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    clock = FakeClock()
    pair_id = make_pair(users, repos)
    target = users.get_or_register(Platform.BALE, 500)
    w = worker(users, repos, senders, clock=clock)
    await w.process_due()  # establishes the "started" state (queue is empty)
    entry_id = enqueue(
        repos, pair_id, payload(target_user_id=target.id, target_chat_id=500)
    )
    repos.outbox.claim_due(clock.now)  # simulate a pass dying after the lease

    clock.advance(minutes=6)  # past the 5-minute stale-lease grace period
    stats = await w.process_due()

    assert stats.delivered == 1
    assert repos.outbox.get_for_pair(pair_id, entry_id).status is OutboxStatus.SENT


# --------------------------------------------------------- retry / backoff


def test_retry_policy_is_exponential_with_jitter_bounds_and_a_cap() -> None:
    policy = RetryPolicy(
        base_delay=timedelta(seconds=10),
        factor=2,
        max_delay=timedelta(seconds=60),
        jitter_ratio=0.2,
        max_attempts=5,
    )

    assert policy.delay_for(1, random_value=0.5) == timedelta(seconds=10)
    assert policy.delay_for(1, random_value=0.0) == timedelta(seconds=8)
    assert policy.delay_for(1, random_value=0.999) <= timedelta(seconds=12)
    assert policy.delay_for(2, random_value=0.5) == timedelta(seconds=20)
    assert policy.delay_for(9, random_value=0.5) == timedelta(seconds=60)
    assert policy.delay_for(2, retry_after=42) == timedelta(seconds=42)


async def test_worker_backoff_grows_then_resets_and_is_stored_in_the_queue(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    clock = FakeClock()
    policy = RetryPolicy(
        base_delay=timedelta(seconds=10),
        factor=2,
        max_delay=timedelta(seconds=60),
        jitter_ratio=0,
        max_attempts=5,
    )
    pair_id = make_pair(users, repos)
    target = users.get_or_register(Platform.BALE, 500)
    entry_id = enqueue(
        repos, pair_id, payload(target_user_id=target.id, target_chat_id=500)
    )
    senders[Platform.BALE].failures[500] = TransportError("down")
    w = worker(users, repos, senders, clock=clock, retry_policy=policy)

    first = await w.process_due()
    first_entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert first.retried == 1
    assert first_entry.attempts == 1
    assert first_entry.next_attempt_at == FIXED_NOW + timedelta(seconds=10)

    clock.advance(seconds=10)
    second = await w.process_due()
    second_entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert second.retried == 1
    assert second_entry.attempts == 2
    assert second_entry.next_attempt_at == clock.now + timedelta(seconds=20)

    senders[Platform.BALE].failures.clear()
    clock.advance(seconds=20)
    final = await w.process_due()

    assert final.delivered == 1
    assert repos.outbox.get_for_pair(pair_id, entry_id).status is OutboxStatus.SENT


async def test_platform_retry_after_wins_over_the_backoff(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    pair_id = make_pair(users, repos)
    target = users.get_or_register(Platform.BALE, 500)
    entry_id = enqueue(
        repos, pair_id, payload(target_user_id=target.id, target_chat_id=500)
    )
    senders[Platform.BALE].failures[500] = RateLimitError(
        "flood control", retry_after=42, platform="bale"
    )

    stats = await worker(users, repos, senders).process_due()

    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert stats.retried == 1
    assert entry.attempts == 1
    assert entry.next_attempt_at == FIXED_NOW + timedelta(seconds=42)


async def test_max_attempts_produces_a_terminal_failure_with_reason(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    clock = FakeClock()
    policy = RetryPolicy(
        base_delay=timedelta(seconds=10),
        factor=2,
        jitter_ratio=0,
        max_attempts=2,
    )
    pair_id = make_pair(users, repos)
    target = users.get_or_register(Platform.BALE, 500)
    entry_id = enqueue(
        repos, pair_id, payload(target_user_id=target.id, target_chat_id=500)
    )
    senders[Platform.BALE].failures[500] = TransportError("still down")
    w = worker(users, repos, senders, clock=clock, retry_policy=policy)

    await w.process_due()
    assert repos.outbox.get_for_pair(pair_id, entry_id).status is OutboxStatus.PENDING
    clock.advance(seconds=10)
    stats = await w.process_due()

    entry = repos.outbox.get_for_pair(pair_id, entry_id)
    assert stats.failed == 1
    assert entry.status is OutboxStatus.FAILED
    assert entry.attempts == 2
    assert entry.last_error == "still down"
    assert repos.outbox.queue_stats(clock.now).errors_24h == 2


# ------------------------------------------------------------- rate limiting


def _fast_limiter(
    clock: FakeClock,
    *,
    per_user: int = 30,
    per_chat: float = 100.0,
    per_platform: int = 1000,
) -> RateLimiter:
    """Limiter whose only meaningful limit is the one each test configures."""
    return RateLimiter(
        global_per_second={Platform.TELEGRAM: per_platform, Platform.BALE: per_platform},
        per_chat_per_second={Platform.TELEGRAM: per_chat, Platform.BALE: per_chat},
        per_user_per_minute=per_user,
        clock=clock,
    )


async def test_user_rate_limit_queues_excess_messages_and_sends_one_notice(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    clock = FakeClock()
    pair_id = make_pair(users, repos)
    source = users.get_or_register(Platform.TELEGRAM, 900)
    target = users.get_or_register(Platform.BALE, 901)
    for index in range(4):
        enqueue(
            repos,
            pair_id,
            payload(
                target_user_id=target.id,
                target_chat_id=1000 + index,
                source_user_id=source.id,
            ),
        )

    stats = await worker(
        users,
        repos,
        senders,
        clock=clock,
        rate_limiter=_fast_limiter(clock, per_user=2),
    ).process_due()

    assert stats == OutboxRunStats(delivered=2, deferred=2)
    assert len(senders[Platform.BALE].sent) == 2
    assert senders[Platform.TELEGRAM].texts_to(900) == [RATE_LIMIT_QUEUED_NOTICE]
    queued = repos.outbox.list_for_pair(pair_id, status=OutboxStatus.PENDING)
    assert len(queued) == 2
    assert all(entry.attempts == 0 for entry in queued), "throttling is not an attempt"
    assert all(
        entry.next_attempt_at == FIXED_NOW + timedelta(seconds=60) for entry in queued
    )


async def test_per_chat_limit_defers_without_a_user_notice(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    clock = FakeClock()
    pair_id = make_pair(users, repos)
    target = users.get_or_register(Platform.BALE, 900)
    first_source = users.get_or_register(Platform.TELEGRAM, 901)
    second_source = users.get_or_register(Platform.TELEGRAM, 902)
    enqueue(
        repos,
        pair_id,
        payload(
            target_user_id=target.id,
            target_chat_id=777,
            source_user_id=first_source.id,
        ),
    )
    enqueue(
        repos,
        pair_id,
        payload(
            target_user_id=target.id,
            target_chat_id=777,
            source_user_id=second_source.id,
        ),
    )

    stats = await worker(
        users,
        repos,
        senders,
        clock=clock,
        rate_limiter=_fast_limiter(clock, per_user=0, per_chat=1.0),
    ).process_due()

    assert stats == OutboxRunStats(delivered=1, deferred=1)
    assert senders[Platform.TELEGRAM].sent == [], "operational throttling stays silent"


async def test_user_limit_can_reject_instead_of_queue(
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    clock = FakeClock()
    pair_id = make_pair(users, repos)
    source = users.get_or_register(Platform.TELEGRAM, 900)
    target = users.get_or_register(Platform.BALE, 901)
    for index in range(2):
        enqueue(
            repos,
            pair_id,
            payload(
                target_user_id=target.id,
                target_chat_id=2000 + index,
                source_user_id=source.id,
            ),
        )

    stats = await worker(
        users,
        repos,
        senders,
        clock=clock,
        rate_limiter=_fast_limiter(clock, per_user=1),
        user_rate_limit_action="reject",
    ).process_due()

    assert stats == OutboxRunStats(delivered=1, failed=1)
    entries = repos.outbox.list_for_pair(pair_id)
    assert entries[0].status is OutboxStatus.SENT
    assert entries[1].status is OutboxStatus.FAILED
    assert entries[1].last_error == "user rate limit exceeded"
    assert senders[Platform.TELEGRAM].texts_to(900) == [RATE_LIMIT_REJECTED_NOTICE]


# --------------------------------------------------------- clean shutdown


class BlockingSender(RecordingSender):
    """Sender that announces when a send starts and waits for a release."""

    def __init__(self, platform: Platform) -> None:
        super().__init__(platform)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def send_message(
        self, chat_id: int, text: str, *, reply_to_message_id: int | None = None
    ):
        self.entered.set()
        await self.release.wait()
        return await super().send_message(
            chat_id, text, reply_to_message_id=reply_to_message_id
        )


async def test_shutdown_lets_the_in_flight_send_finish_and_marks_it_sent(
    users: UserService,
    repos: Repositories,
) -> None:
    pair_id = make_pair(users, repos)
    target = users.get_or_register(Platform.BALE, 500)
    entry_id = enqueue(
        repos, pair_id, payload(target_user_id=target.id, target_chat_id=500)
    )
    blocking = BlockingSender(Platform.BALE)
    senders = {Platform.TELEGRAM: RecordingSender(Platform.TELEGRAM), Platform.BALE: blocking}
    stop = asyncio.Event()
    w = worker(users, repos, senders)

    async def fake_sleep(_delay: float) -> None:
        await asyncio.sleep(0)

    task = asyncio.create_task(w.run(stop=stop, interval=0.01, sleep=fake_sleep))
    await blocking.entered.wait()

    stop.set()  # graceful shutdown requested mid-send
    blocking.release.set()
    await asyncio.wait_for(task, timeout=2.0)

    assert len(blocking.sent) == 1, "the in-flight message completed"
    assert repos.outbox.get_for_pair(pair_id, entry_id).status is OutboxStatus.SENT


# --------------------------------------------------------------- /status


def test_owner_status_reports_queue_errors_last_relay_and_active_users(
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    clock: FakeClock,
) -> None:
    router = CommandRouter(users, pairing, stats=repos.outbox.queue_stats, clock=clock)
    owner = users.get_or_register(Platform.TELEGRAM, 800, role=UserRole.OWNER)
    alice = users.get_or_register(Platform.TELEGRAM, 801)
    bob = users.get_or_register(Platform.BALE, 802)
    request = pairing.request(alice.id, bob.id)
    pairing.accept(request.id, bob.id)

    pair_id = repos.pairs.list_active_for_user(alice.id)[0].id
    sent_id = enqueue(repos, pair_id, payload(target_user_id=bob.id))
    repos.outbox.mark_sent(pair_id, sent_id, sent_at=clock.now)
    failed_id = enqueue(repos, pair_id, payload(target_user_id=bob.id))
    repos.outbox.mark_failed(
        pair_id,
        failed_id,
        error="boom",
        next_attempt_at=clock.now,
        terminal=True,
        now=clock.now,
    )
    enqueue(repos, pair_id, payload(target_user_id=bob.id))

    (action,) = router.handle(owner.id, "/status")

    assert "وضعیت کلی پل" in action.text
    assert "کاربران: 3 (فعال 3، مسدود 0، مالک 1)" in action.text
    assert "جفت‌های فعال: 1" in action.text
    assert "صف ارسال: 1 (در انتظار 1، در حال ارسال 0، ناموفق 1)" in action.text
    assert "خطاهای ۲۴ ساعت گذشته: 1" in action.text
    assert "آخرین رله: 2026-01-01 12:00 UTC" in action.text


# ---------------------------------------------------------------- health


def test_heartbeat_writes_a_fresh_atomic_marker(tmp_path, clock: FakeClock) -> None:
    path = tmp_path / "health" / "bridge.json"
    heartbeat = Heartbeat(path, clock=clock)

    heartbeat.beat()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["status"] == "ok"
    assert payload["time"] == clock.now.isoformat()

    heartbeat.beat(status="degraded")
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "degraded"

    heartbeat.clear()
    assert not path.exists()


def test_failure_classification_matches_the_retry_contract() -> None:
    assert is_retryable(TransportError("network"))
    assert is_retryable(RateLimitError("slow", retry_after=1))
    assert not is_retryable(DeliveryError("blocked", status_code=403))
    assert not is_retryable(ApiError("bad request", status_code=400))
    assert is_retryable(ApiError("server error", status_code=503))


def test_invalid_retry_policy_is_rejected() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(base_delay=timedelta(0))
    with pytest.raises(ValueError):
        RetryPolicy(jitter_ratio=1.5)
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
