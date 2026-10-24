"""Rule-by-rule tests for :mod:`core.pairing`."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from core.errors import (
    AlreadyPairedError,
    DuplicatePairRequestError,
    NotRequesterError,
    NotRequestTargetError,
    PairInviteInvalidError,
    PairLimitReachedError,
    PairNotActiveError,
    PairNotFoundError,
    PairRequestNotFoundError,
    RequestExpiredError,
    RequestNotPendingError,
    SelfPairingError,
    UserBannedError,
    UserNotFoundError,
)
from core.models import Platform
from core.pairing import Pair, PairingService, PairingStatus, PairRequest
from core.users import User, UserRole, UserService
from storage.db import Repositories
from tests.conftest import FakeClock


@dataclass
class Harness:
    """Services plus small helpers, all sharing one database."""

    repos: Repositories
    clock: FakeClock
    users: UserService
    pairing: PairingService
    _next_platform_id: int = 900

    def user(
        self,
        *,
        platform: Platform = Platform.TELEGRAM,
        platform_user_id: int | None = None,
        role: UserRole = UserRole.USER,
    ) -> User:
        if platform_user_id is None:
            self._next_platform_id += 1
            platform_user_id = self._next_platform_id
        return self.users.get_or_register(platform, platform_user_id, role=role)

    def pair(self, first: User, second: User) -> Pair:
        request = self.pairing.request(first.id, second.id)
        return self.pairing.accept(request.id, second.id)

    def table_count(self, table: str) -> int:
        row = self.repos.db.connection.execute(f"SELECT COUNT(*) AS total FROM {table}").fetchone()
        return int(row["total"])


@pytest.fixture
def build(repos: Repositories, clock: FakeClock) -> Callable[..., Harness]:
    """Factory building a harness with custom limits."""

    def _build(
        *,
        max_pairs_per_user: int = 1,
        ttl_hours: float = 24,
    ) -> Harness:
        users = UserService(repos.users, repos.db, max_users=0)
        pairing = PairingService(
            repos.users,
            repos.pairs,
            repos.requests,
            repos.db,
            max_pairs_per_user=max_pairs_per_user,
            request_ttl=timedelta(hours=ttl_hours),
            invites=repos.invites,
            invite_ttl=timedelta(minutes=15),
            clock=clock,
        )
        return Harness(repos=repos, clock=clock, users=users, pairing=pairing)

    return _build


# ------------------------------------------------------------------ constructor


def test_constructor_rejects_invalid_limits(repos: Repositories, clock: FakeClock) -> None:
    def make(**kwargs: object) -> PairingService:
        return PairingService(
            repos.users, repos.pairs, repos.requests, repos.db, **kwargs  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError):
        make(max_pairs_per_user=0)
    with pytest.raises(ValueError):
        make(request_ttl=timedelta(0))
    with pytest.raises(ValueError):
        make(request_ttl=timedelta(hours=-1))


def test_configured_limits_are_exposed(build: Callable[..., Harness]) -> None:
    harness = build(max_pairs_per_user=3, ttl_hours=5)

    assert harness.pairing.max_pairs_per_user == 3
    assert harness.pairing.request_ttl == timedelta(hours=5)


# --------------------------------------------------------------------- request


def test_request_creates_pending_row_with_ttl(build: Callable[..., Harness]) -> None:
    harness = build(ttl_hours=24)
    alice = harness.user()
    bob = harness.user()

    request = harness.pairing.request(alice.id, bob.id)

    assert request.status is PairingStatus.PENDING
    assert request.pair_id is None
    assert request.requester_id == alice.id
    assert request.target_id == bob.id
    assert request.created_at == harness.clock.now
    assert request.expires_at == harness.clock.now + timedelta(hours=24)
    assert request.is_pending
    assert not request.is_overdue(harness.clock.now)


def test_request_ttl_is_configurable(build: Callable[..., Harness]) -> None:
    harness = build(ttl_hours=2)
    alice = harness.user()
    bob = harness.user()

    request = harness.pairing.request(alice.id, bob.id)

    assert request.expires_at == harness.clock.now + timedelta(hours=2)


def test_self_pairing_is_rejected(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()

    with pytest.raises(SelfPairingError):
        harness.pairing.request(alice.id, alice.id)

    assert harness.table_count("pair_requests") == 0


def test_self_pairing_is_rejected_even_for_unknown_id(build: Callable[..., Harness]) -> None:
    harness = build()

    with pytest.raises(SelfPairingError):
        harness.pairing.request(4242, 4242)


def test_unknown_users_are_rejected(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()

    with pytest.raises(UserNotFoundError):
        harness.pairing.request(999_999, alice.id)
    with pytest.raises(UserNotFoundError):
        harness.pairing.request(alice.id, 999_999)


def test_banned_requester_cannot_request(build: Callable[..., Harness]) -> None:
    harness = build()
    owner = harness.user(role=UserRole.OWNER)
    alice = harness.user()
    bob = harness.user()
    harness.users.ban(owner, alice.id)

    with pytest.raises(UserBannedError):
        harness.pairing.request(alice.id, bob.id)

    assert harness.table_count("pair_requests") == 0


def test_banned_target_cannot_receive_requests(build: Callable[..., Harness]) -> None:
    harness = build()
    owner = harness.user(role=UserRole.OWNER)
    alice = harness.user()
    bob = harness.user()
    harness.users.ban(owner, bob.id)

    with pytest.raises(UserBannedError):
        harness.pairing.request(alice.id, bob.id)

    assert harness.table_count("pair_requests") == 0


def test_duplicate_pending_request_is_rejected(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    harness.pairing.request(alice.id, bob.id)

    with pytest.raises(DuplicatePairRequestError):
        harness.pairing.request(alice.id, bob.id)

    assert harness.table_count("pair_requests") == 1


def test_reverse_direction_pending_request_is_also_rejected(
    build: Callable[..., Harness],
) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    harness.pairing.request(alice.id, bob.id)

    with pytest.raises(DuplicatePairRequestError):
        harness.pairing.request(bob.id, alice.id)


def test_expired_pending_request_does_not_block_a_new_one(
    build: Callable[..., Harness],
) -> None:
    harness = build(ttl_hours=24)
    alice = harness.user()
    bob = harness.user()
    first = harness.pairing.request(alice.id, bob.id)

    harness.clock.advance(hours=25)
    second = harness.pairing.request(alice.id, bob.id)

    assert second.id != first.id
    assert harness.repos.requests.get(first.id).status is PairingStatus.EXPIRED
    assert second.status is PairingStatus.PENDING


def test_request_between_already_paired_users_is_rejected(
    build: Callable[..., Harness],
) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    harness.pair(alice, bob)

    with pytest.raises(AlreadyPairedError):
        harness.pairing.request(bob.id, alice.id)


def test_requester_at_pair_limit_cannot_request(build: Callable[..., Harness]) -> None:
    harness = build(max_pairs_per_user=1)
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    harness.pair(alice, bob)

    with pytest.raises(PairLimitReachedError):
        harness.pairing.request(alice.id, carol.id)

    assert harness.pairing.active_pairs(alice.id)


def test_target_at_pair_limit_cannot_receive_request(build: Callable[..., Harness]) -> None:
    harness = build(max_pairs_per_user=1)
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    harness.pair(bob, carol)

    with pytest.raises(PairLimitReachedError):
        harness.pairing.request(alice.id, bob.id)


def test_two_active_pairs_are_allowed_when_limit_is_two(
    build: Callable[..., Harness],
) -> None:
    harness = build(max_pairs_per_user=2)
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    dave = harness.user()

    harness.pair(alice, bob)
    harness.pair(alice, carol)

    assert len(harness.pairing.active_pairs(alice.id)) == 2
    with pytest.raises(PairLimitReachedError):
        harness.pairing.request(alice.id, dave.id)


# ---------------------------------------------------------------------- accept


def test_target_accepts_and_pair_becomes_active(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)

    pair = harness.pairing.accept(request.id, bob.id)

    assert pair.status is PairingStatus.ACTIVE
    assert pair.includes(alice.id) and pair.includes(bob.id)
    assert pair.other(alice.id) == bob.id
    assert pair.other(bob.id) == alice.id
    assert pair.ended_at is None
    stored_request = harness.repos.requests.get(request.id)
    assert stored_request.status is PairingStatus.ACTIVE
    assert stored_request.pair_id == pair.id


def test_pair_members_are_stored_in_canonical_order(build: Callable[..., Harness]) -> None:
    harness = build()
    first = harness.user()
    second = harness.user()
    request = harness.pairing.request(second.id, first.id)

    pair = harness.pairing.accept(request.id, first.id)

    assert pair.user_a_id == min(first.id, second.id)
    assert pair.user_b_id == max(first.id, second.id)


def test_only_target_can_accept(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    request = harness.pairing.request(alice.id, bob.id)

    with pytest.raises(NotRequestTargetError):
        harness.pairing.accept(request.id, alice.id)
    with pytest.raises(NotRequestTargetError):
        harness.pairing.accept(request.id, carol.id)

    assert harness.table_count("pairs") == 0
    assert harness.repos.requests.get(request.id).status is PairingStatus.PENDING


def test_accept_unknown_request(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()

    with pytest.raises(PairRequestNotFoundError):
        harness.pairing.accept(999_999, alice.id)


def test_expired_request_cannot_be_accepted(build: Callable[..., Harness]) -> None:
    harness = build(ttl_hours=1)
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)

    harness.clock.advance(hours=2)
    with pytest.raises(RequestExpiredError):
        harness.pairing.accept(request.id, bob.id)

    assert harness.repos.requests.get(request.id).status is PairingStatus.EXPIRED
    assert harness.table_count("pairs") == 0


@pytest.mark.parametrize("action", ["accept", "reject"])
def test_expired_request_cannot_be_answered(
    build: Callable[..., Harness], action: str
) -> None:
    harness = build(ttl_hours=1)
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.clock.advance(hours=3)

    with pytest.raises(RequestExpiredError):
        getattr(harness.pairing, action)(request.id, bob.id)


def test_expired_request_cannot_be_cancelled(build: Callable[..., Harness]) -> None:
    harness = build(ttl_hours=1)
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.clock.advance(hours=3)

    with pytest.raises(RequestExpiredError):
        harness.pairing.cancel(request.id, alice.id)


def test_answered_request_cannot_be_accepted_again(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.pairing.accept(request.id, bob.id)

    with pytest.raises(RequestNotPendingError):
        harness.pairing.accept(request.id, bob.id)


def test_banned_actor_cannot_accept(build: Callable[..., Harness]) -> None:
    harness = build()
    owner = harness.user(role=UserRole.OWNER)
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.users.ban(owner, bob.id)

    with pytest.raises(UserBannedError):
        harness.pairing.accept(request.id, bob.id)


def test_banned_requester_blocks_acceptance(build: Callable[..., Harness]) -> None:
    harness = build()
    owner = harness.user(role=UserRole.OWNER)
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.users.ban(owner, alice.id)

    with pytest.raises(UserBannedError):
        harness.pairing.accept(request.id, bob.id)


def test_accept_fails_when_target_reached_pair_limit(build: Callable[..., Harness]) -> None:
    harness = build(max_pairs_per_user=1)
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.pair(bob, carol)  # bob is now at capacity

    with pytest.raises(PairLimitReachedError):
        harness.pairing.accept(request.id, bob.id)

    # Atomicity: nothing was created and the request stayed pending.
    assert harness.table_count("pairs") == 1
    assert harness.repos.requests.get(request.id).status is PairingStatus.PENDING


def test_accept_fails_when_already_paired(build: Callable[..., Harness]) -> None:
    harness = build(max_pairs_per_user=2)
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    # Simulate a pair that was activated through another path (e.g. a retried
    # worker) while this pending request was still around.
    harness.repos.pairs.create(
        user_a_id=alice.id,
        user_b_id=bob.id,
        status=PairingStatus.ACTIVE,
        created_at=harness.clock.now,
    )

    with pytest.raises(AlreadyPairedError):
        harness.pairing.accept(request.id, bob.id)

    assert harness.repos.requests.get(request.id).status is PairingStatus.PENDING


def test_failed_accept_rolls_back_completely(build: Callable[..., Harness]) -> None:
    harness = build(max_pairs_per_user=1)
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.pair(bob, carol)  # bob is now at capacity

    with pytest.raises(PairLimitReachedError):
        harness.pairing.accept(request.id, bob.id)

    assert harness.table_count("pairs") == 1
    assert harness.table_count("pair_requests") == 2
    assert harness.repos.requests.get(request.id).status is PairingStatus.PENDING


# ---------------------------------------------------------------------- reject


def test_target_rejects_request(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)

    rejected = harness.pairing.reject(request.id, bob.id)

    assert rejected.status is PairingStatus.REJECTED
    assert harness.table_count("pairs") == 0
    assert harness.pairing.outgoing_requests(alice.id) == ()


def test_only_target_can_reject(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)

    with pytest.raises(NotRequestTargetError):
        harness.pairing.reject(request.id, alice.id)

    assert harness.repos.requests.get(request.id).status is PairingStatus.PENDING


def test_rejected_request_cannot_be_rejected_twice(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.pairing.reject(request.id, bob.id)

    with pytest.raises(RequestNotPendingError):
        harness.pairing.reject(request.id, bob.id)


def test_rejected_request_cannot_be_accepted(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.pairing.reject(request.id, bob.id)

    with pytest.raises(RequestNotPendingError):
        harness.pairing.accept(request.id, bob.id)


def test_rejection_frees_the_pair_for_a_new_request(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    first = harness.pairing.request(alice.id, bob.id)
    harness.pairing.reject(first.id, bob.id)

    second = harness.pairing.request(alice.id, bob.id)

    assert second.id != first.id
    assert second.status is PairingStatus.PENDING


# ---------------------------------------------------------------------- cancel


def test_requester_cancels_request(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)

    cancelled = harness.pairing.cancel(request.id, alice.id)

    assert cancelled.status is PairingStatus.CANCELLED
    assert harness.table_count("pairs") == 0


def test_only_requester_can_cancel(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)

    with pytest.raises(NotRequesterError):
        harness.pairing.cancel(request.id, bob.id)

    assert harness.repos.requests.get(request.id).status is PairingStatus.PENDING


def test_cancelled_request_cannot_be_accepted(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.pairing.cancel(request.id, alice.id)

    with pytest.raises(RequestNotPendingError):
        harness.pairing.accept(request.id, bob.id)


def test_cancelled_request_cannot_be_cancelled_twice(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    harness.pairing.cancel(request.id, alice.id)

    with pytest.raises(RequestNotPendingError):
        harness.pairing.cancel(request.id, alice.id)


def test_cancel_unknown_request(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()

    with pytest.raises(PairRequestNotFoundError):
        harness.pairing.cancel(4242, alice.id)


# ---------------------------------------------------------------------- unpair


def test_member_unpairs_active_pair(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    pair = harness.pair(alice, bob)

    harness.clock.advance(hours=1)
    ended = harness.pairing.unpair(pair.id, alice.id)

    assert ended.status is PairingStatus.UNPAIRED
    assert ended.ended_at == harness.clock.now
    assert harness.pairing.active_pairs(alice.id) == ()
    assert harness.pairing.active_pair(bob.id) is None


def test_either_member_can_unpair(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    pair = harness.pair(alice, bob)

    ended = harness.pairing.unpair(pair.id, bob.id)

    assert ended.status is PairingStatus.UNPAIRED


def test_non_member_cannot_unpair(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    pair = harness.pair(alice, bob)

    with pytest.raises(PairNotFoundError):
        harness.pairing.unpair(pair.id, carol.id)

    assert harness.pairing.active_pair(alice.id).status is PairingStatus.ACTIVE


def test_unpair_unknown_pair(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()

    with pytest.raises(PairNotFoundError):
        harness.pairing.unpair(999_999, alice.id)


def test_unpair_twice_is_rejected(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    pair = harness.pair(alice, bob)
    harness.pairing.unpair(pair.id, alice.id)

    with pytest.raises(PairNotActiveError):
        harness.pairing.unpair(pair.id, bob.id)


def test_unpaired_members_may_pair_again(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    pair = harness.pair(alice, bob)
    harness.pairing.unpair(pair.id, alice.id)

    new_pair = harness.pair(alice, bob)

    assert new_pair.id != pair.id
    assert new_pair.status is PairingStatus.ACTIVE


def test_banned_member_can_still_unpair(build: Callable[..., Harness]) -> None:
    harness = build()
    owner = harness.user(role=UserRole.OWNER)
    alice = harness.user()
    bob = harness.user()
    pair = harness.pair(alice, bob)
    harness.users.ban(owner, alice.id)

    ended = harness.pairing.unpair(pair.id, alice.id)

    assert ended.status is PairingStatus.UNPAIRED


def test_unpair_only_affects_the_targeted_pair(build: Callable[..., Harness]) -> None:
    harness = build(max_pairs_per_user=2)
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    first = harness.pair(alice, bob)
    second = harness.pair(alice, carol)

    harness.pairing.unpair(first.id, alice.id)

    assert harness.pairing.active_pair(alice.id).id == second.id
    assert harness.pairing.active_pair(carol.id).id == second.id
    assert harness.pairing.active_pair(bob.id) is None


# ------------------------------------------------------------- bulk expiry job


def test_expire_stale_requests_bulk_updates(build: Callable[..., Harness]) -> None:
    harness = build(ttl_hours=1)
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    stale = harness.pairing.request(alice.id, bob.id)

    harness.clock.advance(minutes=30)
    fresh = harness.pairing.request(alice.id, carol.id)

    harness.clock.advance(minutes=31)
    changed = harness.pairing.expire_stale_requests()

    assert changed == 1
    assert harness.repos.requests.get(stale.id).status is PairingStatus.EXPIRED
    assert harness.repos.requests.get(fresh.id).status is PairingStatus.PENDING


def test_expire_stale_requests_with_explicit_moment(build: Callable[..., Harness]) -> None:
    harness = build(ttl_hours=1)
    alice = harness.user()
    bob = harness.user()
    request = harness.pairing.request(alice.id, bob.id)

    changed = harness.pairing.expire_stale_requests(harness.clock.now + timedelta(hours=2))

    assert changed == 1
    assert harness.repos.requests.get(request.id).status is PairingStatus.EXPIRED


def test_expired_request_is_not_listed_as_pending(build: Callable[..., Harness]) -> None:
    harness = build(ttl_hours=1)
    alice = harness.user()
    bob = harness.user()
    harness.pairing.request(alice.id, bob.id)

    harness.clock.advance(hours=2)
    harness.pairing.expire_stale_requests()

    assert harness.pairing.incoming_requests(bob.id) == ()
    assert harness.pairing.outgoing_requests(alice.id) == ()


# --------------------------------------------------------------- scoped queries


def test_request_and_pair_lookups_are_member_scoped(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    request = harness.pairing.request(alice.id, bob.id)
    pair = harness.pairing.accept(request.id, bob.id)

    assert harness.pairing.request_for_member(request.id, alice.id).id == request.id
    assert harness.pairing.request_for_member(request.id, bob.id).id == request.id
    with pytest.raises(PairRequestNotFoundError):
        harness.pairing.request_for_member(request.id, carol.id)

    assert harness.pairing.active_pair_for_member(pair.id, alice.id).id == pair.id
    with pytest.raises(PairNotFoundError):
        harness.pairing.active_pair_for_member(pair.id, carol.id)


def test_pending_lists_are_per_user(build: Callable[..., Harness]) -> None:
    harness = build()
    alice = harness.user()
    bob = harness.user()
    carol = harness.user()
    dave = harness.user()
    incoming_for_bob = harness.pairing.request(alice.id, bob.id)
    outgoing_for_carol = harness.pairing.request(carol.id, dave.id)

    assert [r.id for r in harness.pairing.incoming_requests(bob.id)] == [incoming_for_bob.id]
    assert [r.id for r in harness.pairing.outgoing_requests(carol.id)] == [
        outgoing_for_carol.id
    ]
    assert harness.pairing.incoming_requests(alice.id) == ()
    assert harness.pairing.outgoing_requests(dave.id) == ()
    assert harness.pairing.active_pairs(alice.id) == ()


def test_deep_link_invite_pairs_the_first_user_on_the_target_platform(
    build: Callable[..., Harness],
) -> None:
    harness = build()
    telegram = harness.user(platform=Platform.TELEGRAM)
    bale = harness.user(platform=Platform.BALE)

    invite = harness.pairing.create_invite(telegram.id, Platform.BALE)
    pair = harness.pairing.consume_invite(invite.token, bale.id)

    assert pair.is_active
    assert harness.pairing.active_pair(telegram.id) == pair
    assert harness.pairing.active_pair(bale.id) == pair


def test_invite_is_single_use(build: Callable[..., Harness]) -> None:
    harness = build()
    telegram = harness.user(platform=Platform.TELEGRAM)
    first_bale = harness.user(platform=Platform.BALE)
    second_bale = harness.user(platform=Platform.BALE)

    invite = harness.pairing.create_invite(telegram.id, Platform.BALE)
    harness.pairing.consume_invite(invite.token, first_bale.id)

    with pytest.raises(PairInviteInvalidError):
        harness.pairing.consume_invite(invite.token, second_bale.id)


def test_invite_expires(build: Callable[..., Harness]) -> None:
    harness = build()
    telegram = harness.user(platform=Platform.TELEGRAM)
    bale = harness.user(platform=Platform.BALE)
    invite = harness.pairing.create_invite(telegram.id, Platform.BALE)

    harness.clock.advance(minutes=16)

    with pytest.raises(PairInviteInvalidError):
        harness.pairing.consume_invite(invite.token, bale.id)


def test_invite_is_bound_to_the_target_platform(build: Callable[..., Harness]) -> None:
    harness = build()
    telegram = harness.user(platform=Platform.TELEGRAM)
    another_telegram = harness.user(platform=Platform.TELEGRAM)

    invite = harness.pairing.create_invite(telegram.id, Platform.BALE)

    with pytest.raises(PairInviteInvalidError):
        harness.pairing.consume_invite(invite.token, another_telegram.id)


def test_invite_cannot_target_the_inviters_own_platform(
    build: Callable[..., Harness],
) -> None:
    harness = build()
    telegram = harness.user(platform=Platform.TELEGRAM)

    with pytest.raises(SelfPairingError):
        harness.pairing.create_invite(telegram.id, Platform.TELEGRAM)


def test_invite_creator_must_have_a_free_pair_slot(
    build: Callable[..., Harness],
) -> None:
    harness = build(max_pairs_per_user=1)
    telegram = harness.user(platform=Platform.TELEGRAM)
    bale = harness.user(platform=Platform.BALE)
    harness.pair(telegram, bale)

    with pytest.raises(PairLimitReachedError):
        harness.pairing.create_invite(telegram.id, Platform.BALE)


def test_pair_request_object_exposes_helper_properties() -> None:
    created = datetime(2026, 1, 1, tzinfo=UTC)
    request = PairRequest(
        id=1,
        requester_id=1,
        target_id=2,
        status=PairingStatus.PENDING,
        created_at=created,
        expires_at=created + timedelta(hours=1),
    )

    assert request.is_pending
    assert request.is_overdue(created + timedelta(hours=2))
    assert not request.is_overdue(created + timedelta(minutes=59))
