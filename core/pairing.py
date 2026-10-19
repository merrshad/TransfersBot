"""Pairing state machine and the rules that guard every transition.

A *pair request* is created by a user and answered by the target. Accepting a
request materialises an *active pair*; unpairing ends it. Both entities share
one status vocabulary (:class:`PairingStatus`) so the lifecycle is a single,
explicit state machine:

``pending``   → ``active`` | ``rejected`` | ``expired`` | ``cancelled``
``active``    → ``unpaired``
``rejected`` / ``expired`` / ``cancelled`` / ``unpaired`` → (terminal)

Invalid transitions can be probed with :func:`can_transition` and are enforced
by :func:`ensure_transition`, which the service calls before every write.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Protocol, runtime_checkable

from core.errors import (
    AlreadyPairedError,
    DuplicatePairRequestError,
    InvalidTransitionError,
    NotRequesterError,
    NotRequestTargetError,
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
from core.ports import TransactionManager
from core.users import User, UserRepositoryPort


class PairingStatus(str, Enum):
    """Status of both pairing requests and pairs."""

    PENDING = "pending"
    ACTIVE = "active"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    UNPAIRED = "unpaired"


REQUEST_TRANSITIONS: Mapping[PairingStatus, frozenset[PairingStatus]] = {
    PairingStatus.PENDING: frozenset(
        {
            PairingStatus.ACTIVE,
            PairingStatus.REJECTED,
            PairingStatus.EXPIRED,
            PairingStatus.CANCELLED,
        }
    ),
    PairingStatus.ACTIVE: frozenset(),
    PairingStatus.REJECTED: frozenset(),
    PairingStatus.EXPIRED: frozenset(),
    PairingStatus.CANCELLED: frozenset(),
    PairingStatus.UNPAIRED: frozenset(),
}

PAIR_TRANSITIONS: Mapping[PairingStatus, frozenset[PairingStatus]] = {
    PairingStatus.ACTIVE: frozenset({PairingStatus.UNPAIRED}),
    PairingStatus.PENDING: frozenset(),
    PairingStatus.REJECTED: frozenset(),
    PairingStatus.EXPIRED: frozenset(),
    PairingStatus.CANCELLED: frozenset(),
    PairingStatus.UNPAIRED: frozenset(),
}

TERMINAL_STATUSES: frozenset[PairingStatus] = frozenset(
    {
        PairingStatus.REJECTED,
        PairingStatus.EXPIRED,
        PairingStatus.CANCELLED,
        PairingStatus.UNPAIRED,
    }
)


def can_transition(
    current: PairingStatus,
    new: PairingStatus,
    *,
    transitions: Mapping[PairingStatus, frozenset[PairingStatus]] = REQUEST_TRANSITIONS,
) -> bool:
    """Return whether ``current`` may legally move to ``new``."""
    return new in transitions.get(current, frozenset())


def ensure_transition(
    current: PairingStatus,
    new: PairingStatus,
    *,
    transitions: Mapping[PairingStatus, frozenset[PairingStatus]] = REQUEST_TRANSITIONS,
) -> None:
    """Raise :class:`InvalidTransitionError` unless the transition is allowed."""
    if not can_transition(current, new, transitions=transitions):
        raise InvalidTransitionError(
            f"تغییر وضعیت از «{current.value}» به «{new.value}» مجاز نیست."
        )


@dataclass(frozen=True, slots=True)
class Pair:
    """An active or ended link between exactly two users."""

    id: int
    user_a_id: int
    user_b_id: int
    status: PairingStatus
    created_at: datetime
    ended_at: datetime | None = None

    def includes(self, user_id: int) -> bool:
        """Whether ``user_id`` is one of the two members."""
        return user_id in (self.user_a_id, self.user_b_id)

    def other(self, user_id: int) -> int:
        """Return the partner of ``user_id``.

        Raises:
            PairNotFoundError: If ``user_id`` is not a member of the pair.
        """
        if user_id == self.user_a_id:
            return self.user_b_id
        if user_id == self.user_b_id:
            return self.user_a_id
        raise PairNotFoundError()

    @property
    def is_active(self) -> bool:
        """Whether the pair is still bridged."""
        return self.status is PairingStatus.ACTIVE


@dataclass(frozen=True, slots=True)
class PairRequest:
    """A pending or answered pairing request between two users."""

    id: int
    requester_id: int
    target_id: int
    status: PairingStatus
    created_at: datetime
    expires_at: datetime
    pair_id: int | None = None

    @property
    def is_pending(self) -> bool:
        """Whether the request still waits for the target's answer."""
        return self.status is PairingStatus.PENDING

    def is_overdue(self, now: datetime) -> bool:
        """Whether the request's TTL elapsed at ``now``."""
        return now >= self.expires_at


@dataclass(frozen=True, slots=True)
class PairingOverview:
    """Aggregate pairing counters for owner-facing reporting (no content)."""

    active_pairs: int
    pending_requests: int


@runtime_checkable
class PairRepositoryPort(Protocol):
    """Persistence operations required by :class:`PairingService`."""

    def create(
        self,
        *,
        user_a_id: int,
        user_b_id: int,
        status: PairingStatus,
        created_at: datetime,
    ) -> Pair:
        """Insert a pair row with canonical member ordering."""
        ...

    def get(self, pair_id: int) -> Pair | None:
        """Fetch a pair by id (internal use)."""
        ...

    def get_for_user(self, pair_id: int, user_id: int) -> Pair | None:
        """Fetch a pair only if ``user_id`` is one of its members."""
        ...

    def find_active_between(self, user_a_id: int, user_b_id: int) -> Pair | None:
        """The active pair between two users, if any."""
        ...

    def list_active_for_user(self, user_id: int) -> tuple[Pair, ...]:
        """All active pairs a user is a member of."""
        ...

    def count_active_for_user(self, user_id: int) -> int:
        """Number of active pairs a user is a member of."""
        ...

    def count_active(self) -> int:
        """Total number of active pairs (owner-facing aggregate)."""
        ...

    def set_status(
        self, pair_id: int, status: PairingStatus, *, ended_at: datetime | None = None
    ) -> Pair | None:
        """Update a pair's status; ``None`` when the pair does not exist."""
        ...


@runtime_checkable
class PairRequestRepositoryPort(Protocol):
    """Persistence operations required by :class:`PairingService`."""

    def create(
        self,
        *,
        requester_id: int,
        target_id: int,
        created_at: datetime,
        expires_at: datetime,
    ) -> PairRequest:
        """Insert a pending request."""
        ...

    def get(self, request_id: int) -> PairRequest | None:
        """Fetch a request by id (internal use)."""
        ...

    def get_for_user(self, request_id: int, user_id: int) -> PairRequest | None:
        """Fetch a request only if the user sent or received it."""
        ...

    def find_pending_between(self, user_a_id: int, user_b_id: int) -> PairRequest | None:
        """A pending request between two users, in either direction."""
        ...

    def list_pending_incoming(self, user_id: int) -> tuple[PairRequest, ...]:
        """Requests awaiting this user's answer."""
        ...

    def list_pending_outgoing(self, user_id: int) -> tuple[PairRequest, ...]:
        """Requests this user sent that are still pending."""
        ...

    def set_status(
        self, request_id: int, status: PairingStatus, *, pair_id: int | None = None
    ) -> PairRequest | None:
        """Update a request's status; ``None`` when it does not exist."""
        ...

    def count_pending(self) -> int:
        """Total number of pending requests (owner-facing aggregate)."""
        ...

    def expire_overdue(self, now: datetime) -> int:
        """Bulk-expire overdue pending requests; returns rows changed."""
        ...


class PairingService:
    """Applies every pairing rule on top of the repositories.

    Args:
        users: User repository used for existence/ban checks.
        pairs: Pair repository.
        requests: Pair-request repository.
        tx: Transaction manager sharing the connection with the repositories.
        max_pairs_per_user: Maximum simultaneously active pairs per user.
        request_ttl: Lifetime of a freshly created pending request.
        clock: Injectable UTC clock, mainly for deterministic tests.
    """

    def __init__(
        self,
        users: UserRepositoryPort,
        pairs: PairRepositoryPort,
        requests: PairRequestRepositoryPort,
        tx: TransactionManager,
        *,
        max_pairs_per_user: int = 1,
        request_ttl: timedelta = timedelta(hours=24),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if max_pairs_per_user < 1:
            raise ValueError("max_pairs_per_user must be >= 1.")
        if request_ttl <= timedelta(0):
            raise ValueError("request_ttl must be positive.")
        self._users = users
        self._pairs = pairs
        self._requests = requests
        self._tx = tx
        self._max_pairs_per_user = max_pairs_per_user
        self._request_ttl = request_ttl
        self._clock = clock or (lambda: datetime.now(UTC))

    @property
    def max_pairs_per_user(self) -> int:
        """Configured active-pair cap per user."""
        return self._max_pairs_per_user

    @property
    def request_ttl(self) -> timedelta:
        """Configured lifetime of a pending request."""
        return self._request_ttl

    # ------------------------------------------------------------------ rules

    def _require_active_user(self, user_id: int, *, role: str = "کاربر") -> User:
        """Return an existing, non-banned user or raise a domain error."""
        user = self._users.get_by_id(user_id)
        if user is None:
            raise UserNotFoundError(f"{role} موردنظر پیدا نشد.")
        if user.is_banned:
            raise UserBannedError(f"{role} موردنظر مسدود شده است.")
        return user

    def _ensure_capacity(self, user: User) -> None:
        """Ensure ``user`` has a free active-pair slot."""
        if self._pairs.count_active_for_user(user.id) >= self._max_pairs_per_user:
            raise PairLimitReachedError(
                "تعداد جفت‌های فعال این کاربر به سقف مجاز رسیده است."
            )

    def _expire_if_overdue(self, request: PairRequest, now: datetime) -> bool:
        """Mark a pending request as expired when its TTL has passed."""
        if request.is_pending and request.is_overdue(now):
            ensure_transition(request.status, PairingStatus.EXPIRED)
            self._requests.set_status(request.id, PairingStatus.EXPIRED)
            return True
        return False

    def _expire_overdue_request(self, request_id: int) -> None:
        """Persist ``pending`` → ``expired`` in its own committed transaction.

        This runs *before* the transaction that may raise
        :class:`RequestExpiredError`; otherwise the expiry write would be rolled
        back together with the failed attempt and the row would stay pending.
        """
        now = self._clock()
        with self._tx.transaction():
            request = self._requests.get(request_id)
            if request is None or not request.is_pending or not request.is_overdue(now):
                return
            ensure_transition(request.status, PairingStatus.EXPIRED)
            self._requests.set_status(request.id, PairingStatus.EXPIRED)

    def _require_actionable(self, request: PairRequest, now: datetime) -> None:
        """Ensure a request may still be answered by target/requester (read-only).

        Raises:
            RequestExpiredError: If the TTL passed.
            RequestNotPendingError: If it was already accepted/rejected/etc.
        """
        if request.status is PairingStatus.EXPIRED or (
            request.is_pending and request.is_overdue(now)
        ):
            raise RequestExpiredError()
        if not request.is_pending:
            raise RequestNotPendingError()

    # ------------------------------------------------------------- mutations

    def request(self, requester_id: int, target_id: int) -> PairRequest:
        """Create a pending pairing request from ``requester_id`` to ``target_id``.

        Rules enforced:
            * both users must exist and be active (banned users are refused);
            * self-pairing is refused;
            * an existing active pair is refused;
            * a pending request in either direction is refused (unless expired);
            * neither side may exceed ``MAX_PAIRS_PER_USER`` active pairs.

        Returns:
            The newly created pending :class:`PairRequest`.
        """
        now = self._clock()
        if requester_id == target_id:
            raise SelfPairingError()

        with self._tx.transaction():
            requester = self._require_active_user(requester_id, role="فرستنده")
            target = self._require_active_user(target_id, role="گیرنده")

            if self._pairs.find_active_between(requester.id, target.id) is not None:
                raise AlreadyPairedError()

            pending = self._requests.find_pending_between(requester.id, target.id)
            if pending is not None:
                if not self._expire_if_overdue(pending, now):
                    raise DuplicatePairRequestError()

            self._ensure_capacity(requester)
            self._ensure_capacity(target)

            return self._requests.create(
                requester_id=requester.id,
                target_id=target.id,
                created_at=now,
                expires_at=now + self._request_ttl,
            )

    def accept(self, request_id: int, actor_id: int) -> Pair:
        """Accept a pending request, creating an active pair.

        Only the request target may accept, and only while the request is
        pending and unexpired.

        Returns:
            The newly created active :class:`Pair`.
        """
        now = self._clock()
        self._expire_overdue_request(request_id)
        with self._tx.transaction():
            request = self._requests.get(request_id)
            if request is None:
                raise PairRequestNotFoundError()
            if actor_id != request.target_id:
                raise NotRequestTargetError()

            actor = self._require_active_user(actor_id, role="گیرنده")
            self._require_actionable(request, now)

            requester = self._require_active_user(request.requester_id, role="فرستنده")

            if self._pairs.find_active_between(requester.id, actor.id) is not None:
                raise AlreadyPairedError()

            self._ensure_capacity(requester)
            self._ensure_capacity(actor)

            ensure_transition(request.status, PairingStatus.ACTIVE)
            pair = self._pairs.create(
                user_a_id=min(requester.id, actor.id),
                user_b_id=max(requester.id, actor.id),
                status=PairingStatus.ACTIVE,
                created_at=now,
            )
            self._requests.set_status(request.id, PairingStatus.ACTIVE, pair_id=pair.id)
            return pair

    def reject(self, request_id: int, actor_id: int) -> PairRequest:
        """Reject a pending request. Only the target may reject."""
        now = self._clock()
        self._expire_overdue_request(request_id)
        with self._tx.transaction():
            request = self._requests.get(request_id)
            if request is None:
                raise PairRequestNotFoundError()
            if actor_id != request.target_id:
                raise NotRequestTargetError()
            self._require_actionable(request, now)

            ensure_transition(request.status, PairingStatus.REJECTED)
            updated = self._requests.set_status(request.id, PairingStatus.REJECTED)
        if updated is None:  # pragma: no cover - request existed moments ago
            raise PairRequestNotFoundError()
        return updated

    def cancel(self, request_id: int, actor_id: int) -> PairRequest:
        """Cancel a pending request. Only the requester may cancel."""
        now = self._clock()
        self._expire_overdue_request(request_id)
        with self._tx.transaction():
            request = self._requests.get(request_id)
            if request is None:
                raise PairRequestNotFoundError()
            if actor_id != request.requester_id:
                raise NotRequesterError()
            self._require_actionable(request, now)

            ensure_transition(request.status, PairingStatus.CANCELLED)
            updated = self._requests.set_status(request.id, PairingStatus.CANCELLED)
        if updated is None:  # pragma: no cover - request existed moments ago
            raise PairRequestNotFoundError()
        return updated

    def unpair(self, pair_id: int, actor_id: int) -> Pair:
        """End an active pair. Only one of its two members may call this."""
        now = self._clock()
        with self._tx.transaction():
            pair = self._pairs.get_for_user(pair_id, actor_id)
            if pair is None:
                raise PairNotFoundError()
            if not pair.is_active:
                raise PairNotActiveError()

            ensure_transition(pair.status, PairingStatus.UNPAIRED, transitions=PAIR_TRANSITIONS)
            updated = self._pairs.set_status(
                pair.id, PairingStatus.UNPAIRED, ended_at=now
            )
        if updated is None:  # pragma: no cover - pair existed moments ago
            raise PairNotFoundError()
        return updated

    def abort(self, request_id: int, *, reason: str) -> PairRequest:
        """Cancel a still-pending request on behalf of the system.

        Used when a request cannot be delivered to its target (for example the
        target blocked the bot). Only the request's own exit transition to
        ``cancelled`` is reused, so the state vocabulary is unchanged.

        Args:
            request_id: Request to cancel.
            reason: Short, non-user-authored explanation for logs.

        Raises:
            PairRequestNotFoundError: The request does not exist.
            RequestNotPendingError: It was already answered.
            RequestExpiredError: Its TTL had already passed.
        """
        now = self._clock()
        self._expire_overdue_request(request_id)
        with self._tx.transaction():
            request = self._requests.get(request_id)
            if request is None:
                raise PairRequestNotFoundError()
            self._require_actionable(request, now)
            ensure_transition(request.status, PairingStatus.CANCELLED)
            updated = self._requests.set_status(request.id, PairingStatus.CANCELLED)
        if updated is None:  # pragma: no cover - request existed moments ago
            raise PairRequestNotFoundError()
        return updated

    def expire_stale_requests(self, now: datetime | None = None) -> int:
        """Bulk-expire overdue pending requests and return how many changed.

        Intended for a periodic background task; it is not required for
        correctness because ``accept``/``reject``/``cancel`` expire lazily.
        """
        moment = now or self._clock()
        with self._tx.transaction():
            return self._requests.expire_overdue(moment)

    # ---------------------------------------------------------------- queries

    def active_pairs(self, user_id: int) -> tuple[Pair, ...]:
        """All active pairs of a single user."""
        return self._pairs.list_active_for_user(user_id)

    def active_pair(self, user_id: int) -> Pair | None:
        """The user's active pair, or ``None`` when they have none."""
        pairs = self._pairs.list_active_for_user(user_id)
        return pairs[0] if pairs else None

    def active_pair_for_member(self, pair_id: int, user_id: int) -> Pair:
        """Fetch a pair that ``user_id`` belongs to.

        Raises:
            PairNotFoundError: If the pair does not exist or the user is not a
                member (non-members must not learn that the pair exists).
        """
        pair = self._pairs.get_for_user(pair_id, user_id)
        if pair is None:
            raise PairNotFoundError()
        return pair

    def overview(self) -> PairingOverview:
        """Aggregate counters (active pairs, pending requests)."""
        return PairingOverview(
            active_pairs=self._pairs.count_active(),
            pending_requests=self._requests.count_pending(),
        )

    def incoming_requests(self, user_id: int) -> tuple[PairRequest, ...]:
        """Pending requests waiting for this user's answer."""
        return self._requests.list_pending_incoming(user_id)

    def outgoing_requests(self, user_id: int) -> tuple[PairRequest, ...]:
        """Pending requests this user sent and may still cancel."""
        return self._requests.list_pending_outgoing(user_id)

    def request_for_member(self, request_id: int, user_id: int) -> PairRequest:
        """Fetch a request that ``user_id`` sent or received.

        Raises:
            PairRequestNotFoundError: If the request is unknown to the user.
        """
        request = self._requests.get_for_user(request_id, user_id)
        if request is None:
            raise PairRequestNotFoundError()
        return request


__all__ = [
    "PAIR_TRANSITIONS",
    "REQUEST_TRANSITIONS",
    "TERMINAL_STATUSES",
    "Pair",
    "PairRepositoryPort",
    "PairingOverview",
    "PairRequest",
    "PairRequestRepositoryPort",
    "PairingService",
    "PairingStatus",
    "can_transition",
    "ensure_transition",
]
