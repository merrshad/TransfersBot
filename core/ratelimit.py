"""In-memory send-rate limiting with an injectable clock.

Three independent limits are enforced before every outbound delivery unit
(one text message, one attachment or one album):

* **global per platform** — total sends per rolling second;
* **per chat** — sends to one platform chat per rolling second;
* **per user** — messages a source user may relay per rolling minute (the
  anti-spam limit; configurable, and it can either queue or reject).

The limiter never sleeps. A denied delivery is rescheduled by the durable
outbox, which is what lets the worker stay responsive and lets tests advance a
fake clock instead of waiting. State is deliberately in-memory: after a
restart the queue re-establishes a safe pace from an empty window.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from core.models import Platform

GLOBAL_WINDOW = timedelta(seconds=1)
CHAT_WINDOW = timedelta(seconds=1)
USER_WINDOW = timedelta(minutes=1)

#: Conservative defaults, below the documented platform ceilings. Operators can
#: tighten or disable each limit through settings.
DEFAULT_GLOBAL_PER_SECOND: Mapping[Platform, int] = {
    Platform.TELEGRAM: 25,
    Platform.BALE: 20,
}
DEFAULT_CHAT_PER_SECOND: Mapping[Platform, float] = {
    Platform.TELEGRAM: 1.0,
    Platform.BALE: 1.0,
}
DEFAULT_USER_PER_MINUTE = 30

#: Scopes named in a decision (also used for logging; never user content).
SCOPE_PLATFORM = "platform"
SCOPE_CHAT = "chat"
SCOPE_USER = "user"


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """Outcome of one rate-limit check.

    Attributes:
        allowed: Whether the send may proceed now.
        retry_after: Minimum seconds to wait when denied.
        scope: Which limit denied the send (``platform``/``chat``/``user``).
    """

    allowed: bool
    retry_after: float = 0.0
    scope: str | None = None


ALLOWED = RateLimitDecision(allowed=True)


class RateLimiter:
    """Sliding-window limiter for platform, chat and user scopes.

    Args:
        global_per_second: Sends allowed per platform per second (0 disables).
        per_chat_per_second: Sends allowed to one chat per second (0 disables).
        per_user_per_minute: Relayed messages allowed per source user per
            minute (0 disables).
        clock: Injectable UTC clock.
        enabled: Master switch; when false every check passes immediately.
    """

    def __init__(
        self,
        *,
        global_per_second: Mapping[Platform, int] | None = None,
        per_chat_per_second: Mapping[Platform, float] | None = None,
        per_user_per_minute: int = DEFAULT_USER_PER_MINUTE,
        clock: Callable[[], datetime] | None = None,
        enabled: bool = True,
    ) -> None:
        self._global_limits = dict(
            DEFAULT_GLOBAL_PER_SECOND if global_per_second is None else global_per_second
        )
        self._chat_limits = dict(
            DEFAULT_CHAT_PER_SECOND if per_chat_per_second is None else per_chat_per_second
        )
        if per_user_per_minute < 0:
            raise ValueError("per_user_per_minute must be >= 0.")
        self._user_limit = per_user_per_minute
        self._clock = clock or (lambda: datetime.now(UTC))
        self._enabled = enabled
        self._platform_events: dict[Platform, list[datetime]] = defaultdict(list)
        self._chat_events: dict[tuple[Platform, int], list[datetime]] = defaultdict(list)
        self._user_events: dict[int, list[datetime]] = defaultdict(list)

    def check(
        self,
        *,
        platform: Platform,
        chat_id: int,
        user_id: int | None = None,
        now: datetime | None = None,
        cost: int = 1,
    ) -> RateLimitDecision:
        """Check every scope and reserve capacity when the send is allowed.

        ``cost`` lets an album count as one platform/chat call while optionally
        counting as several user messages. A denied check reserves nothing.
        """
        if not self._enabled or cost < 1:
            return ALLOWED
        moment = now or self._clock()
        denied = False
        worst_wait = 0.0
        worst_scope: str | None = None

        checks: list[tuple[str, list[datetime], timedelta, float]] = []
        global_limit = float(self._global_limits.get(platform, 0) or 0)
        if global_limit > 0:
            checks.append(
                (
                    SCOPE_PLATFORM,
                    self._platform_events[platform],
                    GLOBAL_WINDOW,
                    global_limit,
                )
            )
        chat_limit = float(self._chat_limits.get(platform, 0) or 0)
        if chat_limit > 0:
            checks.append(
                (
                    SCOPE_CHAT,
                    self._chat_events[(platform, chat_id)],
                    CHAT_WINDOW,
                    chat_limit,
                )
            )
        if user_id is not None and self._user_limit > 0:
            checks.append(
                (SCOPE_USER, self._user_events[user_id], USER_WINDOW, float(self._user_limit))
            )

        for scope, events, window, limit in checks:
            self._prune(events, moment - window)
            # A positive fractional limit still means "one at a time"; never
            # truncate it to zero and then index an empty event list.
            effective_limit = max(int(limit), 1)
            overflow = len(events) + cost - effective_limit
            if overflow <= 0:
                continue
            denied = True
            # Waiting for the ``overflow`` oldest events to leave the window
            # makes room for this send.
            expiry = events[overflow - 1] + window
            wait = max((expiry - moment).total_seconds(), 0.0)
            if worst_scope is None or wait > worst_wait:
                worst_wait = wait
                worst_scope = scope

        if denied:
            return RateLimitDecision(
                allowed=False, retry_after=worst_wait, scope=worst_scope
            )

        for _, events, _, _ in checks:
            events.extend([moment] * cost)
        return ALLOWED

    def reset(self) -> None:
        """Forget every recorded send (used when tests restart a runtime)."""
        self._platform_events.clear()
        self._chat_events.clear()
        self._user_events.clear()

    @staticmethod
    def _prune(events: list[datetime], cutoff: datetime) -> None:
        """Drop timestamps that no longer count towards the window."""
        if events and events[0] <= cutoff:
            del events[: next((i for i, e in enumerate(events) if e > cutoff), len(events))]


__all__ = [
    "ALLOWED",
    "CHAT_WINDOW",
    "DEFAULT_CHAT_PER_SECOND",
    "DEFAULT_GLOBAL_PER_SECOND",
    "DEFAULT_USER_PER_MINUTE",
    "GLOBAL_WINDOW",
    "RateLimitDecision",
    "RateLimiter",
    "SCOPE_CHAT",
    "SCOPE_PLATFORM",
    "SCOPE_USER",
    "USER_WINDOW",
]
