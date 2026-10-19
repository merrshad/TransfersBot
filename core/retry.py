"""Retry policy: exponential backoff with jitter and a hard attempt cap.

The policy is a pure value object so it can be tested with a fake clock and an
injected random value. The worker asks it for the delay after a failed attempt;
the queue stores the resulting ``next_attempt_at`` in SQLite, which means a
restart resumes the same schedule instead of restarting from attempt one.

Delay table (base 10s, factor 2, cap 5 min, jitter ±20% by default):

| failed attempt | nominal delay | jittered range |
|----------------|---------------|----------------|
| 1              | 10 s          | 8–12 s         |
| 2              | 20 s          | 16–24 s        |
| 3              | 40 s          | 32–48 s        |
| 4              | 80 s          | 64–96 s        |
| 5              | 160 s         | 128–192 s      |
| 6+             | 300 s (cap)   | 240–360 s      |

A platform ``retry_after`` (Telegram/Bale HTTP 429) is authoritative: the
policy returns exactly that delay, without jitter, so the bot never retries
before the platform allows it.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta

from core.errors import (
    ApiError,
    DeliveryError,
    MessagingError,
    RateLimitError,
    TransportError,
)

DEFAULT_BASE_DELAY = timedelta(seconds=10)
DEFAULT_MAX_DELAY = timedelta(minutes=5)
DEFAULT_JITTER_RATIO = 0.2
DEFAULT_MAX_ATTEMPTS = 5


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential backoff with multiplicative jitter.

    Args:
        base_delay: Delay after the first failed attempt.
        factor: Multiplier applied per additional attempt.
        max_delay: Ceiling for the nominal delay (before jitter).
        jitter_ratio: Fraction of the nominal delay to randomise by, in
            ``[0, 1]``. ``0`` disables jitter (useful in deterministic tests).
        max_attempts: Total failed attempts allowed before an entry is marked
            ``failed`` terminally.
    """

    base_delay: timedelta = DEFAULT_BASE_DELAY
    factor: float = 2.0
    max_delay: timedelta = DEFAULT_MAX_DELAY
    jitter_ratio: float = DEFAULT_JITTER_RATIO
    max_attempts: int = DEFAULT_MAX_ATTEMPTS

    def __post_init__(self) -> None:
        if self.base_delay <= timedelta(0):
            raise ValueError("base_delay must be positive.")
        if self.factor < 1:
            raise ValueError("factor must be >= 1.")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay.")
        if not 0.0 <= self.jitter_ratio <= 1.0:
            raise ValueError("jitter_ratio must be between 0 and 1.")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1.")

    def nominal_delay(self, attempt: int) -> timedelta:
        """Un-jittered delay after failed attempt number ``attempt`` (1-based)."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1.")
        exponent = attempt - 1
        seconds = self.base_delay.total_seconds() * (self.factor**exponent)
        return min(timedelta(seconds=seconds), self.max_delay)

    def delay_for(
        self,
        attempt: int,
        *,
        retry_after: float | None = None,
        random_value: float | None = None,
    ) -> timedelta:
        """Delay before the next attempt.

        Args:
            attempt: Number of the attempt that just failed (1-based).
            retry_after: Platform-provided minimum wait in seconds. When set it
                wins outright (429 responses), and no jitter is added.
            random_value: Uniform sample in ``[0, 1)``; injected by tests so the
                schedule is reproducible. Defaults to :func:`random.random`.
        """
        if retry_after is not None:
            return timedelta(seconds=max(float(retry_after), 0.0))
        nominal = self.nominal_delay(attempt)
        if self.jitter_ratio == 0:
            return nominal
        sample = random.random() if random_value is None else random_value
        # Clamp the sample so an injected value outside [0, 1) cannot turn a
        # backoff into an immediate retry (or a negative delay).
        sample = min(max(sample, 0.0), 1.0)
        factor = 1.0 + (2.0 * sample - 1.0) * self.jitter_ratio
        return timedelta(seconds=max(nominal.total_seconds() * factor, 0.0))


def is_retryable(error: MessagingError) -> bool:
    """Whether a messaging failure is worth trying again later.

    The explicit contract for this phase: network/transport failures, rate
    limits and HTTP 5xx are retryable; an unreachable recipient is permanent;
    other 4xx API errors are terminal (a malformed request will not fix itself).
    """
    if isinstance(error, (RateLimitError, TransportError)):
        return True
    if isinstance(error, DeliveryError):
        return False
    if isinstance(error, ApiError):
        return error.status_code is not None and error.status_code >= 500
    return not error.permanent


__all__ = [
    "DEFAULT_BASE_DELAY",
    "DEFAULT_JITTER_RATIO",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_DELAY",
    "RetryPolicy",
    "is_retryable",
]
