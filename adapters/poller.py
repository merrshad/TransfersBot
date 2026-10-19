"""Long-polling loop shared by both adapters.

The loop owns three concerns that must not leak into the adapters:

* **Offsets** — every successfully processed batch is persisted, so a restart
  resumes exactly where the process stopped and commands are not replayed.
* **Backoff** — rate limits honour ``retry_after``; other errors back off
  exponentially up to a ceiling, and the backoff resets after a good batch.
* **Isolation** — a failing handler must not kill the bot, so handler exceptions
  are logged and the loop continues (and still acknowledges the update, because
  a poison update that is retried forever would block every later update).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol, runtime_checkable

from adapters.base import BaseAdapter, PollResult
from core.errors import MessagingError, RateLimitError
from core.models import Platform, UnifiedMessage

#: Awaited for every parsed inbound message.
MessageHandler = Callable[[UnifiedMessage], Awaitable[None]]


@runtime_checkable
class PollOffsetStorePort(Protocol):
    """Persistence of the last confirmed offset per platform."""

    def get(self, platform: Platform) -> int | None:
        """Return the stored offset or ``None``."""
        ...

    def set(self, platform: Platform, offset: int) -> int:
        """Persist the confirmed offset and return it."""
        ...


async def _wait(delay: float, sleep: Callable[[float], Awaitable[None]]) -> None:
    """Sleep for ``delay`` seconds using the (injectable) sleeper."""
    if delay <= 0:
        return
    await sleep(delay)


async def run_polling(
    adapter: BaseAdapter,
    handler: MessageHandler,
    offsets: PollOffsetStorePort,
    *,
    poll_timeout: int = 25,
    initial_backoff: float = 2.0,
    max_backoff: float = 60.0,
    stop: asyncio.Event | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Poll ``adapter`` forever, handing every parsed message to ``handler``.

    Args:
        adapter: Adapter to poll (its ``platform`` selects the offset row).
        handler: Awaitable called once per inbound message.
        offsets: Offset store, normally ``Repositories.poll_offsets``.
        poll_timeout: Long-poll timeout passed to the platform, in seconds.
        initial_backoff: Delay before the first retry after a failure.
        max_backoff: Ceiling for the exponential backoff.
        stop: Optional event; when set the loop returns after the current poll.
        sleep: Injectable sleep (tests pass a recorder instead of real sleeping).
        logger: Logger override.

    Note:
        Shutdown is deliberately observed between iterations only, so stopping
        can take up to ``poll_timeout`` seconds (or one backoff delay). The loop
        is normally cancelled by the caller, which propagates immediately.
    """
    log = logger or logging.getLogger("bridge.poller")
    wait: Callable[[float], Awaitable[None]] = sleep or asyncio.sleep
    offset = offsets.get(adapter.platform)
    backoff = initial_backoff

    while True:
        if stop is not None and stop.is_set():
            log.info("poller stopped for platform=%s", adapter.platform.value)
            return
        try:
            result: PollResult = await adapter.poll_once(offset, timeout=poll_timeout)
        except RateLimitError as exc:
            delay = max(exc.retry_after, 0.0)
            log.warning(
                "rate limited on platform=%s; waiting %.1fs",
                adapter.platform.value,
                delay,
            )
            await _wait(delay, wait)
            continue
        except MessagingError as exc:
            log.warning(
                "poll failed on platform=%s (%s): %s; retrying in %.1fs",
                adapter.platform.value,
                type(exc).__name__,
                exc.message,
                backoff,
            )
            await _wait(backoff, wait)
            backoff = min(backoff * 2, max_backoff)
            continue
        except asyncio.CancelledError:
            log.info("poller cancelled for platform=%s", adapter.platform.value)
            raise

        backoff = initial_backoff
        for message in result.messages:
            try:
                await handler(message)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the loop must survive any handler bug
                log.exception(
                    "handler failed for an update on platform=%s", adapter.platform.value
                )

        if result.next_offset is not None:
            offsets.set(adapter.platform, result.next_offset)
            offset = result.next_offset


async def run_all(
    jobs: Sequence[
        tuple[BaseAdapter, MessageHandler, PollOffsetStorePort]
    ],
    *,
    poll_timeout: int = 25,
    stop: asyncio.Event | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Run one polling loop per adapter until ``stop`` is set."""
    await asyncio.gather(
        *(
            run_polling(
                adapter,
                handler,
                offsets,
                poll_timeout=poll_timeout,
                stop=stop,
                logger=logger,
            )
            for adapter, handler, offsets in jobs
        )
    )


__all__ = [
    "MessageHandler",
    "PollOffsetStorePort",
    "run_all",
    "run_polling",
]
