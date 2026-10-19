"""Tests for :mod:`adapters.poller` — deterministic, no sleeping, no network."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from typing import Any

import pytest

from adapters.base import BaseAdapter, PollResult
from adapters.poller import run_all, run_polling
from core.errors import RateLimitError, TransportError
from core.models import Platform, SentMessage, UnifiedMessage
from storage.db import Repositories
from tests.conftest import FIXED_NOW


def unified(text: str, *, platform: Platform = Platform.TELEGRAM, message_id: int = 1) -> UnifiedMessage:
    """Minimal inbound message for handler assertions."""
    return UnifiedMessage(
        source=platform,
        sender_id=5001,
        chat_id=5001,
        message_id=message_id,
        text=text,
        timestamp=FIXED_NOW,
    )


def batch(
    *messages: UnifiedMessage,
    next_offset: int | None = None,
    received: int | None = None,
) -> PollResult:
    """Scripted poll result."""
    return PollResult(
        messages=tuple(messages),
        next_offset=next_offset,
        received=len(messages) if received is None else received,
    )


class ScriptedAdapter(BaseAdapter):
    """Adapter replaying a script of results/errors and recording its calls."""

    def __init__(
        self,
        script: Sequence[Any],
        *,
        platform: Platform = Platform.TELEGRAM,
        on_poll: Callable[[], None] | None = None,
        block: bool = False,
    ) -> None:
        self._script = list(script)
        self._platform = platform
        self._on_poll = on_poll
        self._block = block
        self.poll_offsets: list[int | None] = []
        self.timeouts: list[int] = []
        self.closed = False

    @property
    def platform(self) -> Platform:
        return self._platform

    async def poll_once(self, offset: int | None, *, timeout: int) -> PollResult:
        self.poll_offsets.append(offset)
        self.timeouts.append(timeout)
        if self._block:
            await asyncio.sleep(30)
        # A real adapter always suspends on network I/O; yield control so that
        # sibling poll loops scheduled by run_all() get a chance to run.
        # Without this the fake returns synchronously, the caller's loop spins
        # without yielding, and the other task is starved forever.
        await asyncio.sleep(0)
        item: Any = self._script.pop(0) if self._script else batch()
        if self._on_poll is not None:
            self._on_poll()
        if isinstance(item, Exception):
            raise item
        assert isinstance(item, PollResult)
        return item

    async def send_message(
        self, chat_id: int, text: str, *, reply_to_message_id: int | None = None
    ) -> SentMessage:  # pragma: no cover - poller never sends
        raise NotImplementedError

    async def close(self) -> None:
        self.closed = True


class SleepRecorder:
    """Records (and optionally stops the loop after) requested sleeps."""

    def __init__(self, *, stop_after: int | None = None, stop: asyncio.Event | None = None) -> None:
        self.delays: list[float] = []
        self._stop_after = stop_after
        self._stop = stop

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        if self._stop_after is not None and len(self.delays) >= self._stop_after:
            assert self._stop is not None
            self._stop.set()


async def test_polling_persists_offsets_and_resumes(
    repos: Repositories,
) -> None:
    stop = asyncio.Event()
    handled: list[str] = []

    def handler_done() -> None:
        if len(handled) == 2:
            stop.set()

    adapter = ScriptedAdapter(
        [
            batch(unified("one", message_id=1), next_offset=11, received=1),
            batch(unified("two", message_id=2), next_offset=12, received=1),
        ]
    )

    async def handler(message: UnifiedMessage) -> None:
        handled.append(message.text or "")
        handler_done()

    repos.poll_offsets.set(Platform.TELEGRAM, 5)
    await run_polling(adapter, handler, repos.poll_offsets, poll_timeout=7, stop=stop)

    assert handled == ["one", "two"]
    assert adapter.poll_offsets == [5, 11], "the stored offset must be resumed and advanced"
    assert adapter.timeouts == [7, 7]
    assert repos.poll_offsets.get(Platform.TELEGRAM) == 12


async def test_empty_batch_does_not_overwrite_the_stored_offset(
    repos: Repositories,
) -> None:
    stop = asyncio.Event()
    calls = {"count": 0}

    def on_poll() -> None:
        calls["count"] += 1
        if calls["count"] >= 1:
            stop.set()

    adapter = ScriptedAdapter([batch(next_offset=None, received=0)], on_poll=on_poll)
    repos.poll_offsets.set(Platform.TELEGRAM, 5)

    async def handler(message: UnifiedMessage) -> None:  # pragma: no cover
        raise AssertionError("no message expected")

    await run_polling(adapter, handler, repos.poll_offsets, stop=stop)

    assert repos.poll_offsets.get(Platform.TELEGRAM) == 5


async def test_rate_limit_honours_retry_after(repos: Repositories) -> None:
    stop = asyncio.Event()
    calls = {"count": 0}

    def on_poll() -> None:
        calls["count"] += 1
        if calls["count"] >= 2:
            stop.set()

    sleeper = SleepRecorder()
    adapter = ScriptedAdapter(
        [RateLimitError("slow down", retry_after=3.5), batch(unified("ok"), next_offset=2)],
        on_poll=on_poll,
    )
    handled: list[str] = []

    async def handler(message: UnifiedMessage) -> None:
        handled.append(message.text or "")

    await run_polling(
        adapter, handler, repos.poll_offsets, stop=stop, sleep=sleeper
    )

    assert sleeper.delays == [3.5]
    assert handled == ["ok"]
    assert repos.poll_offsets.get(Platform.TELEGRAM) == 2


async def test_error_backoff_grows_then_resets_after_success(
    repos: Repositories,
) -> None:
    stop = asyncio.Event()
    calls = {"count": 0}

    def on_poll() -> None:
        calls["count"] += 1

    sleeper = SleepRecorder(stop_after=3, stop=stop)
    adapter = ScriptedAdapter(
        [
            TransportError("down"),
            TransportError("down again"),
            batch(next_offset=7, received=0),
            TransportError("down once more"),
            batch(next_offset=8, received=0),
        ],
        on_poll=on_poll,
    )

    async def handler(message: UnifiedMessage) -> None:  # pragma: no cover
        raise AssertionError("no message expected")

    await run_polling(adapter, handler, repos.poll_offsets, stop=stop, sleep=sleeper)

    assert sleeper.delays == [2.0, 4.0, 2.0], "backoff must reset after a good batch"


async def test_handler_exception_is_logged_and_does_not_kill_the_loop(
    repos: Repositories, caplog: pytest.LogCaptureFixture
) -> None:
    stop = asyncio.Event()
    seen: list[str] = []

    adapter = ScriptedAdapter(
        [batch(unified("first"), unified("second"), next_offset=99, received=2)],
    )

    async def handler(message: UnifiedMessage) -> None:
        seen.append(message.text or "")
        if message.text == "first":
            raise RuntimeError("handler bug")
        stop.set()

    with caplog.at_level(logging.ERROR, logger="bridge.poller"):
        await run_polling(adapter, handler, repos.poll_offsets, stop=stop)

    assert seen == ["first", "second"], "the loop must continue after a handler failure"
    assert repos.poll_offsets.get(Platform.TELEGRAM) == 99
    assert "handler failed" in caplog.text
    assert "handler bug" in caplog.text


async def test_stop_event_set_before_start_prevents_polling(repos: Repositories) -> None:
    stop = asyncio.Event()
    stop.set()
    adapter = ScriptedAdapter([batch(unified("never"))])

    async def handler(message: UnifiedMessage) -> None:  # pragma: no cover
        raise AssertionError("no message expected")

    await run_polling(adapter, handler, repos.poll_offsets, stop=stop)

    assert adapter.poll_offsets == []


async def test_cancellation_propagates(repos: Repositories) -> None:
    adapter = ScriptedAdapter([batch()], block=True)

    async def handler(message: UnifiedMessage) -> None:  # pragma: no cover
        raise AssertionError("no message expected")

    task = asyncio.create_task(run_polling(adapter, handler, repos.poll_offsets))
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_run_all_polls_every_adapter(repos: Repositories) -> None:
    stop = asyncio.Event()
    handled: list[Platform] = []

    telegram = ScriptedAdapter(
        [batch(unified("t", platform=Platform.TELEGRAM), next_offset=3)],
        platform=Platform.TELEGRAM,
    )
    bale = ScriptedAdapter(
        [batch(unified("b", platform=Platform.BALE), next_offset=4)],
        platform=Platform.BALE,
    )

    async def handler(message: UnifiedMessage) -> None:
        handled.append(message.source)
        if len(handled) == 2:
            stop.set()

    await run_all(
        [
            (telegram, handler, repos.poll_offsets),
            (bale, handler, repos.poll_offsets),
        ],
        poll_timeout=3,
        stop=stop,
    )

    assert set(handled) == {Platform.TELEGRAM, Platform.BALE}
    assert repos.poll_offsets.get(Platform.TELEGRAM) == 3
    assert repos.poll_offsets.get(Platform.BALE) == 4
