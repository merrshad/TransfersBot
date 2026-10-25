"""Process entrypoint: wire the runtime and poll both messengers forever.

``build_runtime`` performs no I/O beyond opening the SQLite database, so it can
be exercised in tests without any network or real token.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from adapters.bale import BaleAdapter
from adapters.base import BaseAdapter
from adapters.poller import MessageHandler, PollOffsetStorePort, run_polling
from adapters.telegram import TelegramAdapter
from config import Settings, get_settings
from core.bridge import Bridge
from core.commands import CommandRouter
from core.delivery import OutboxWorker
from core.health import Heartbeat
from core.media import (
    MEGABYTE,
    MediaGroupCollector,
    MediaPolicy,
    TempMediaStore,
    policy_from_settings,
)
from core.models import Platform
from core.pairing import PairingService
from core.ratelimit import RateLimiter
from core.retry import RetryPolicy
from core.users import UserService
from logging_setup import configure_logging
from storage.db import Repositories

LOGGER_NAME = "bridge.main"

#: Spool files older than this are leftovers from a crashed run and are deleted
#: at startup (a live relay never holds a file for anywhere near this long).
MEDIA_SPOOL_MAX_AGE_SECONDS = 24 * 60 * 60.0


@dataclass(slots=True)
class Runtime:
    """Everything the process needs, assembled once."""

    settings: Settings
    repos: Repositories
    users: UserService
    pairing: PairingService
    router: CommandRouter
    bridge: Bridge
    worker: OutboxWorker
    group_worker: OutboxWorker | None
    adapters: dict[Platform, BaseAdapter]
    collector: MediaGroupCollector
    media: TempMediaStore | None
    policy: MediaPolicy
    stop: asyncio.Event
    poll_timeout: int
    outbox_interval: float = 2.0
    shutdown_timeout: float = 30.0
    heartbeat: Heartbeat | None = None

    def poll_jobs(
        self, handler: MessageHandler
    ) -> list[tuple[BaseAdapter, MessageHandler, PollOffsetStorePort]]:
        """One polling job per adapter, all sharing the message handler."""
        return [
            (adapter, handler, self.repos.poll_offsets)
            for adapter in self.adapters.values()
        ]

    async def close(self) -> None:
        """Flush buffered albums, close every adapter and clear the heartbeat."""
        await self.collector.drain()
        for adapter in self.adapters.values():
            await adapter.close()
        if self.heartbeat is not None:
            self.heartbeat.clear()


def owner_platform_ids(settings: Settings) -> dict[Platform, int]:
    """Map each platform to its configured owner identity."""
    return {
        Platform.TELEGRAM: settings.telegram_owner_id,
        Platform.BALE: settings.bale_owner_id,
    }


def build_media_store(settings: Settings) -> TempMediaStore | None:
    """Spool store for attachments, or ``None`` when media relaying is off.

    Opening the store also sweeps files a previous run left behind (a crash
    between download and send), so the temp directory starts clean.
    """
    if not settings.media_enabled:
        return None
    store = TempMediaStore(
        settings.media_temp_dir,
        quota_bytes=settings.media_user_quota_mb * MEGABYTE,
    )
    removed = store.sweep(max_age_seconds=MEDIA_SPOOL_MAX_AGE_SECONDS)
    if removed:
        logging.getLogger(LOGGER_NAME).info(
            "media spool sweep removed %s stale file(s)", removed
        )
    return store


def build_runtime(settings: Settings, repos: Repositories) -> Runtime:
    """Assemble services, adapters and the bridge from settings and storage."""
    policy = policy_from_settings(settings)
    users = UserService(repos.users, repos.db, max_users=settings.max_users)
    pairing = PairingService(
        repos.users,
        repos.pairs,
        repos.requests,
        repos.db,
        max_pairs_per_user=settings.max_pairs_per_user,
        request_ttl=timedelta(hours=settings.pair_request_ttl_hours),
        invites=repos.invites,
        invite_ttl=timedelta(minutes=settings.pair_invite_ttl_minutes),
    )
    router = CommandRouter(
        users,
        pairing,
        stats=repos.outbox.queue_stats,
        telegram_bot_username=settings.telegram_bot_username,
        bale_bot_username=settings.bale_bot_username,
    )
    owners = owner_platform_ids(settings)
    adapters: dict[Platform, BaseAdapter] = {
        Platform.TELEGRAM: TelegramAdapter(
            settings.telegram_bot_token,
            max_download_bytes=policy.for_platform(Platform.TELEGRAM).max_download_bytes,
            connect_timeout=settings.telegram_connect_timeout_seconds,
            read_timeout=settings.telegram_read_timeout_seconds,
            write_timeout=settings.telegram_write_timeout_seconds,
        ),
        Platform.BALE: BaleAdapter(
            settings.bale_bot_token,
            base_url=settings.bale_api_base_url,
            max_download_bytes=policy.for_platform(Platform.BALE).max_download_bytes,
        ),
    }
    media_store = build_media_store(settings)
    rate_limiter = RateLimiter(
        global_per_second={
            Platform.TELEGRAM: settings.telegram_global_messages_per_second,
            Platform.BALE: settings.bale_global_messages_per_second,
        },
        per_chat_per_second={
            Platform.TELEGRAM: settings.telegram_chat_messages_per_second,
            Platform.BALE: settings.bale_chat_messages_per_second,
        },
        per_user_per_minute=settings.user_messages_per_minute,
        enabled=settings.rate_limit_enabled,
    )
    retry_policy = RetryPolicy(
        base_delay=timedelta(seconds=settings.outbox_retry_base_seconds),
        max_delay=timedelta(seconds=settings.outbox_retry_max_seconds),
        jitter_ratio=settings.outbox_retry_jitter,
        max_attempts=settings.outbox_max_attempts,
    )
    heartbeat = (
        Heartbeat(settings.healthcheck_file)
        if settings.healthcheck_file is not None
        else None
    )
    log = logging.getLogger(LOGGER_NAME)
    group_sync_ready = settings.group_sync_enabled
    if group_sync_ready:
        if settings.telegram_group_id is not None and settings.bale_group_id is not None:
            seeded = repos.group_links.seed_if_empty(
                telegram_chat_id=settings.telegram_group_id,
                bale_chat_id=settings.bale_group_id,
                now=datetime.now(UTC),
            )
            if seeded is not None:
                log.info(
                    "group bootstrap link_id=%s telegram_chat=%s bale_chat=%s",
                    seeded.id,
                    seeded.telegram_chat_id,
                    seeded.bale_chat_id,
                )
        else:
            log.info(
                "group bridge enabled without env bootstrap; use /groupsync in a group"
            )
        log.info(
            "group bridge enabled links=%s",
            len(repos.group_links.list_active()),
        )
    bridge = Bridge(
        users=users,
        pairing=pairing,
        router=router,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=adapters,
        owner_platform_ids=owners,
        group_links=repos.group_links if group_sync_ready else None,
        group_messages=repos.group_messages if group_sync_ready else None,
        group_outbox=repos.group_outbox if group_sync_ready else None,
        group_invites=repos.group_invites if group_sync_ready else None,
        known_groups=repos.known_groups if group_sync_ready else None,
        group_invite_ttl=timedelta(minutes=settings.group_invite_ttl_minutes),
    )
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=adapters,
        retry_policy=retry_policy,
        rate_limiter=rate_limiter,
        user_rate_limit_action=settings.user_rate_limit_action,
        rate_limit_notice_cooldown=timedelta(
            seconds=settings.user_rate_limit_notice_cooldown_seconds
        ),
        stale_sending_after=timedelta(
            seconds=settings.outbox_stale_sending_seconds
        ),
        media_timeout_terminal=settings.outbox_media_timeout_terminal,
        sent_retention=timedelta(days=settings.outbox_sent_retention_days),
        heartbeat=heartbeat,
        store=media_store,
        policy=policy,
    )
    group_worker = (
        OutboxWorker(
            users=users,
            messages=repos.messages,
            outbox=repos.group_outbox,
            group_messages=repos.group_messages,
            tx=repos.db,
            senders=adapters,
            retry_policy=retry_policy,
            rate_limiter=rate_limiter,
            user_rate_limit_action=settings.user_rate_limit_action,
            rate_limit_notice_cooldown=timedelta(
                seconds=settings.user_rate_limit_notice_cooldown_seconds
            ),
            stale_sending_after=timedelta(
                seconds=settings.outbox_stale_sending_seconds
            ),
            media_timeout_terminal=settings.outbox_media_timeout_terminal,
            sent_retention=timedelta(days=settings.outbox_sent_retention_days),
            store=media_store,
            policy=policy,
        )
        if group_sync_ready
        else None
    )
    collector = MediaGroupCollector(
        window=settings.media_group_window_seconds,
        flush=bridge.handle_messages,
    )
    return Runtime(
        settings=settings,
        repos=repos,
        users=users,
        pairing=pairing,
        router=router,
        bridge=bridge,
        worker=worker,
        group_worker=group_worker,
        adapters=adapters,
        collector=collector,
        media=media_store,
        policy=policy,
        stop=asyncio.Event(),
        poll_timeout=settings.poll_timeout_seconds,
        outbox_interval=settings.outbox_poll_interval_seconds,
        shutdown_timeout=settings.shutdown_timeout_seconds,
        heartbeat=heartbeat,
    )


def install_stop_handlers(
    loop: asyncio.AbstractEventLoop, stop: asyncio.Event
) -> Callable[[], None]:
    """Turn SIGINT/SIGTERM into a cooperative ``stop`` event.

    Returns a callable that restores the previous handlers. On Windows
    ``add_signal_handler`` is not implemented, so the synchronous
    ``signal.signal`` fallback is used and the event is set thread-safely.
    """

    restorers: list[Callable[[], None]] = []

    def _set_stop(*_args: object) -> None:
        loop.call_soon_threadsafe(stop.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            try:
                previous = signal.signal(signum, _set_stop)
            except (ValueError, OSError):  # pragma: no cover - non-main thread
                continue

            def _restore_previous(
                sig: signal.Signals = signum,
                prev: object = previous,
            ) -> None:
                signal.signal(sig, prev)  # type: ignore[arg-type]

            restorers.append(_restore_previous)
        else:

            def _remove_handler(sig: signal.Signals = signum) -> None:
                loop.remove_signal_handler(sig)

            restorers.append(_remove_handler)

    def _restore_all() -> None:
        for restore in restorers:
            restore()

    return _restore_all


async def run(runtime: Runtime, *, handle_signals: bool = True) -> None:
    """Run pollers and the outbox worker until a clean stop is requested.

    Shutdown order matters:

    1. stop accepting new updates (signal handler sets ``stop``);
    2. cancel the long-poll tasks (they make no more network reads);
    3. flush buffered album members into the durable queue;
    4. let the worker finish the pass it is already executing — a send in
       flight is never cancelled, so it is either marked ``sent`` or scheduled
       for a retry;
    5. close adapters, clear the health marker and close the database.
    """
    log = logging.getLogger(LOGGER_NAME)
    restore_signals = (
        install_stop_handlers(asyncio.get_running_loop(), runtime.stop)
        if handle_signals
        else (lambda: None)
    )
    log.info(
        "bridge polling started platforms=%s timeout=%ss",
        ",".join(platform.value for platform in runtime.adapters),
        runtime.poll_timeout,
    )
    # One task per adapter, so they can be stopped independently of the worker.
    poll_tasks = [
        asyncio.create_task(
            run_polling(
                adapter,
                handler,
                offsets,
                poll_timeout=runtime.poll_timeout,
                stop=runtime.stop,
            ),
            name=f"poller-{adapter.platform.value}",
        )
        for adapter, handler, offsets in runtime.poll_jobs(runtime.collector.add)
    ]
    worker_tasks = [
        asyncio.create_task(
            runtime.worker.run(stop=runtime.stop, interval=runtime.outbox_interval),
            name="outbox-worker",
        )
    ]
    if runtime.group_worker is not None:
        worker_tasks.append(
            asyncio.create_task(
                runtime.group_worker.run(
                    stop=runtime.stop, interval=runtime.outbox_interval
                ),
                name="group-outbox-worker",
            )
        )
    stop_task = asyncio.create_task(runtime.stop.wait(), name="stop-wait")
    try:
        done, _ = await asyncio.wait(
            [stop_task, *poll_tasks], return_when=asyncio.FIRST_COMPLETED
        )
        if stop_task not in done:
            # A poller returned on its own (a bug outside the MessagingError
            # handler). Do not hang forever waiting for a signal.
            log.error("a polling task stopped unexpectedly; shutting down")
            runtime.stop.set()
    except asyncio.CancelledError:
        runtime.stop.set()
        raise
    finally:
        runtime.stop.set()
        stop_task.cancel()
        for task in poll_tasks:
            task.cancel()
        await asyncio.gather(stop_task, *poll_tasks, return_exceptions=True)
        try:
            await runtime.collector.drain()
        except Exception as exc:  # noqa: BLE001 - cleanup is best effort
            log.error("collector drain failed (%s)", type(exc).__name__)
        try:
            await asyncio.wait_for(
                asyncio.gather(*worker_tasks), timeout=runtime.shutdown_timeout
            )
        except TimeoutError:
            log.warning(
                "outbox workers did not stop within %.1fs; cancelling",
                runtime.shutdown_timeout,
            )
            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            raise
        try:
            await runtime.close()
        finally:
            # Keep the signal handlers active until cleanup is done, so a
            # second SIGINT/SIGTERM just repeats the stop request.
            restore_signals()
        log.info("bridge stopped and adapters closed")


def main() -> int:
    """Load settings, configure logging and run the bridge."""
    settings = get_settings()
    configure_logging(
        settings.log_level,
        secrets=[
            settings.telegram_bot_token.get_secret_value(),
            settings.bale_bot_token.get_secret_value(),
        ],
    )
    log = logging.getLogger(LOGGER_NAME)
    repos = Repositories.open(settings.db_path)
    try:
        runtime = build_runtime(settings, repos)
        log.info(
            "starting bridge db=%s schema_version=%s max_users=%s max_pairs_per_user=%s",
            repos.db.path,
            repos.db.schema_version(),
            settings.max_users,
            settings.max_pairs_per_user,
        )
        asyncio.run(run(runtime))
    except KeyboardInterrupt:
        log.info("bridge interrupted by user")
    finally:
        repos.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    raise SystemExit(main())
