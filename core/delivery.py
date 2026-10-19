"""The outbound delivery loop: turns ``outbox`` rows into platform sends.

The bridge only *records* a relay (message row + outbox entry, atomically);
actual network sends happen here. This keeps the inbound path fast, crash-safe
(a restart resumes pending entries) and replay-safe (each entry is delivered
and marked exactly once).

Retry policy is deliberately simple in this phase: transient failures are
retried after a fixed delay; permanent failures (blocked bot, unreachable chat)
and payloads that can never succeed are marked ``failed`` terminally, and so is
any entry that exhausted ``max_attempts``. Smarter backoff/notify comes later.

Failure containment: one bad entry can never stall the queue. A sender that
raises something other than a :class:`~core.errors.MessagingError` is treated as
a failed attempt (only the exception *type* is recorded — a stray exception
message may embed user content), and a pass that dies for any other reason is
logged by exception type and retried on the next interval instead of killing the
worker loop.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from adapters.base import guess_suffix
from core.errors import (
    MediaTooLargeError,
    MediaUnsupportedError,
    MessagingError,
    RateLimitError,
    TransportError,
)
from core.media import (
    RELAY_MEDIA_KIND,
    STICKER_PLACEHOLDER,
    MediaDeliveryPlan,
    MediaPolicy,
    TempMediaStore,
    can_send_album,
    check_download_size,
    check_upload_size,
    convert_sticker_to_png,
    media_item_from_payload,
    plan_media_delivery,
)
from core.models import (
    MediaItem,
    MediaKind,
    OutboundMedia,
    OutboxEntry,
    OutboxStats,
    Platform,
    SentMessage,
)
from core.ports import (
    MessageRepositoryPort,
    MessageSenderPort,
    OutboxRepositoryPort,
    TransactionManager,
)
from core.ratelimit import (
    SCOPE_USER,
    RateLimitDecision,
    RateLimiter,
)
from core.retry import RetryPolicy, is_retryable
from core.users import UserService

#: Payload discriminator written by the bridge (``outbox.payload["kind"]``).
RELAY_PAYLOAD_KIND = "relay_text"

#: How long a malformed payload error string may grow (log/DB hygiene).
_MAX_ERROR_LENGTH = 200

#: Default pause between outbox passes when the worker runs forever.
OUTBOX_POLL_INTERVAL_SECONDS = 2.0

#: Error recorded when a media entry arrives without a configured spool.
NO_SPOOL_ERROR = "media relaying is not configured (no temp store)"
#: Error recorded when an entry's payload cannot be understood.
MALFORMED_MEDIA_ERROR = "malformed media payload"
#: Notice sent to the sender when their file cannot be delivered.
TOO_LARGE_NOTICE_TEMPLATE = (
    "پیام شما ارسال نشد: {reason}"
)
#: Guidance sent once per cooldown when a heavy user is throttled (queued).
RATE_LIMIT_QUEUED_NOTICE = (
    "شما در حال ارسال پیام‌های زیادی هستید. برای جلوگیری از اسپم، "
    "پیام‌های شما با کمی تأخیر ارسال می‌شوند."
)
#: Guidance sent when the configured policy is to reject excess messages.
RATE_LIMIT_REJECTED_NOTICE = (
    "پیام شما ارسال نشد: به سقف مجاز ارسال پیام در هر دقیقه رسیده‌اید."
)
#: Reason stored on an entry rejected by the per-user rate limit.
RATE_LIMIT_REJECT_ERROR = "user rate limit exceeded"
#: Default action when the per-user limit is hit.
USER_RATE_LIMIT_ACTIONS = ("queue", "reject")


@dataclass(frozen=True, slots=True)
class OutboxRunStats:
    """Counters for one ``process_due`` pass (ids and outcomes, no content)."""

    delivered: int = 0
    retried: int = 0
    failed: int = 0
    skipped: int = 0
    deferred: int = 0

    @property
    def total(self) -> int:
        """Entries examined in the pass."""
        return self.delivered + self.retried + self.failed + self.skipped + self.deferred


@dataclass(slots=True)
class _MediaJob:
    """One media outbox entry, resolved into items, plans and spool files."""

    entry: OutboxEntry
    items: tuple[MediaItem, ...]
    plans: tuple[MediaDeliveryPlan, ...]
    text: str | None
    reply_to_message_id: int | None
    source_platform: Platform
    source_user_id: int | None
    target_user_id: int
    target_chat_id: int
    target_platform: Platform
    spooled: list[Path] = field(default_factory=list)
    notice: str | None = None


@dataclass(frozen=True, slots=True)
class _PreparedItem:
    """One downloaded (and possibly converted) attachment, ready to send."""

    job: _MediaJob
    index: int
    item: OutboundMedia


class _TransientMediaError(Exception):
    """A media failure that must be classified by the retry policy."""

    def __init__(self, error: MessagingError) -> None:
        super().__init__(error.message)
        self.error = error


def coalesce_albums(
    entries: Sequence[OutboxEntry],
) -> tuple[tuple[OutboxEntry, ...], ...]:
    """Group due media entries that belong to the same album.

    Album members are enqueued together with the same ``album_id``, so a single
    pass can hand them to the platform as one album (and count them as one
    failure/retry unit). Singles are returned untouched, in queue order.
    """
    groups: list[tuple[OutboxEntry, ...]] = []
    index = 0
    while index < len(entries):
        entry = entries[index]
        payload = entry.payload
        album_id = payload.get("album_id") if payload.get("kind") == RELAY_MEDIA_KIND else None
        if not isinstance(album_id, str) or not album_id:
            groups.append((entry,))
            index += 1
            continue
        members = [entry]
        lookahead = index + 1
        while lookahead < len(entries):
            candidate = entries[lookahead]
            if candidate.payload.get("album_id") != album_id:
                break
            members.append(candidate)
            lookahead += 1
        groups.append(tuple(members))
        index = lookahead
    return tuple(groups)


def _platform_or_none(value: object) -> Platform | None:
    """``value`` as a :class:`Platform` when it names one, else ``None``."""
    if isinstance(value, Platform):
        return value
    if isinstance(value, str):
        try:
            return Platform(value)
        except ValueError:
            return None
    return None


class OutboxWorker:
    """Delivers pending ``outbox`` entries via the platform senders.

    Args:
        users: User service (target lookup).
        messages: Message repository (records delivered target ids).
        outbox: Outbox repository (queue + status bookkeeping).
        tx: Transaction manager sharing the connection with the repositories.
        senders: One sender per platform (the adapters in production).
        clock: Injectable UTC clock for due/backoff decisions.
        retry_delay: Base delay for the first retry (kept for callers of the
            previous phase; ``retry_policy`` overrides it).
        max_attempts: Total failed attempts allowed before an entry is
            terminal (also overridden by ``retry_policy``).
        retry_policy: Full exponential-backoff policy (base, factor, cap,
            jitter, attempt cap).
        rate_limiter: Send-rate limiter; ``None`` creates the default limiter.
        user_rate_limit_action: ``queue`` (default) reschedules excess messages
            and sends one guidance note; ``reject`` marks the excess entries
            ``failed`` instead.
        random_source: Uniform ``[0, 1)`` sample used for jitter; tests inject a
            constant value for a reproducible schedule.
        stale_sending_after: Lease age after which a still-``sending`` row is
            recovered even without a restart (defensive safety net).
        heartbeat: Optional health file writer, called after every pass.
        logger: Logger override.
    """

    def __init__(
        self,
        *,
        users: UserService,
        messages: MessageRepositoryPort,
        outbox: OutboxRepositoryPort,
        tx: TransactionManager,
        senders: Mapping[Platform, MessageSenderPort],
        clock: Callable[[], datetime] | None = None,
        retry_delay: timedelta = timedelta(seconds=10),
        max_attempts: int = 5,
        retry_policy: RetryPolicy | None = None,
        rate_limiter: RateLimiter | None = None,
        user_rate_limit_action: str = "queue",
        rate_limit_notice_cooldown: timedelta = timedelta(minutes=1),
        media_timeout_terminal: bool = True,
        sent_retention: timedelta = timedelta(days=7),
        random_source: Callable[[], float] | None = None,
        stale_sending_after: timedelta = timedelta(minutes=5),
        heartbeat: object | None = None,
        store: TempMediaStore | None = None,
        policy: MediaPolicy | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if retry_delay <= timedelta(0):
            raise ValueError("retry_delay must be positive.")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1.")
        if user_rate_limit_action not in USER_RATE_LIMIT_ACTIONS:
            raise ValueError(
                "user_rate_limit_action must be one of "
                f"{', '.join(USER_RATE_LIMIT_ACTIONS)}."
            )
        if stale_sending_after < timedelta(0):
            raise ValueError("stale_sending_after must be >= 0.")
        if rate_limit_notice_cooldown < timedelta(0):
            raise ValueError("rate_limit_notice_cooldown must be >= 0.")
        if sent_retention < timedelta(0):
            raise ValueError("sent_retention must be >= 0.")
        self._users = users
        self._messages = messages
        self._outbox = outbox
        self._tx = tx
        self._senders = dict(senders)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._retry = retry_policy or RetryPolicy(
            base_delay=retry_delay, max_attempts=max_attempts
        )
        self._max_attempts = self._retry.max_attempts
        self._rate_limiter = (
            rate_limiter if rate_limiter is not None else RateLimiter(clock=self._clock)
        )
        self._user_rate_limit_action = user_rate_limit_action
        self._rate_limit_notice_cooldown = rate_limit_notice_cooldown
        self._media_timeout_terminal = media_timeout_terminal
        self._sent_retention = sent_retention
        self._random_source = random_source or random.random
        self._stale_sending_after = stale_sending_after
        self._heartbeat = heartbeat
        self._store = store
        self._policy = policy if policy is not None else MediaPolicy()
        self._log = logger or logging.getLogger("bridge.delivery")
        self._started = False
        self._rate_notice_at: dict[int, datetime] = {}

    async def process_due(self, *, limit: int = 100) -> OutboxRunStats:
        """Deliver every due pending entry once and return the counters.

        Album members that are due in the same pass are delivered together, so
        the partner receives one album instead of N separate messages.
        """
        stats = OutboxRunStats()
        now = self._clock()
        if not self._started:
            # A fresh worker owns the queue: any lease it sees was left by a
            # process that crashed mid-send. Recover it before claiming.
            recovered = self._outbox.recover_orphaned(now)
            self._started = True
            if recovered:
                self._log.warning("recovered %s orphaned sending entries", recovered)
            if self._sent_retention > timedelta(0):
                purged = self._outbox.purge_sent(now - self._sent_retention)
                if purged:
                    self._log.info(
                        "purged %s content-free sent outbox rows", purged
                    )
        else:
            recovered = self._outbox.recover_stale(
                now, max_age=self._stale_sending_after
            )
            if recovered:
                self._log.warning("recovered %s stale sending entries", recovered)

        for group in coalesce_albums(self._outbox.claim_due(now, limit=limit)):
            try:
                outcomes = await self._deliver_group(group)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - one bad unit must not stall
                self._log.error(
                    "delivery unit crashed (%s) entries=%s",
                    type(exc).__name__,
                    len(group),
                )
                outcomes = tuple(
                    self._recover_crashed_entry(entry) for entry in group
                )
            stats = OutboxRunStats(
                delivered=stats.delivered + sum(o == "delivered" for o in outcomes),
                retried=stats.retried + sum(o == "retried" for o in outcomes),
                failed=stats.failed + sum(o == "failed" for o in outcomes),
                skipped=stats.skipped + sum(o == "skipped" for o in outcomes),
                deferred=stats.deferred + sum(o == "deferred" for o in outcomes),
            )
        if stats.total:
            self._log.info(
                "outbox pass done delivered=%s retried=%s failed=%s skipped=%s "
                "deferred=%s",
                stats.delivered,
                stats.retried,
                stats.failed,
                stats.skipped,
                stats.deferred,
            )
        self._beat()
        return stats

    def recover_orphaned(self) -> int:
        """Recover every ``sending`` row now (used at process start)."""
        recovered = self._outbox.recover_orphaned(self._clock())
        if recovered:
            self._log.warning("recovered %s orphaned sending entries", recovered)
        return recovered

    async def run(
        self,
        *,
        stop: asyncio.Event | None = None,
        interval: float = OUTBOX_POLL_INTERVAL_SECONDS,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        """Process the outbox forever (or until ``stop`` is set).

        Shutdown is cooperative: a pass that is already sending is allowed to
        finish, then the loop exits. Nothing is cancelled mid-send, so a clean
        SIGINT/SIGTERM cannot leave a half-delivered message behind.
        """
        wait = sleep or asyncio.sleep
        self._beat()
        while stop is None or not stop.is_set():
            try:
                await self.process_due()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the loop must survive any bug
                self._log.error(
                    "outbox pass aborted (%s); retrying next interval",
                    type(exc).__name__,
                )
                self._beat(status="degraded")
            if stop is not None and stop.is_set():
                break
            await wait(interval)

    def _beat(self, *, status: str = "ok") -> None:
        """Update the optional health marker; never let it break delivery."""
        if self._heartbeat is None:
            return
        try:
            beat = getattr(self._heartbeat, "beat")
            beat(status=status)
        except Exception as exc:  # noqa: BLE001 - health must not kill the worker
            self._log.warning("heartbeat update failed (%s)", type(exc).__name__)

    # --------------------------------------------------------------- internals

    async def _deliver_group(self, group: Sequence[OutboxEntry]) -> tuple[str, ...]:
        """Deliver one entry, or one album, and return an outcome per entry."""
        if len(group) == 1:
            return (await self._deliver_entry(group[0]),)
        return await self._deliver_album(group)

    async def _deliver_entry(self, entry: OutboxEntry) -> str:
        """Deliver one entry; returns ``delivered``/``retried``/``failed``/``skipped``."""
        if entry.payload.get("kind") == RELAY_MEDIA_KIND:
            return (await self._deliver_album((entry,)))[0]

        resolved = self._resolve_delivery(entry)
        if isinstance(resolved, str):  # unsolvable payload: terminal failure
            self._mark_terminal(entry, resolved)
            return "failed"

        sender, chat_id = resolved
        decision = self._rate_limiter.check(
            platform=sender.platform,
            chat_id=chat_id,
            user_id=_optional_int(entry.payload.get("source_user_id"))
            or _optional_int(entry.payload.get("target_user_id")),
            now=self._clock(),
        )
        if not decision.allowed:
            return await self._rate_limited_entry(entry, decision)

        try:
            sent = await sender.send_message(
                chat_id,
                str(entry.payload.get("text", "")),
                reply_to_message_id=_optional_int(entry.payload.get("reply_to_message_id")),
            )
        except MessagingError as exc:
            return "failed" if self._mark_failure(entry, exc) else "retried"
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - an adapter bug must not stall the queue
            # Only the exception *type* is recorded: a stray exception message can
            # carry message content, which must never reach the log or the DB.
            return (
                "failed"
                if self._mark_failure(
                    entry,
                    f"unexpected {type(exc).__name__} from the platform sender",
                )
                else "retried"
            )

        self._mark_delivered(entry, sent)
        return "delivered"

    # ----------------------------------------------------------------- media

    async def _deliver_album(
        self, entries: Sequence[OutboxEntry]
    ) -> tuple[str, ...]:
        """Deliver one media entry, or the members of one album together.

        Pipeline per item: validate the payload → download to the spool (or
        reuse the file a previous attempt left) → enforce the target's upload
        cap → rasterize a sticker when the plan says so → send (as one album
        when the target supports it) → delete the spooled file. A file that is
        too large for the target is *not* sent; the sender gets a notice instead.
        """
        outcomes: dict[int, str] = {}
        jobs: list[_MediaJob] = []
        for entry in entries:
            resolved = self._resolve_media_job(entry)
            if isinstance(resolved, str):
                self._mark_terminal(entry, resolved)
                outcomes[entry.id] = "failed"
            else:
                jobs.append(resolved)
        if not jobs:
            return tuple(outcomes.get(entry.id, "skipped") for entry in entries)

        target = self._users.get(jobs[0].target_user_id)
        sender = self._senders.get(target.platform) if target is not None else None
        if target is None or sender is None:
            reason = (
                f"unknown target user_id={jobs[0].target_user_id}"
                if target is None
                else f"no sender configured for platform={target.platform.value}"
            )
            for job in jobs:
                self._mark_terminal(job.entry, reason)
                outcomes[job.entry.id] = "failed"
            self._cleanup(jobs)
            return tuple(outcomes.get(entry.id, "skipped") for entry in entries)

        # One inbound album is one logical relay unit: even when the target has
        # no album API and every item travels separately, it is charged as one
        # platform/chat/user unit. A standalone attachment is its own group and
        # therefore its own unit.
        decision = self._rate_limiter.check(
            platform=target.platform,
            chat_id=jobs[0].target_chat_id,
            user_id=jobs[0].source_user_id or jobs[0].target_user_id,
            now=self._clock(),
        )
        if not decision.allowed:
            if self._reject_user_rate_limit(decision):
                for job in jobs:
                    self._mark_terminal(job.entry, RATE_LIMIT_REJECT_ERROR)
                    outcomes[job.entry.id] = "failed"
                await self._notify_rate_limit(
                    {"source_user_id": jobs[0].source_user_id}, decision
                )
                self._cleanup(jobs)
            else:
                self._defer_jobs(jobs, decision)
                for job in jobs:
                    outcomes[job.entry.id] = "deferred"
                await self._notify_rate_limit(
                    {"source_user_id": jobs[0].source_user_id}, decision
                )
            return tuple(outcomes.get(entry.id, "skipped") for entry in entries)

        try:
            try:
                prepared, placeholders = await self._prepare_items(jobs, target.platform)
            except _TransientMediaError as exc:
                for job in jobs:
                    terminal = self._mark_failure(job.entry, exc.error)
                    outcomes[job.entry.id] = "failed" if terminal else "retried"
                return tuple(outcomes.get(entry.id, "skipped") for entry in entries)

            # Items that cannot travel (too large, missing converter) are dropped
            # here; the sender is told about the size ones.
            for job in jobs:
                if job.notice is not None:
                    self._mark_terminal(job.entry, job.notice)
                    outcomes[job.entry.id] = "failed"
                    await self._notify_source(job, job.notice)

            await self._send_prepared(prepared, placeholders, outcomes, target.platform)
            for job in jobs:
                outcomes.setdefault(job.entry.id, "skipped")
            return tuple(outcomes.get(entry.id, "skipped") for entry in entries)
        finally:
            # A delivered or terminally failed entry no longer needs its file; a
            # retried/deferred one keeps it so the next attempt does not download
            # the same bytes again.
            self._cleanup(
                [
                    job
                    for job in jobs
                    if outcomes.get(job.entry.id) not in {"retried", "deferred"}
                ]
            )

    async def _prepare_items(
        self, jobs: Sequence[_MediaJob], target_platform: Platform
    ) -> tuple[list[_PreparedItem], list[tuple[_MediaJob, str]]]:
        """Download and validate every deliverable item of this batch.

        Raises:
            _TransientMediaError: A download failed in a retryable way.
        """
        prepared: list[_PreparedItem] = []
        placeholders: list[tuple[_MediaJob, str]] = []
        for job in jobs:
            for index, (item, plan) in enumerate(zip(job.items, job.plans)):
                if plan.is_placeholder:
                    placeholders.append((job, plan.text or ""))
                    continue
                try:
                    outbound = await self._prepare_item(
                        job, index, item, plan, target_platform=target_platform
                    )
                except MediaTooLargeError as exc:
                    job.notice = exc.message
                    self._log.warning(
                        "media too large entry_id=%s kind=%s bytes=%s limit=%s",
                        job.entry.id,
                        item.kind.value,
                        exc.size_bytes,
                        exc.limit_bytes,
                    )
                    continue
                except MediaUnsupportedError as exc:
                    if plan.action != "convert":
                        # Nothing can carry this kind to the target.
                        job.notice = exc.message
                        continue
                    # No converter available: degrade to a text placeholder
                    # instead of failing the relay.
                    self._log.info(
                        "media conversion unavailable entry_id=%s kind=%s",
                        job.entry.id,
                        item.kind.value,
                    )
                    placeholders.append((job, STICKER_PLACEHOLDER))
                    continue
                except MessagingError as exc:
                    raise _TransientMediaError(exc) from None
                prepared.append(_PreparedItem(job=job, index=index, item=outbound))
        return prepared, placeholders

    async def _prepare_item(
        self,
        job: _MediaJob,
        index: int,
        item: MediaItem,
        plan: MediaDeliveryPlan,
        *,
        target_platform: Platform,
    ) -> OutboundMedia:
        """Spool one attachment and turn it into an :class:`OutboundMedia`."""
        store = self._store
        if store is None:  # pragma: no cover - guarded by _resolve_media_job
            raise MediaUnsupportedError(kind=item.kind.value)

        check_download_size(
            size_bytes=item.file_size,
            source_platform=job.source_platform,
            policy=self._policy,
        )
        path = store.path_for(
            user_id=job.target_user_id,
            entry_id=job.entry.id,
            index=index,
            suffix=guess_suffix(item),
        )
        reused = path.exists() and path.stat().st_size > 0
        if not reused:
            if not store.has_room_for(job.target_user_id, item.file_size or 0):
                raise MediaTooLargeError(
                    size_bytes=store.usage(job.target_user_id) + (item.file_size or 0),
                    limit_bytes=store.quota_bytes,
                    kind="فایل موقت",
                    message=(
                        "فضای موقت ارسال رسانه پر شده است؛ چند دقیقه بعد دوباره تلاش کنید."
                    ),
                )
            downloader = self._senders.get(job.source_platform)
            if downloader is None:
                raise MediaUnsupportedError(kind=item.kind.value)
            store.prepare(path)
            try:
                await downloader.download_media(item, path)
            except MessagingError:
                store.discard(path)  # a partial file must not poison the retry
                raise
        # A reused file is still this attempt's to delete once it is delivered or
        # terminally failed; without this the retry would leak it.
        if path not in job.spooled:
            job.spooled.append(path)

        size = path.stat().st_size
        delivery_kind = plan.kind or item.kind
        check_upload_size(
            size_bytes=size,
            kind=delivery_kind,
            target_platform=target_platform,
            policy=self._policy,
        )

        mime_type = item.mime_type
        file_name = item.file_name
        if plan.action == "convert":
            converted = store.prepare(path.with_suffix(".png"))
            try:
                convert_sticker_to_png(path, converted)
            except MediaUnsupportedError:
                store.discard(converted)
                raise
            job.spooled.append(converted)
            path = converted
            size = converted.stat().st_size
            mime_type = "image/png"
            file_name = f"sticker{converted.suffix}"
            check_upload_size(
                size_bytes=size,
                kind=MediaKind.PHOTO,
                target_platform=target_platform,
                policy=self._policy,
            )

        return OutboundMedia(
            kind=delivery_kind,
            path=path,
            file_name=file_name,
            mime_type=mime_type,
            file_size=size,
            duration=item.duration,
            width=item.width,
            height=item.height,
            emoji=item.emoji,
            caption=job.text,
        )

    async def _send_prepared(
        self,
        prepared: Sequence[_PreparedItem],
        placeholders: Sequence[tuple[_MediaJob, str]],
        outcomes: dict[int, str],
        target_platform: Platform,
    ) -> None:
        """Send what was prepared: one album when possible, else one by one."""
        pending = [item for item in prepared if item.job.entry.id not in outcomes]
        album_plans = [item.job.plans[item.index] for item in pending]
        one_per_entry = len({item.job.entry.id for item in pending}) == len(pending)
        if (
            len(pending) >= 2
            and one_per_entry
            and can_send_album(album_plans, target_platform, self._policy)
        ):
            await self._send_album_items(pending, outcomes)
            pending = []

        for entry in pending:
            await self._send_single_item(entry, outcomes)

        for job, text in placeholders:
            if job.entry.id in outcomes:
                continue
            await self._send_placeholder(job, text, outcomes)

    async def _send_album_items(
        self, items: Sequence[_PreparedItem], outcomes: dict[int, str]
    ) -> None:
        """Deliver a prepared batch as one album."""
        head = items[0]
        sender = self._senders.get(head.job.target_platform)
        if sender is None:  # pragma: no cover - checked before preparing
            for item in items:
                self._mark_terminal(item.job.entry, "no sender configured")
                outcomes[item.job.entry.id] = "failed"
            return
        pending = list(items)
        try:
            sent = await sender.send_media_group(
                head.job.target_chat_id,
                [item.item for item in pending],
                reply_to_message_id=head.job.reply_to_message_id,
            )
        except MessagingError as exc:
            for item in pending:
                terminal = self._mark_failure(item.job.entry, exc)
                outcomes[item.job.entry.id] = "failed" if terminal else "retried"
            return
        except Exception as exc:  # noqa: BLE001 - an adapter bug must not stall
            self._log.error(
                "album send crashed (%s) entries=%s", type(exc).__name__, len(pending)
            )
            for item in pending:
                terminal = self._mark_failure(
                    item.job.entry,
                    f"unexpected {type(exc).__name__} from the platform sender",
                )
                outcomes[item.job.entry.id] = "failed" if terminal else "retried"
            return

        for index, item in enumerate(pending):
            sent_message = sent[index] if index < len(sent) else sent[0]
            self._mark_delivered(item.job.entry, sent_message)
            outcomes[item.job.entry.id] = "delivered"

    async def _send_single_item(
        self, prepared: _PreparedItem, outcomes: dict[int, str]
    ) -> None:
        """Deliver one prepared attachment on its own."""
        job = prepared.job
        sender = self._senders.get(job.target_platform)
        if sender is None:  # pragma: no cover - checked before preparing
            self._mark_terminal(job.entry, "no sender configured")
            outcomes[job.entry.id] = "failed"
            return
        try:
            sent = await sender.send_media(
                job.target_chat_id,
                prepared.item,
                reply_to_message_id=job.reply_to_message_id,
            )
        except MessagingError as exc:
            terminal = self._mark_failure(job.entry, exc)
            outcomes[job.entry.id] = "failed" if terminal else "retried"
            return
        except Exception as exc:  # noqa: BLE001 - an adapter bug must not stall
            terminal = self._mark_failure(
                job.entry,
                f"unexpected {type(exc).__name__} from the platform sender",
            )
            outcomes[job.entry.id] = "failed" if terminal else "retried"
            return
        self._mark_delivered(job.entry, sent)
        outcomes[job.entry.id] = "delivered"

    async def _send_placeholder(
        self, job: _MediaJob, text: str, outcomes: dict[int, str]
    ) -> None:
        """Deliver a text placeholder in place of an attachment."""
        sender = self._senders.get(job.target_platform)
        if sender is None:  # pragma: no cover - checked before preparing
            self._mark_terminal(job.entry, "no sender configured")
            outcomes[job.entry.id] = "failed"
            return
        body = f"{job.text}\n{text}" if job.text else text
        try:
            sent = await sender.send_message(
                job.target_chat_id, body, reply_to_message_id=job.reply_to_message_id
            )
        except MessagingError as exc:
            terminal = self._mark_failure(job.entry, exc)
            outcomes[job.entry.id] = "failed" if terminal else "retried"
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - an adapter bug must not stall
            terminal = self._mark_failure(
                job.entry,
                f"unexpected {type(exc).__name__} from the platform sender",
            )
            outcomes[job.entry.id] = "failed" if terminal else "retried"
            return
        self._mark_delivered(job.entry, sent)
        outcomes[job.entry.id] = "delivered"

    def _resolve_media_job(self, entry: OutboxEntry) -> _MediaJob | str:
        """Validate a media payload; returns a job or a terminal error string."""
        payload = entry.payload
        target_user_id = _optional_int(payload.get("target_user_id"))
        target_chat_id = _optional_int(payload.get("target_chat_id"))
        raw_items = payload.get("items")
        if (
            target_user_id is None
            or target_chat_id is None
            or not isinstance(raw_items, Sequence)
            or isinstance(raw_items, str)
            or not raw_items
        ):
            return MALFORMED_MEDIA_ERROR
        platform = _platform_or_none(payload.get("source_platform"))
        if platform is None:
            return f"{MALFORMED_MEDIA_ERROR}: unknown source platform"
        if self._store is None:
            return NO_SPOOL_ERROR
        user = self._users.get(target_user_id)
        if user is None:
            return f"unknown target user_id={target_user_id}"
        try:
            items = tuple(media_item_from_payload(raw) for raw in raw_items)
        except ValueError as exc:
            return f"{MALFORMED_MEDIA_ERROR}: {exc}"

        text = payload.get("text")
        return _MediaJob(
            entry=entry,
            items=items,
            plans=tuple(
                plan_media_delivery(item, user.platform, self._policy) for item in items
            ),
            text=text if isinstance(text, str) and text else None,
            reply_to_message_id=_optional_int(payload.get("reply_to_message_id")),
            source_platform=platform,
            source_user_id=_optional_int(payload.get("source_user_id")),
            target_user_id=target_user_id,
            target_chat_id=target_chat_id,
            target_platform=user.platform,
        )

    def _cleanup(self, jobs: Sequence[_MediaJob], *, keep: bool = False) -> None:
        """Delete spooled files, unless the entries still need them for a retry."""
        if keep or self._store is None:
            return
        for job in jobs:
            for path in job.spooled:
                self._store.discard(path)
            job.spooled.clear()

    async def _notify_source(self, job: _MediaJob, reason: str) -> None:
        """Tell the sender why their file was not relayed (ids and sizes only)."""
        if job.source_user_id is None:
            return
        sender = self._senders.get(job.source_platform)
        if sender is None:
            return
        user = self._users.get(job.source_user_id)
        chat_id = (
            (user.last_chat_id or user.platform_user_id) if user is not None else None
        )
        if chat_id is None:
            return
        try:
            await sender.send_message(
                chat_id, TOO_LARGE_NOTICE_TEMPLATE.format(reason=reason)
            )
        except MessagingError as exc:
            self._log.warning(
                "media notice to user_id=%s failed (%s)",
                job.source_user_id,
                type(exc).__name__,
            )

    def _resolve_delivery(
        self, entry: OutboxEntry
    ) -> tuple[MessageSenderPort, int] | str:
        """Validate the payload and resolve its sender.

        Returns the ``(sender, chat_id)`` pair, or an error string when the
        entry can never succeed (unknown kind, missing fields, unknown target,
        missing sender) — the caller marks it terminally failed.
        """
        payload = entry.payload
        if payload.get("kind") != RELAY_PAYLOAD_KIND:
            return "malformed payload: unsupported kind"

        target_user_id = _optional_int(payload.get("target_user_id"))
        chat_id = _optional_int(payload.get("target_chat_id"))
        if target_user_id is None or chat_id is None:
            return "malformed payload: missing target"
        if not isinstance(payload.get("text"), str):
            return "malformed payload: missing text"

        user = self._users.get(target_user_id)
        if user is None:
            return f"unknown target user_id={target_user_id}"
        sender = self._senders.get(user.platform)
        if sender is None:
            return f"no sender configured for platform={user.platform.value}"
        return sender, chat_id

    def _recover_crashed_entry(self, entry: OutboxEntry) -> str:
        """Record an unexpected delivery-unit crash so the lease is not stuck."""
        try:
            terminal = self._mark_failure(
                entry, "unexpected error while delivering this entry"
            )
            return "failed" if terminal else "retried"
        except Exception as exc:  # noqa: BLE001 - recovery is best effort
            self._log.error(
                "could not recover crashed entry_id=%s (%s)",
                entry.id,
                type(exc).__name__,
            )
            return "skipped"

    def _mark_delivered(self, entry: OutboxEntry, sent: SentMessage) -> None:
        """Mark the entry sent and remember the delivered platform message id."""
        message_id = _optional_int(entry.payload.get("message_id"))
        with self._tx.transaction():
            self._outbox.mark_sent(
                entry.pair_id, entry.id, sent_at=self._clock()
            )
            if message_id is not None:
                self._messages.attach_target_message(
                    entry.pair_id, message_id, sent.message_id
                )
        self._log.info(
            "outbox delivered entry_id=%s pair_id=%s target_message_id=%s",
            entry.id,
            entry.pair_id,
            sent.message_id,
        )

    def _mark_terminal(self, entry: OutboxEntry, error: str) -> None:
        """Record a failure that can never succeed, without retrying."""
        now = self._clock()
        with self._tx.transaction():
            self._outbox.mark_failed(
                entry.pair_id,
                entry.id,
                error=error[:_MAX_ERROR_LENGTH],
                next_attempt_at=now,
                terminal=True,
                now=now,
            )
        self._log.warning(
            "outbox delivery failed entry_id=%s pair_id=%s attempts=%s "
            "terminal=True error=%s",
            entry.id,
            entry.pair_id,
            entry.attempts + 1,
            error[:_MAX_ERROR_LENGTH],
        )

    def _mark_failure(self, entry: OutboxEntry, error: MessagingError | str) -> bool:
        """Record a failed attempt using the retry policy.

        Returns:
            ``True`` when this failure is terminal.
        """
        now = self._clock()
        attempts = entry.attempts + 1
        if isinstance(error, MessagingError):
            message = error.message
            retryable = is_retryable(error)
            retry_after = (
                error.retry_after if isinstance(error, RateLimitError) else None
            )
            permanent = error.permanent
        else:
            message = error
            retryable = True
            retry_after = None
            permanent = False

        ambiguous_timeout = self._is_ambiguous_media_timeout(entry, error)
        if ambiguous_timeout:
            # The file may already be on the platform: retrying would duplicate
            # it. Stop after this attempt and keep a clear reason.
            message = (
                f"{message} (media upload timed out; delivery may have succeeded; "
                "not retried to avoid duplicates)"
            )
            retryable = False

        terminal = permanent or not retryable or attempts >= self._max_attempts
        delay = self._retry.delay_for(
            attempts,
            retry_after=retry_after,
            random_value=self._random_source(),
        )
        with self._tx.transaction():
            self._outbox.mark_failed(
                entry.pair_id,
                entry.id,
                error=message[:_MAX_ERROR_LENGTH],
                next_attempt_at=now + delay,
                terminal=terminal,
                now=now,
            )
        self._log.warning(
            "outbox delivery failed entry_id=%s pair_id=%s attempts=%s "
            "terminal=%s retry_in=%.1fs error=%s",
            entry.id,
            entry.pair_id,
            attempts,
            terminal,
            delay.total_seconds(),
            message[:_MAX_ERROR_LENGTH],
        )
        return terminal

    async def _rate_limited_entry(
        self, entry: OutboxEntry, decision: RateLimitDecision
    ) -> str:
        """Queue or reject a text entry denied by the rate limiter."""
        if self._reject_user_rate_limit(decision):
            self._mark_terminal(entry, RATE_LIMIT_REJECT_ERROR)
            await self._notify_rate_limit(entry.payload, decision)
            return "failed"
        self._defer_entry(entry, decision)
        await self._notify_rate_limit(entry.payload, decision)
        return "deferred"

    def _is_ambiguous_media_timeout(
        self, entry: OutboxEntry, error: MessagingError | str
    ) -> bool:
        """A media timeout may mean the platform already accepted the upload."""
        if not self._media_timeout_terminal:
            return False
        if entry.payload.get("kind") != RELAY_MEDIA_KIND:
            return False
        if not isinstance(error, TransportError):
            return False
        return "timed out" in error.message.lower() or "timeout" in error.message.lower()

    def _reject_user_rate_limit(self, decision: RateLimitDecision) -> bool:
        """Whether the per-user limit is configured to reject excess messages."""
        return (
            decision.scope == SCOPE_USER
            and self._user_rate_limit_action == "reject"
        )

    def _defer_entry(self, entry: OutboxEntry, decision: RateLimitDecision) -> None:
        """Reschedule one entry without spending an attempt."""
        now = self._clock()
        with self._tx.transaction():
            self._outbox.defer(
                entry.pair_id,
                entry.id,
                next_attempt_at=now + timedelta(seconds=max(decision.retry_after, 0.0)),
                reason=f"rate limited ({decision.scope})",
                now=now,
            )
        self._log.info(
            "outbox deferred entry_id=%s pair_id=%s scope=%s retry_in=%.1fs",
            entry.id,
            entry.pair_id,
            decision.scope,
            decision.retry_after,
        )

    def _defer_jobs(
        self, jobs: Sequence[_MediaJob], decision: RateLimitDecision
    ) -> None:
        """Reschedule every entry of one media delivery unit."""
        now = self._clock()
        with self._tx.transaction():
            for job in jobs:
                self._outbox.defer(
                    job.entry.pair_id,
                    job.entry.id,
                    next_attempt_at=now
                    + timedelta(seconds=max(decision.retry_after, 0.0)),
                    reason=f"rate limited ({decision.scope})",
                    now=now,
                )
        for job in jobs:
            self._log.info(
                "outbox deferred entry_id=%s pair_id=%s scope=%s retry_in=%.1fs",
                job.entry.id,
                job.entry.pair_id,
                decision.scope,
                decision.retry_after,
            )

    async def _notify_rate_limit(
        self, payload: Mapping[str, object], decision: RateLimitDecision
    ) -> None:
        """Send at most one guidance message per cooldown to a throttled user."""
        if decision.scope != SCOPE_USER:
            return
        source_user_id = _optional_int(payload.get("source_user_id"))
        if source_user_id is None:
            return
        now = self._clock()
        previous = self._rate_notice_at.get(source_user_id)
        if previous is not None and now - previous < self._rate_limit_notice_cooldown:
            return
        self._rate_notice_at[source_user_id] = now

        user = self._users.get(source_user_id)
        if user is None:
            return
        sender = self._senders.get(user.platform)
        if sender is None:
            return
        chat_id = user.last_chat_id or user.platform_user_id
        text = (
            RATE_LIMIT_REJECTED_NOTICE
            if self._user_rate_limit_action == "reject"
            else RATE_LIMIT_QUEUED_NOTICE
        )
        try:
            await sender.send_message(chat_id, text)
        except MessagingError as exc:
            self._log.warning(
                "rate-limit notice to user_id=%s failed (%s)",
                source_user_id,
                type(exc).__name__,
            )
        except Exception as exc:  # noqa: BLE001 - a notice must not kill the queue
            self._log.warning(
                "rate-limit notice to user_id=%s crashed (%s)",
                source_user_id,
                type(exc).__name__,
            )


def _optional_int(value: object) -> int | None:
    """Return ``value`` when it is an int (bools excluded), else ``None``."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


__all__ = [
    "OUTBOX_POLL_INTERVAL_SECONDS",
    "RATE_LIMIT_QUEUED_NOTICE",
    "RATE_LIMIT_REJECTED_NOTICE",
    "OutboxRunStats",
    "OutboxStats",
    "OutboxWorker",
    "RELAY_PAYLOAD_KIND",
    "RetryPolicy",
    "is_retryable",
]
