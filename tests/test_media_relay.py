"""End-to-end media relaying: bridge payloads, downloads, uploads, cleanup.

Everything runs offline: the fake senders download scripted bytes and record
uploads, the spool lives in ``tmp_path``, and the clock is fixed. The acceptance
criteria exercised here are that every supported kind travels in both
directions, that nothing is sent when a file does not fit, and that no spooled
file is left behind after a send or a terminal failure.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pytest

from core.bridge import Bridge
from core.delivery import RELAY_PAYLOAD_KIND, OutboxWorker, coalesce_albums
from core.errors import TransportError
from core.media import (
    MEGABYTE,
    RELAY_MEDIA_KIND,
    MediaGroupCollector,
    MediaPolicy,
    PlatformMediaLimits,
    TempMediaStore,
    unsupported_placeholder,
)
from core.models import (
    MediaItem,
    MediaKind,
    OutboxStatus,
    Platform,
    UnifiedMessage,
)
from core.pairing import Pair, PairingService
from core.users import User, UserService
from storage.db import Repositories
from tests.conftest import FAKE_MEDIA_BYTES, FIXED_NOW, RecordingSender

TELEGRAM_USER = 5001
BALE_USER = 7001


# ------------------------------------------------------------------- helpers


def media_message(
    platform: Platform,
    sender_id: int,
    kind: MediaKind,
    *,
    message_id: int,
    file_id: str = "file-1",
    text: str | None = None,
    file_size: int | None = None,
    mime_type: str | None = None,
    file_name: str | None = None,
    emoji: str | None = None,
    media_group_id: str | None = None,
    unsupported: str | None = None,
    with_media: bool = True,
) -> UnifiedMessage:
    """An inbound message carrying one attachment (and optional caption)."""
    media = (
        MediaItem(
            kind=kind,
            file_id=file_id,
            file_size=file_size,
            mime_type=mime_type,
            file_name=file_name,
            emoji=emoji,
        ),
    )
    return UnifiedMessage(
        source=platform,
        sender_id=sender_id,
        chat_id=sender_id,
        message_id=message_id,
        text=text,
        media=media if with_media else (),
        media_group_id=media_group_id,
        unsupported=unsupported,
        timestamp=FIXED_NOW,
    )


async def make_pair(
    bridge: Bridge, users: UserService, pairing: PairingService, repos: Repositories
) -> tuple[User, User, Pair]:
    """Register one user per platform through the bridge and pair them."""
    await bridge.handle_message(
        media_message(Platform.TELEGRAM, TELEGRAM_USER, MediaKind.PHOTO, message_id=1)
        if False
        else UnifiedMessage(
            source=Platform.TELEGRAM,
            sender_id=TELEGRAM_USER,
            chat_id=TELEGRAM_USER,
            message_id=1,
            text="/start",
            timestamp=FIXED_NOW,
        )
    )
    await bridge.handle_message(
        UnifiedMessage(
            source=Platform.BALE,
            sender_id=BALE_USER,
            chat_id=BALE_USER,
            message_id=1,
            text="/start",
            timestamp=FIXED_NOW,
        )
    )
    alice = users.get_by_platform(Platform.TELEGRAM, TELEGRAM_USER)
    bob = users.get_by_platform(Platform.BALE, BALE_USER)
    assert alice is not None and bob is not None
    await bridge.handle_message(
        UnifiedMessage(
            source=Platform.TELEGRAM,
            sender_id=TELEGRAM_USER,
            chat_id=TELEGRAM_USER,
            message_id=2,
            text=f"/pair {bob.public_id}",
            timestamp=FIXED_NOW,
        )
    )
    request = repos.requests.list_pending_incoming(bob.id)[0]
    await bridge.handle_message(
        UnifiedMessage(
            source=Platform.BALE,
            sender_id=BALE_USER,
            chat_id=BALE_USER,
            message_id=2,
            text=f"/accept {request.id}",
            timestamp=FIXED_NOW,
        )
    )
    pair = pairing.active_pair(alice.id)
    assert pair is not None
    return alice, bob, pair


def clear_senders(senders: dict[Platform, RecordingSender]) -> None:
    for sender in senders.values():
        sender.sent.clear()
        sender.media.clear()
        sender.downloads.clear()


def payloads(repos: Repositories, pair: Pair) -> list[dict]:
    return [dict(entry.payload) for entry in repos.outbox.list_for_pair(pair.id)]


def latest_payload(repos: Repositories, pair: Pair) -> dict:
    return payloads(repos, pair)[-1]


# ------------------------------------------------------------------ relaying


async def test_photo_with_caption_is_queued_and_then_delivered(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    media_store: TempMediaStore,
) -> None:
    alice, bob, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.PHOTO,
            message_id=10,
            file_id="tg-photo",
            text="کپشن عکس",
            file_size=2000,
            mime_type="image/jpeg",
        )
    )

    payload = latest_payload(repos, pair)
    assert payload["kind"] == RELAY_MEDIA_KIND
    assert payload["text"] == "کپشن عکس"
    assert payload["target_user_id"] == bob.id
    assert payload["source_platform"] == "telegram"
    assert payload["source_user_id"] == alice.id
    (item,) = payload["items"]
    assert item["kind"] == "photo" and item["file_id"] == "tg-photo"
    assert "data" not in item, "only the handle travels in the payload"

    stats = await media_worker.process_due()

    assert stats.delivered == 1
    assert senders[Platform.TELEGRAM].media == [], "never back to the sender"
    (call,) = senders[Platform.BALE].media
    assert call.chat_id == BALE_USER
    assert call.kinds == (MediaKind.PHOTO,)
    assert call.captions == ("کپشن عکس",)
    # The size handed to the platform is the *real* downloaded one.
    assert call.items[0].file_size == len(FAKE_MEDIA_BYTES)
    assert media_store.files() == (), "the spool is empty after a send"


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (MediaKind.PHOTO, MediaKind.PHOTO),
        (MediaKind.VIDEO, MediaKind.VIDEO),
        (MediaKind.ANIMATION, MediaKind.ANIMATION),
        (MediaKind.DOCUMENT, MediaKind.DOCUMENT),
        (MediaKind.AUDIO, MediaKind.AUDIO),
        (MediaKind.VOICE, MediaKind.VOICE),
        (MediaKind.VIDEO_NOTE, MediaKind.VIDEO),
        # Bale has no sticker endpoint and no converter is configured here.
        (MediaKind.STICKER, None),
    ],
)
async def test_every_telegram_kind_reaches_bale_or_degrades(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    media_store: TempMediaStore,
    kind: MediaKind,
    expected: MediaKind | None,
) -> None:
    _, _, _ = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            kind,
            message_id=10,
            text="کپشن",
            file_size=1000,
            mime_type={
                MediaKind.PHOTO: "image/jpeg",
                MediaKind.VIDEO: "video/mp4",
                MediaKind.ANIMATION: "video/mp4",
                MediaKind.DOCUMENT: "application/pdf",
                MediaKind.AUDIO: "audio/mpeg",
                MediaKind.VOICE: "audio/ogg",
                MediaKind.VIDEO_NOTE: "video/mp4",
                MediaKind.STICKER: "image/webp",
            }[kind],
        )
    )

    stats = await media_worker.process_due()

    assert stats.delivered == 1
    bale = senders[Platform.BALE]
    if expected is None:
        assert bale.media == []
        assert any("sticker" in text for text in bale.texts_to(BALE_USER))
    else:
        (call,) = bale.media
        assert call.kinds == (expected,)
    assert media_store.files() == ()


@pytest.mark.parametrize(
    ("kind", "mime", "expected"),
    [
        (MediaKind.PHOTO, "image/jpeg", MediaKind.PHOTO),
        (MediaKind.VIDEO, "video/mp4", MediaKind.VIDEO),
        (MediaKind.ANIMATION, "video/mp4", MediaKind.ANIMATION),
        (MediaKind.DOCUMENT, "application/zip", MediaKind.DOCUMENT),
        (MediaKind.AUDIO, "audio/mpeg", MediaKind.AUDIO),
        (MediaKind.VOICE, "audio/ogg", MediaKind.VOICE),
        (MediaKind.VIDEO_NOTE, "video/mp4", MediaKind.VIDEO_NOTE),
        (MediaKind.STICKER, "image/webp", MediaKind.STICKER),
    ],
)
async def test_every_bale_kind_reaches_telegram(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    media_store: TempMediaStore,
    kind: MediaKind,
    mime: str,
    expected: MediaKind,
) -> None:
    _, _, _ = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        media_message(
            Platform.BALE,
            BALE_USER,
            kind,
            message_id=20,
            text="caption",
            file_size=1000,
            mime_type=mime,
        )
    )

    stats = await media_worker.process_due()

    assert stats.delivered == 1
    (call,) = senders[Platform.TELEGRAM].media
    assert call.kinds == (expected,)
    assert call.captions == ("caption",)
    assert media_store.files() == ()


async def test_relay_is_bidirectional_for_media(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
) -> None:
    await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.PHOTO,
            message_id=10,
            mime_type="image/jpeg",
            file_size=100,
        )
    )
    await bridge.handle_message(
        media_message(
            Platform.BALE,
            BALE_USER,
            MediaKind.VOICE,
            message_id=20,
            mime_type="audio/ogg",
            file_size=100,
        )
    )
    await media_worker.process_due()

    assert senders[Platform.BALE].media[0].kinds == (MediaKind.PHOTO,)
    assert senders[Platform.TELEGRAM].media[0].kinds == (MediaKind.VOICE,)
    assert senders[Platform.TELEGRAM].media[0].chat_id == TELEGRAM_USER
    assert senders[Platform.BALE].media[0].chat_id == BALE_USER


async def test_media_message_without_a_pair_gets_the_guide(
    bridge: Bridge,
    users: UserService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    await bridge.handle_message(
        UnifiedMessage(
            source=Platform.TELEGRAM,
            sender_id=TELEGRAM_USER,
            chat_id=TELEGRAM_USER,
            message_id=1,
            text="/start",
            timestamp=FIXED_NOW,
        )
    )
    clear_senders(senders)

    await bridge.handle_message(
        media_message(Platform.TELEGRAM, TELEGRAM_USER, MediaKind.PHOTO, message_id=2)
    )

    assert repos.outbox.list_due(FIXED_NOW) == ()
    assert any("/pair" in text for text in senders[Platform.TELEGRAM].texts_to(TELEGRAM_USER))


async def test_media_from_a_banned_sender_is_not_queued(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
) -> None:
    from core.users import UserStatus

    alice, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    repos.users.set_status(alice.id, UserStatus.BANNED)

    await bridge.handle_message(
        media_message(Platform.TELEGRAM, TELEGRAM_USER, MediaKind.PHOTO, message_id=3)
    )

    assert repos.outbox.list_for_pair(pair.id) == ()


async def test_duplicate_media_delivery_is_relayed_once(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    media_worker: OutboxWorker,
    senders: dict[Platform, RecordingSender],
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    message = media_message(
        Platform.TELEGRAM, TELEGRAM_USER, MediaKind.PHOTO, message_id=10, mime_type="image/jpeg"
    )

    await bridge.handle_message(message)
    await bridge.handle_message(message)
    await media_worker.process_due()

    assert len(repos.outbox.list_for_pair(pair.id)) == 1
    assert len(senders[Platform.BALE].media) == 1


# ------------------------------------------------------ placeholders & limits


async def test_unsupported_message_type_becomes_a_placeholder(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    with caplog.at_level(logging.INFO, logger="bridge.core"):
        await bridge.handle_message(
            media_message(
                Platform.TELEGRAM,
                TELEGRAM_USER,
                MediaKind.PHOTO,
                message_id=10,
                with_media=False,
                unsupported="dice",
            )
        )

    payload = latest_payload(repos, pair)
    assert payload["kind"] == RELAY_PAYLOAD_KIND
    assert payload["text"] == unsupported_placeholder("dice")
    assert "dice" in caplog.text, "the type name is logged, the content is not"

    await media_worker.process_due()
    assert senders[Platform.BALE].texts_to(BALE_USER) == [unsupported_placeholder("dice")]


async def test_a_mismatched_mime_type_is_relayed_as_text(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    media_store: TempMediaStore,
) -> None:
    _, _, _ = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.PHOTO,
            message_id=10,
            text="کپشن",
            mime_type="text/plain",
        )
    )
    stats = await media_worker.process_due()

    assert stats.delivered == 1
    bale = senders[Platform.BALE]
    assert bale.media == []
    assert any("text/plain" in text for text in bale.texts_to(BALE_USER))
    assert senders[Platform.TELEGRAM].downloads == [], "nothing is downloaded"
    assert media_store.files() == ()


async def test_a_file_over_the_target_limit_is_not_sent_and_the_sender_is_told(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    media_store: TempMediaStore,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    # 12 MB photo: fine for Telegram's downloader, over Bale's 10 MB photo cap.
    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.PHOTO,
            message_id=10,
            mime_type="image/jpeg",
            file_size=12 * MEGABYTE,
        )
    )
    senders[Platform.TELEGRAM].download_data = b"x" * 12_000_000

    stats = await media_worker.process_due()

    assert stats.failed == 1
    assert senders[Platform.BALE].media == [], "nothing is uploaded"
    notices = senders[Platform.TELEGRAM].texts_to(TELEGRAM_USER)
    assert notices and "ارسال نشد" in notices[0]
    (entry,) = repos.outbox.list_for_pair(pair.id)
    assert entry.status is OutboxStatus.FAILED
    assert "10.0 MB" in (entry.last_error or "")
    assert media_store.files() == (), "the spool is empty after a terminal failure"


async def test_a_file_over_the_download_limit_is_not_fetched(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    media_store: TempMediaStore,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        media_message(
            Platform.BALE,
            BALE_USER,
            MediaKind.DOCUMENT,
            message_id=20,
            mime_type="application/pdf",
            file_size=25 * MEGABYTE,
        )
    )

    stats = await media_worker.process_due()

    assert stats.failed == 1
    assert senders[Platform.BALE].downloads == []
    assert repos.outbox.list_for_pair(pair.id)[0].status is OutboxStatus.FAILED
    assert media_store.files() == ()


async def test_the_per_user_spool_quota_is_enforced(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_policy: MediaPolicy,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    tiny_store = TempMediaStore(repos.db.path + "-spool", quota_bytes=10)
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        retry_delay=timedelta(seconds=10),
        store=tiny_store,
        policy=media_policy,
    )

    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.PHOTO,
            message_id=10,
            mime_type="image/jpeg",
            file_size=1000,
        )
    )
    stats = await worker.process_due()

    assert stats.failed == 1
    assert senders[Platform.BALE].media == []
    assert any("فضای موقت" in text for text in senders[Platform.TELEGRAM].texts_to(TELEGRAM_USER))
    assert tiny_store.files() == ()


async def test_media_entries_fail_terminally_without_a_spool(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_policy: MediaPolicy,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        store=None,
        policy=media_policy,
    )

    await bridge.handle_message(
        media_message(Platform.TELEGRAM, TELEGRAM_USER, MediaKind.PHOTO, message_id=10)
    )
    stats = await worker.process_due()

    assert stats.failed == 1
    assert "not configured" in (repos.outbox.list_for_pair(pair.id)[0].last_error or "")


async def test_a_webp_sticker_is_rasterized_and_sent_as_a_photo(
    tmp_path: Path,
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_store: TempMediaStore,
) -> None:
    """With Pillow available a Bale-bound sticker becomes a PNG photo."""
    pillow = pytest.importorskip("PIL.Image")
    _, _, _ = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    source = tmp_path / "sticker.webp"
    pillow.new("RGBA", (64, 64), (0, 0, 255, 255)).save(source, format="WEBP")
    senders[Platform.TELEGRAM].download_data = source.read_bytes()
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        store=media_store,
        policy=MediaPolicy(can_convert_stickers=True),
    )

    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.STICKER,
            message_id=10,
            mime_type="image/webp",
            file_size=len(source.read_bytes()),
        )
    )
    stats = await worker.process_due()

    assert stats.delivered == 1
    (call,) = senders[Platform.BALE].media
    assert call.kinds == (MediaKind.PHOTO,)
    sent = call.items[0]
    assert sent.mime_type == "image/png" and sent.file_name is not None
    assert sent.path.suffix == ".png"
    assert media_store.files() == ()


async def test_an_unreadable_sticker_degrades_to_a_placeholder(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_store: TempMediaStore,
) -> None:
    """A corrupt attachment must become text, never crash the delivery loop."""
    _, _, _ = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        store=media_store,
        policy=MediaPolicy(can_convert_stickers=True),
    )
    senders[Platform.TELEGRAM].download_data = b"definitely not an image"

    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.STICKER,
            message_id=10,
            mime_type="image/webp",
            file_size=22,
        )
    )
    stats = await worker.process_due()

    assert stats.delivered == 1
    assert senders[Platform.BALE].media == []
    assert any("sticker" in text for text in senders[Platform.BALE].texts_to(BALE_USER))
    assert media_store.files() == ()


# -------------------------------------------------------------------- retries


async def test_media_timeout_is_terminal_to_avoid_duplicate_uploads(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
) -> None:
    """A timeout after an upload may have succeeded; retrying duplicates it."""
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)

    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.PHOTO,
            message_id=10,
            mime_type="image/jpeg",
            file_size=100,
        )
    )
    senders[Platform.BALE].media_error = TransportError("Timed out")

    stats = await media_worker.process_due()

    assert stats.failed == 1
    (entry,) = repos.outbox.list_for_pair(pair.id)
    assert entry.status is OutboxStatus.FAILED
    assert entry.attempts == 1, "an ambiguous media timeout is not retried"
    assert "not retried" in (entry.last_error or "")


async def test_a_transient_upload_failure_keeps_the_spooled_file_for_the_retry(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_store: TempMediaStore,
    media_policy: MediaPolicy,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    now = [FIXED_NOW]

    def clock() -> object:
        return now[0]

    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=clock,  # type: ignore[arg-type]
        retry_delay=timedelta(seconds=10),
        random_source=lambda: 0.5,
        store=media_store,
        policy=media_policy,
    )
    await bridge.handle_message(
        media_message(
            Platform.TELEGRAM,
            TELEGRAM_USER,
            MediaKind.PHOTO,
            message_id=10,
            mime_type="image/jpeg",
            file_size=100,
        )
    )
    senders[Platform.BALE].media_error = TransportError("bale down")

    first = await worker.process_due()

    assert first.retried == 1
    (entry,) = repos.outbox.list_for_pair(pair.id)
    assert entry.status is OutboxStatus.PENDING
    spooled = media_store.files()
    assert len(spooled) == 1, "the download survives for the retry"
    assert senders[Platform.TELEGRAM].downloads, "downloaded once"

    senders[Platform.BALE].media_error = None
    now[0] = FIXED_NOW + timedelta(seconds=11)
    second = await worker.process_due()

    assert second.delivered == 1
    assert senders[Platform.BALE].media[0].items[0].path == spooled[0], "reused, not re-downloaded"
    assert len(senders[Platform.TELEGRAM].downloads) == 1
    assert media_store.files() == ()


async def test_a_failed_download_leaves_no_partial_file(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    media_store: TempMediaStore,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    await bridge.handle_message(
        media_message(Platform.TELEGRAM, TELEGRAM_USER, MediaKind.PHOTO, message_id=10)
    )
    senders[Platform.TELEGRAM].download_error = TransportError("source down")

    stats = await media_worker.process_due()

    assert stats.retried == 1
    assert repos.outbox.list_for_pair(pair.id)[0].status is OutboxStatus.PENDING
    assert media_store.files() == (), "a partial download is never kept"


# --------------------------------------------------------------------- albums


async def relay_album(
    bridge: Bridge,
    collector: MediaGroupCollector,
    *,
    count: int = 3,
    base_message_id: int = 100,
) -> None:
    """Feed ``count`` album members through the collector and flush them."""
    for index in range(count):
        await collector.add(
            media_message(
                Platform.TELEGRAM,
                TELEGRAM_USER,
                MediaKind.PHOTO,
                message_id=base_message_id + index,
                file_id=f"photo-{index}",
                text=f"کپشن {index}",
                mime_type="image/jpeg",
                file_size=100,
                media_group_id="group-1",
            )
        )
    await collector.drain()


def build_collector(bridge: Bridge) -> MediaGroupCollector:
    async def no_wait(_: float) -> None:
        return None

    return MediaGroupCollector(window=0.5, flush=bridge.handle_messages, sleep=no_wait)


async def test_an_album_is_grouped_into_one_message_in_order(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    media_store: TempMediaStore,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    collector = build_collector(bridge)

    await relay_album(bridge, collector)
    queued = payloads(repos, pair)

    assert len(queued) == 3, "one outbox entry per album member"
    assert {payload["album_id"] for payload in queued} == {"telegram:5001:group-1"}
    assert [payload["album_index"] for payload in queued] == [0, 1, 2]

    stats = await media_worker.process_due()

    assert stats.delivered == 3
    (call,) = senders[Platform.BALE].media
    assert call.album is True
    assert call.kinds == (MediaKind.PHOTO,) * 3
    assert call.captions == ("کپشن 0", "کپشن 1", "کپشن 2"), "order is preserved"
    assert media_store.files() == ()
    # Every member got its own delivered id recorded.
    records = repos.messages.list_for_pair(pair.id)
    assert len(records) == 3
    assert all(record.target_message_id is not None for record in records)


async def test_an_album_degrades_to_separate_messages_when_the_target_has_none(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_store: TempMediaStore,
) -> None:
    _, _, pair = await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    collector = build_collector(bridge)
    no_albums = MediaPolicy(
        limits={
            Platform.TELEGRAM: PlatformMediaLimits(20 * MEGABYTE, 50 * MEGABYTE, 10 * MEGABYTE),
            Platform.BALE: PlatformMediaLimits(
                20 * MEGABYTE, 50 * MEGABYTE, 10 * MEGABYTE, supports_albums=False
            ),
        }
    )
    worker = OutboxWorker(
        users=users,
        messages=repos.messages,
        outbox=repos.outbox,
        tx=repos.db,
        senders=senders,
        clock=lambda: FIXED_NOW,
        store=media_store,
        policy=no_albums,
    )

    await relay_album(bridge, collector)
    stats = await worker.process_due()

    assert stats.delivered == 3
    calls = senders[Platform.BALE].media
    assert len(calls) == 3, "three separate messages, not one album"
    assert all(call.album is False for call in calls)
    assert [call.captions[0] for call in calls] == ["کپشن 0", "کپشن 1", "کپشن 2"]
    assert media_store.files() == ()


async def test_a_single_message_never_becomes_an_album() -> None:
    class Entry:
        def __init__(self, entry_id: int, album_id: str | None) -> None:
            self.id = entry_id
            self.payload = (
                {"kind": RELAY_MEDIA_KIND, "album_id": album_id} if album_id else {"kind": "relay_text"}
            )

    entries = [Entry(1, None), Entry(2, "a"), Entry(3, "a"), Entry(4, None), Entry(5, "b")]

    groups = coalesce_albums(entries)  # type: ignore[arg-type]

    assert [[entry.id for entry in group] for group in groups] == [[1], [2, 3], [4], [5]]


# ------------------------------------------------------------------- logging


async def test_media_logs_carry_no_content(
    bridge: Bridge,
    users: UserService,
    pairing: PairingService,
    repos: Repositories,
    senders: dict[Platform, RecordingSender],
    media_worker: OutboxWorker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await make_pair(bridge, users, pairing, repos)
    clear_senders(senders)
    secret = "این کپشن محرمانه است"
    file_name = "top-secret.pdf"

    with caplog.at_level(logging.INFO):
        await bridge.handle_message(
            media_message(
                Platform.TELEGRAM,
                TELEGRAM_USER,
                MediaKind.DOCUMENT,
                message_id=10,
                text=secret,
                file_name=file_name,
                mime_type="application/pdf",
                file_size=1234,
            )
        )
        await media_worker.process_due()

    assert "relay queued" in caplog.text
    assert "bale media sent" in caplog.text or "outbox delivered" in caplog.text
    for leak in (secret, file_name, "کپشن"):
        assert leak not in caplog.text
