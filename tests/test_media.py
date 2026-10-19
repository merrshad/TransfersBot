"""Tests for :mod:`core.media`: limits, plans, placeholders, spool, albums."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from core.errors import MediaTooLargeError, MediaUnsupportedError
from core.media import (
    BALE_MEDIA_LIMITS,
    MEGABYTE,
    STICKER_PLACEHOLDER,
    TELEGRAM_MEDIA_LIMITS,
    MediaGroupCollector,
    MediaPolicy,
    PlatformMediaLimits,
    TempMediaStore,
    can_send_album,
    check_download_size,
    check_upload_size,
    convert_sticker_to_png,
    describe_contact,
    describe_location,
    describe_poll,
    kind_label,
    media_item_from_payload,
    media_item_payload,
    plan_album,
    plan_media_delivery,
    policy_from_settings,
    unsupported_placeholder,
)
from core.models import MediaItem, MediaKind, Platform, UnifiedMessage
from tests.conftest import FIXED_NOW

CONVERTING = MediaPolicy(can_convert_stickers=True)
PLAIN = MediaPolicy(can_convert_stickers=False)


def item(kind: MediaKind, **kwargs: object) -> MediaItem:
    """A media item with a platform handle (the shape adapters produce)."""
    return MediaItem(kind=kind, file_id="file-1", **kwargs)  # type: ignore[arg-type]


# ------------------------------------------------------------------- limits


def test_documented_defaults_are_what_the_docs_say() -> None:
    assert TELEGRAM_MEDIA_LIMITS.max_download_bytes == 20 * MEGABYTE
    assert BALE_MEDIA_LIMITS.max_download_bytes == 20 * MEGABYTE
    for limits in (TELEGRAM_MEDIA_LIMITS, BALE_MEDIA_LIMITS):
        assert limits.max_upload_bytes == 50 * MEGABYTE
        assert limits.max_photo_upload_bytes == 10 * MEGABYTE
        assert limits.supports_albums is True
        assert (limits.album_min_items, limits.album_max_items) == (2, 10)
    # Telegram documents stickers and video notes; Bale documents neither.
    assert TELEGRAM_MEDIA_LIMITS.supports_stickers is True
    assert BALE_MEDIA_LIMITS.supports_stickers is False
    assert BALE_MEDIA_LIMITS.supports_video_notes is False


def test_upload_limit_is_stricter_for_photos() -> None:
    assert TELEGRAM_MEDIA_LIMITS.upload_limit_for(MediaKind.PHOTO) == 10 * MEGABYTE
    assert TELEGRAM_MEDIA_LIMITS.upload_limit_for(MediaKind.VIDEO) == 50 * MEGABYTE


def test_sticker_limit_depends_on_the_format() -> None:
    assert TELEGRAM_MEDIA_LIMITS.sticker_limit_for("image/webp") == 512 * 1024
    assert TELEGRAM_MEDIA_LIMITS.sticker_limit_for("video/webm") == 256 * 1024
    assert TELEGRAM_MEDIA_LIMITS.sticker_limit_for(None) == 512 * 1024


def test_policy_from_settings_reads_every_byte_ceiling() -> None:
    class FakeSettings:
        telegram_max_download_mb = 2
        telegram_max_upload_mb = 5
        telegram_max_photo_upload_mb = 1
        bale_max_download_mb = 3
        bale_max_upload_mb = 6
        bale_max_photo_upload_mb = 2

    policy = policy_from_settings(FakeSettings())

    telegram = policy.for_platform(Platform.TELEGRAM)
    bale = policy.for_platform(Platform.BALE)
    assert telegram.max_download_bytes == 2 * MEGABYTE
    assert telegram.max_photo_upload_bytes == 1 * MEGABYTE
    assert bale.max_download_bytes == 3 * MEGABYTE
    assert bale.max_upload_bytes == 6 * MEGABYTE
    # Capabilities stay tied to the API, not to the settings.
    assert telegram.supports_stickers is True
    assert bale.supports_stickers is False


@pytest.mark.parametrize(
    ("size", "kind", "platform", "raises"),
    [
        (9 * MEGABYTE, MediaKind.PHOTO, Platform.BALE, False),
        (11 * MEGABYTE, MediaKind.PHOTO, Platform.BALE, True),
        (11 * MEGABYTE, MediaKind.VIDEO, Platform.BALE, False),
        (51 * MEGABYTE, MediaKind.DOCUMENT, Platform.TELEGRAM, True),
    ],
)
def test_check_upload_size(
    size: int, kind: MediaKind, platform: Platform, raises: bool
) -> None:
    if raises:
        with pytest.raises(MediaTooLargeError) as excinfo:
            check_upload_size(
                size_bytes=size, kind=kind, target_platform=platform, policy=PLAIN
            )
        assert excinfo.value.size_bytes == size
    else:
        check_upload_size(
            size_bytes=size, kind=kind, target_platform=platform, policy=PLAIN
        )


def test_check_download_size_uses_the_platform_cap() -> None:
    check_download_size(size_bytes=None, source_platform=Platform.BALE, policy=PLAIN)
    check_download_size(
        size_bytes=20 * MEGABYTE, source_platform=Platform.BALE, policy=PLAIN
    )
    with pytest.raises(MediaTooLargeError):
        check_download_size(
            size_bytes=21 * MEGABYTE, source_platform=Platform.BALE, policy=PLAIN
        )


# -------------------------------------------------------------------- plans


@pytest.mark.parametrize(
    ("kind", "platform", "expected_kind", "action"),
    [
        (MediaKind.PHOTO, Platform.BALE, MediaKind.PHOTO, "relay"),
        (MediaKind.VIDEO, Platform.BALE, MediaKind.VIDEO, "relay"),
        (MediaKind.ANIMATION, Platform.BALE, MediaKind.ANIMATION, "relay"),
        (MediaKind.DOCUMENT, Platform.BALE, MediaKind.DOCUMENT, "relay"),
        (MediaKind.AUDIO, Platform.BALE, MediaKind.AUDIO, "relay"),
        (MediaKind.VOICE, Platform.BALE, MediaKind.VOICE, "relay"),
        (MediaKind.VIDEO_NOTE, Platform.BALE, MediaKind.VIDEO, "relay"),
        (MediaKind.PHOTO, Platform.TELEGRAM, MediaKind.PHOTO, "relay"),
        (MediaKind.VIDEO_NOTE, Platform.TELEGRAM, MediaKind.VIDEO_NOTE, "relay"),
        (MediaKind.VOICE, Platform.TELEGRAM, MediaKind.VOICE, "relay"),
    ],
)
def test_relayable_kinds(
    kind: MediaKind, platform: Platform, expected_kind: MediaKind, action: str
) -> None:
    plan = plan_media_delivery(item(kind), platform, PLAIN)

    assert plan.action == action
    assert plan.kind is expected_kind


def test_video_note_becomes_a_video_when_the_target_has_no_video_notes() -> None:
    plan = plan_media_delivery(item(MediaKind.VIDEO_NOTE), Platform.BALE, PLAIN)

    assert plan.kind is MediaKind.VIDEO
    assert plan.reason == "video_note_as_video"


def test_sticker_relays_when_the_target_supports_it() -> None:
    plan = plan_media_delivery(
        item(MediaKind.STICKER, mime_type="image/webp", file_size=1000),
        Platform.TELEGRAM,
        PLAIN,
    )

    assert plan.action == "relay"
    assert plan.kind is MediaKind.STICKER


def test_oversized_sticker_is_converted_when_a_converter_exists() -> None:
    plan = plan_media_delivery(
        item(MediaKind.STICKER, mime_type="image/webp", file_size=600 * 1024),
        Platform.TELEGRAM,
        CONVERTING,
    )

    assert plan.action == "convert"
    assert plan.kind is MediaKind.PHOTO


def test_sticker_to_bale_becomes_a_photo_when_conversion_is_possible() -> None:
    plan = plan_media_delivery(
        item(MediaKind.STICKER, mime_type="image/webp", file_size=1000),
        Platform.BALE,
        CONVERTING,
    )

    assert plan.action == "convert"
    assert plan.kind is MediaKind.PHOTO


def test_sticker_falls_back_to_a_placeholder_without_a_converter() -> None:
    plan = plan_media_delivery(
        item(MediaKind.STICKER, mime_type="image/webp", emoji="🙂"),
        Platform.BALE,
        PLAIN,
    )

    assert plan.is_placeholder
    assert plan.text == STICKER_PLACEHOLDER.format(emoji=" 🙂")
    assert plan.reason == "sticker_unsupported"


def test_animated_sticker_without_conversion_is_a_placeholder() -> None:
    plan = plan_media_delivery(
        item(MediaKind.STICKER, mime_type="application/x-tgsticker"),
        Platform.BALE,
        CONVERTING,
    )

    assert plan.is_placeholder


@pytest.mark.parametrize(
    ("kind", "mime"),
    [
        (MediaKind.PHOTO, "text/plain"),
        (MediaKind.VIDEO, "image/png"),
        (MediaKind.VOICE, "video/mp4"),
        (MediaKind.ANIMATION, "audio/mpeg"),
    ],
)
def test_mismatched_mime_becomes_a_placeholder(kind: MediaKind, mime: str) -> None:
    plan = plan_media_delivery(item(kind, mime_type=mime), Platform.BALE, PLAIN)

    assert plan.is_placeholder, "a contradicting mime type must not reach the API"
    assert mime in (plan.text or "")
    assert plan.reason == "mime_mismatch"
    assert plan.kind is None


def test_documents_accept_any_mime() -> None:
    plan = plan_media_delivery(
        item(MediaKind.DOCUMENT, mime_type="application/x-msdownload"),
        Platform.BALE,
        PLAIN,
    )

    assert plan.action == "relay"


# -------------------------------------------------------------------- albums


def plans_for(*kinds: MediaKind) -> list:
    return [plan_media_delivery(item(kind), Platform.TELEGRAM, PLAIN) for kind in kinds]


def test_album_groups_photos_and_videos_together() -> None:
    plans = plans_for(MediaKind.PHOTO, MediaKind.VIDEO)

    assert plan_album(plans) is MediaKind.VIDEO
    assert can_send_album(plans, Platform.TELEGRAM, PLAIN) is True


def test_album_requires_a_single_document_or_audio_type_family() -> None:
    assert plan_album(plans_for(MediaKind.DOCUMENT, MediaKind.DOCUMENT)) is MediaKind.DOCUMENT
    assert plan_album(plans_for(MediaKind.AUDIO, MediaKind.AUDIO)) is MediaKind.AUDIO
    assert plan_album(plans_for(MediaKind.PHOTO, MediaKind.DOCUMENT)) is None
    assert plan_album(plans_for(MediaKind.AUDIO, MediaKind.DOCUMENT)) is None


def test_album_size_bounds_are_enforced() -> None:
    single = plans_for(MediaKind.PHOTO)
    eleven = plans_for(*([MediaKind.PHOTO] * 11))

    assert can_send_album(single, Platform.TELEGRAM, PLAIN) is False
    assert can_send_album(eleven, Platform.TELEGRAM, PLAIN) is False
    assert can_send_album(plans_for(*([MediaKind.PHOTO] * 10)), Platform.TELEGRAM, PLAIN)


def test_album_requires_a_platform_that_supports_it() -> None:
    plans = plans_for(MediaKind.PHOTO, MediaKind.PHOTO)
    no_albums = MediaPolicy(
        limits={
            Platform.TELEGRAM: PlatformMediaLimits(1, 1, 1, supports_albums=False),
        }
    )

    assert can_send_album(plans, Platform.TELEGRAM, no_albums) is False


def test_placeholders_never_join_an_album() -> None:
    plans = plans_for(MediaKind.PHOTO) + [
        plan_media_delivery(item(MediaKind.STICKER), Platform.BALE, PLAIN)
    ]

    assert plan_album(plans) is None


# -------------------------------------------------------------- placeholders


def test_placeholders_and_text_renderings() -> None:
    assert unsupported_placeholder("venue") == "[unsupported message type: venue]"
    assert "51.50000" in describe_location(latitude=51.5, longitude=-0.12)
    contact = describe_contact(name=" Ali ", phone_number=" 0912 ")
    assert "Ali" in contact and "0912" in contact
    assert "@ali" in describe_contact(name="A", phone_number="1", username="@ali")
    poll = describe_poll(question=" کدام؟ ", options=["بله", "خیر"], poll_type="quiz")
    assert "کدام؟" in poll and "• بله" in poll and "quiz" in poll


def test_kind_labels_cover_every_kind() -> None:
    for kind in MediaKind:
        assert kind_label(kind)


# ------------------------------------------------------------- serialization


def test_payload_round_trip_keeps_metadata() -> None:
    original = MediaItem(
        kind=MediaKind.VIDEO,
        file_id="abc",
        file_name="clip.mp4",
        mime_type="video/mp4",
        file_size=1234,
        duration=9.5,
        width=640,
        height=480,
        caption="کپشن",
    )

    rebuilt = media_item_from_payload(media_item_payload(original))

    assert rebuilt == original


def test_payload_round_trip_keeps_in_memory_bytes() -> None:
    original = MediaItem(kind=MediaKind.PHOTO, data=b"\x00\x01\x02", mime_type="image/png")

    rebuilt = media_item_from_payload(media_item_payload(original))

    assert rebuilt.data == b"\x00\x01\x02"


@pytest.mark.parametrize(
    "raw",
    [
        "not-an-object",
        {"kind": "nope", "file_id": "x"},
        {"kind": "photo"},
        {"kind": "photo", "file_id": "x", "data": "!!!not-base64!!!"},
    ],
)
def test_malformed_payload_items_are_rejected(raw: object) -> None:
    with pytest.raises(ValueError):
        media_item_from_payload(raw)


# ------------------------------------------------------------------- spool


def test_spool_paths_are_deterministic_and_quota_aware(tmp_path: Path) -> None:
    store = TempMediaStore(tmp_path, quota_bytes=1000)

    path = store.path_for(user_id=7, entry_id=3, index=0, suffix=".jpg")
    assert path == tmp_path / "u7" / "3-0.jpg"

    store.prepare(path).write_bytes(b"x" * 600)
    assert store.usage(7) == 600
    assert store.has_room_for(7, 400) is True
    assert store.has_room_for(7, 401) is False
    assert store.has_room_for(8, 1000) is True


def test_discarding_a_file_removes_its_directory(tmp_path: Path) -> None:
    store = TempMediaStore(tmp_path, quota_bytes=1000)
    path = store.prepare(store.path_for(user_id=1, entry_id=1, index=0))
    path.write_bytes(b"data")

    assert store.discard(path) is True
    assert store.files() == ()
    assert not (tmp_path / "u1").exists()
    assert store.discard(path) is False, "already gone"


def test_discard_all_and_total_bytes(tmp_path: Path) -> None:
    store = TempMediaStore(tmp_path, quota_bytes=10_000)
    for index in range(3):
        store.prepare(store.path_for(user_id=2, entry_id=1, index=index)).write_bytes(b"ab")

    assert store.total_bytes() == 6
    assert len(store.files()) == 3
    assert store.discard_all(2) == 3
    assert store.files() == ()
    assert store.discard_all(2) == 0


def test_sweep_removes_only_old_files(tmp_path: Path) -> None:
    store = TempMediaStore(tmp_path, quota_bytes=10_000)
    fresh = store.prepare(store.path_for(user_id=1, entry_id=1, index=0))
    stale = store.prepare(store.path_for(user_id=1, entry_id=2, index=0))
    fresh.write_bytes(b"a")
    stale.write_bytes(b"b")
    import os

    old = 1_000_000.0
    os.utime(stale, (old, old))

    removed = store.sweep(max_age_seconds=3600, now=old + 7200)

    assert removed == 1
    assert store.files() == (fresh,)


def test_store_rejects_a_non_positive_quota(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        TempMediaStore(tmp_path, quota_bytes=0)


# ---------------------------------------------------------------- conversion


def test_sticker_conversion_writes_a_png(tmp_path: Path) -> None:
    pillow = pytest.importorskip("PIL.Image")
    source = tmp_path / "sticker.webp"
    pillow.new("RGBA", (64, 64), (255, 0, 0, 255)).save(source, format="WEBP")
    destination = tmp_path / "sticker.png"

    convert_sticker_to_png(source, destination)

    with pillow.open(destination) as converted:
        assert converted.format == "PNG"


def test_conversion_without_pillow_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core import media as media_module

    monkeypatch.setattr(media_module, "sticker_converter", lambda: None)
    with pytest.raises(MediaUnsupportedError) as excinfo:
        convert_sticker_to_png(tmp_path / "missing.webp", tmp_path / "out.png")

    assert excinfo.value.code == "media_unsupported"


# ----------------------------------------------------------------- collector


def inbound(text: str, *, message_id: int, group: str | None = None) -> UnifiedMessage:
    return UnifiedMessage(
        source=Platform.TELEGRAM,
        sender_id=5001,
        chat_id=5001,
        message_id=message_id,
        text=text,
        media_group_id=group,
        timestamp=FIXED_NOW,
    )


async def test_plain_messages_are_flushed_immediately() -> None:
    batches: list[tuple[UnifiedMessage, ...]] = []
    collector = MediaGroupCollector(
        window=5.0, flush=_recorder(batches), sleep=_never_sleep
    )

    await collector.add(inbound("سلام", message_id=1))

    assert [message.text for message in batches[0]] == ["سلام"]
    assert collector.pending_groups == 0


async def test_album_members_wait_for_the_window_and_keep_order() -> None:
    batches: list[tuple[UnifiedMessage, ...]] = []
    collector = MediaGroupCollector(
        window=0.5, flush=_recorder(batches), sleep=_immediate_sleep
    )

    await collector.add(inbound("۱", message_id=1, group="g1"))
    await collector.add(inbound("۲", message_id=2, group="g1"))
    await collector.add(inbound("۳", message_id=3, group="g1"))
    await _settle()

    assert len(batches) == 1
    assert [message.text for message in batches[0]] == ["۱", "۲", "۳"]
    assert collector.pending_groups == 0


async def test_two_groups_are_flushed_separately() -> None:
    batches: list[tuple[UnifiedMessage, ...]] = []
    collector = MediaGroupCollector(
        window=0.5, flush=_recorder(batches), sleep=_immediate_sleep
    )

    await collector.add(inbound("a", message_id=1, group="g1"))
    await collector.add(inbound("b", message_id=2, group="g2"))
    await _settle()

    assert sorted(len(batch) for batch in batches) == [1, 1]


async def test_drain_flushes_buffered_groups() -> None:
    batches: list[tuple[UnifiedMessage, ...]] = []
    collector = MediaGroupCollector(
        window=60.0, flush=_recorder(batches), sleep=_never_sleep
    )

    await collector.add(inbound("۱", message_id=1, group="g1"))
    await collector.add(inbound("۲", message_id=2, group="g1"))
    await collector.drain()

    assert len(batches) == 1
    assert len(batches[0]) == 2
    assert collector.pending_groups == 0


def test_group_key_ignores_messages_without_a_group() -> None:
    assert MediaGroupCollector.key(inbound("x", message_id=1)) is None
    assert MediaGroupCollector.key(inbound("x", message_id=2, group="g")) == (
        "telegram",
        5001,
        "g",
    )


def test_collector_rejects_a_negative_window() -> None:
    with pytest.raises(ValueError):
        MediaGroupCollector(window=-1, flush=_noop_flush)


async def _noop_flush(_: tuple[UnifiedMessage, ...]) -> None:
    return None


def _recorder(batches: list[tuple[UnifiedMessage, ...]]):
    async def flush(batch: tuple[UnifiedMessage, ...]) -> None:
        batches.append(batch)

    return flush


async def _immediate_sleep(_: float) -> None:
    await asyncio.sleep(0)


async def _never_sleep(_: float) -> None:
    await asyncio.sleep(3600)


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)
