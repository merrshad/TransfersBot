"""Tests for :mod:`core.models` value objects."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from core.models import (
    MediaItem,
    MediaKind,
    MessageRecord,
    MessageRef,
    OutboxEntry,
    OutboxStatus,
    Platform,
    UnifiedMessage,
)

UTC = UTC


def test_platform_values_are_the_wire_names() -> None:
    assert Platform.TELEGRAM.value == "telegram"
    assert Platform.BALE.value == "bale"
    assert Platform("bale") is Platform.BALE


def test_media_kind_matches_messenger_attachment_names() -> None:
    assert {kind.value for kind in MediaKind} == {
        "photo",
        "video",
        "audio",
        "voice",
        "document",
        "sticker",
        "animation",
        "video_note",
    }


def test_media_item_accepts_bytes_payload() -> None:
    item = MediaItem(kind=MediaKind.PHOTO, file_name="a.jpg", mime_type="image/jpeg", data=b"\x01")

    assert item.data == b"\x01"
    assert item.url is None
    assert item.file_size is None


def test_media_item_accepts_url_payload() -> None:
    item = MediaItem(kind=MediaKind.DOCUMENT, url="https://example.test/f.pdf", file_size=10)

    assert item.url == "https://example.test/f.pdf"
    assert item.data is None


def test_media_item_requires_exactly_one_payload() -> None:
    with pytest.raises(ValueError):
        MediaItem(kind=MediaKind.PHOTO)
    with pytest.raises(ValueError):
        MediaItem(kind=MediaKind.PHOTO, data=b"x", url="https://example.test/x")


def test_media_item_rejects_empty_data_and_blank_url() -> None:
    with pytest.raises(ValueError):
        MediaItem(kind=MediaKind.PHOTO, data=b"")
    with pytest.raises(ValueError):
        MediaItem(kind=MediaKind.PHOTO, url="   ")


def test_media_item_rejects_negative_size() -> None:
    with pytest.raises(ValueError):
        MediaItem(kind=MediaKind.PHOTO, url="https://example.test/x", file_size=-1)


def test_media_item_is_immutable() -> None:
    item = MediaItem(kind=MediaKind.PHOTO, data=b"x")

    with pytest.raises((AttributeError, TypeError)):
        item.kind = MediaKind.VIDEO  # type: ignore[misc]


def test_unified_message_key_matches_storage_unique_index() -> None:
    message = UnifiedMessage(
        source=Platform.BALE,
        sender_id=7,
        chat_id=42,
        message_id=99,
    )

    assert message.key == ("bale", 42, 99)


def test_unified_message_defaults() -> None:
    message = UnifiedMessage(
        source=Platform.TELEGRAM,
        sender_id=1,
        chat_id=2,
        message_id=3,
    )

    assert message.text is None
    assert message.entities == ()
    assert message.media == ()
    assert message.reply_to is None
    assert message.has_media is False
    assert message.timestamp.tzinfo is not None


def test_unified_message_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError):
        UnifiedMessage(
            source=Platform.TELEGRAM,
            sender_id=1,
            chat_id=2,
            message_id=3,
            timestamp=datetime(2026, 1, 1),
        )


def test_unified_message_carries_media_entities_and_reply() -> None:
    reply = MessageRef(platform=Platform.TELEGRAM, chat_id=10, message_id=11)
    message = UnifiedMessage(
        source=Platform.TELEGRAM,
        sender_id=1,
        chat_id=10,
        message_id=12,
        text="hello",
        entities=({"type": "bold", "offset": 0, "length": 5},),
        media=(MediaItem(kind=MediaKind.PHOTO, data=b"img"),),
        reply_to=reply,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=1),
    )

    assert message.has_media is True
    assert message.reply_to == reply
    assert message.entities[0]["type"] == "bold"


def test_unified_messages_compare_by_value() -> None:
    kwargs = dict(source=Platform.BALE, sender_id=1, chat_id=2, message_id=3)
    moment = datetime(2026, 1, 1, tzinfo=UTC)

    assert UnifiedMessage(**kwargs, timestamp=moment) == UnifiedMessage(**kwargs, timestamp=moment)


def test_message_record_keeps_pair_scope() -> None:
    record = MessageRecord(
        id=1,
        pair_id=5,
        source_platform=Platform.TELEGRAM,
        source_chat_id=10,
        source_message_id=11,
        target_message_id=None,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert record.pair_id == 5
    assert record.target_message_id is None


def test_outbox_entry_defaults_belong_to_a_pair() -> None:
    entry = OutboxEntry(
        id=1,
        pair_id=2,
        payload={"text": "hi"},
        attempts=0,
        next_attempt_at=datetime(2026, 1, 1, tzinfo=UTC),
        status=OutboxStatus.PENDING,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert entry.status is OutboxStatus.PENDING
    assert entry.last_error is None
    assert entry.payload["text"] == "hi"
