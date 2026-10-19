"""Media paths of both adapters: parsing, downloading and sending, all offline."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
import pytest
from telegram import Bot, Message, ReplyParameters, Update

from adapters.bale import BaleAdapter, parse_update
from adapters.base import download_url_to_file
from adapters.telegram import TelegramAdapter, TelegramFilePort, to_unified_message
from core.errors import ApiError, DeliveryError, MediaTooLargeError, MediaUnsupportedError
from core.media import MEGABYTE
from core.models import MediaItem, MediaKind, OutboundMedia, Platform

TELEGRAM_TOKEN = "123456:TESTTOKEN"
BALE_TOKEN = "SECRET-BALE-TOKEN"
BALE_BASE_URL = "https://tapi.bale.ai"
TELEGRAM_BOT = Bot(TELEGRAM_TOKEN)


# ------------------------------------------------------------------ fixtures


def telegram_update(**message: Any) -> Update:
    """A real ``telegram.Update`` built from a raw message payload."""
    payload: dict[str, Any] = {
        "message_id": 11,
        "chat": {"id": 5001, "type": "private"},
        "date": 1767225600,
        "from": {"id": 5001, "is_bot": False, "first_name": "Ali"},
    }
    payload.update(message)
    return Update.de_json({"update_id": 1, "message": payload}, TELEGRAM_BOT)


class FakeTelegramFile:
    """``telegram.File`` stand-in that writes scripted bytes to disk."""

    def __init__(self, data: bytes, *, file_size: int | None = None) -> None:
        self.data = data
        self.file_size: int | None = (
            len(data) if file_size is None else file_size
        )
        self.downloaded_to: Path | None = None

    async def download_to_drive(self, custom_path: str | Path | None = None) -> Path:
        path = Path(custom_path) if custom_path is not None else Path("out.bin")
        path.write_bytes(self.data)
        self.downloaded_to = path
        return path


class FakeMediaBot:
    """Bot double covering ``getFile`` and every media ``send*`` method."""

    def __init__(
        self,
        *,
        file: FakeTelegramFile | None = None,
        get_file_error: Exception | None = None,
        send_error: Exception | None = None,
    ) -> None:
        self.file = file
        self.get_file_error = get_file_error
        self.send_error = send_error
        self.get_file_calls: list[str] = []
        self.media_calls: list[dict[str, Any]] = []
        self.group_calls: list[dict[str, Any]] = []
        self._next_id = 500

    # -- polling/message stubs (protocol completeness) --------------------

    async def get_updates(
        self,
        offset: int | None = None,
        limit: int | None = None,
        timeout: int | None = None,
        allowed_updates: Sequence[str] | None = None,
    ) -> tuple[Update, ...]:
        return ()

    async def send_message(self, chat_id: int, text: str, **kwargs: Any) -> Message:
        return self._record("send_message", chat_id=chat_id, text=text, **kwargs)

    # -- download ---------------------------------------------------------

    async def get_file(self, file_id: str) -> TelegramFilePort:
        self.get_file_calls.append(file_id)
        if self.get_file_error is not None:
            raise self.get_file_error
        assert self.file is not None, "no file scripted"
        return self.file

    # -- uploads ----------------------------------------------------------

    def _record(self, method: str, **kwargs: Any) -> Message:
        if self.send_error is not None:
            raise self.send_error
        body = kwargs.get(method_name_for(method))
        self.media_calls.append({"method": method, "kind": kwargs, "body": body})
        self._next_id += 1
        return Message.de_json(
            {
                "message_id": self._next_id,
                "date": 1767225600,
                "chat": {"id": kwargs["chat_id"], "type": "private"},
            },
            TELEGRAM_BOT,
        )

    async def send_photo(self, chat_id: int, photo: Any, **kwargs: Any) -> Message:
        return self._record("send_photo", chat_id=chat_id, photo=photo, **kwargs)

    async def send_video(self, chat_id: int, video: Any, **kwargs: Any) -> Message:
        return self._record("send_video", chat_id=chat_id, video=video, **kwargs)

    async def send_animation(self, chat_id: int, animation: Any, **kwargs: Any) -> Message:
        return self._record("send_animation", chat_id=chat_id, animation=animation, **kwargs)

    async def send_document(self, chat_id: int, document: Any, **kwargs: Any) -> Message:
        return self._record("send_document", chat_id=chat_id, document=document, **kwargs)

    async def send_audio(self, chat_id: int, audio: Any, **kwargs: Any) -> Message:
        return self._record("send_audio", chat_id=chat_id, audio=audio, **kwargs)

    async def send_voice(self, chat_id: int, voice: Any, **kwargs: Any) -> Message:
        return self._record("send_voice", chat_id=chat_id, voice=voice, **kwargs)

    async def send_video_note(
        self, chat_id: int, video_note: Any, **kwargs: Any
    ) -> Message:
        return self._record("send_video_note", chat_id=chat_id, video_note=video_note, **kwargs)

    async def send_sticker(self, chat_id: int, sticker: Any, **kwargs: Any) -> Message:
        return self._record("send_sticker", chat_id=chat_id, sticker=sticker, **kwargs)

    async def send_media_group(
        self, chat_id: int, media: Sequence[Any], **kwargs: Any
    ) -> tuple[Message, ...]:
        if self.send_error is not None:
            raise self.send_error
        self.group_calls.append({"chat_id": chat_id, "media": list(media), **kwargs})
        sent = []
        for _ in media:
            self._next_id += 1
            sent.append(
                Message.de_json(
                    {
                        "message_id": self._next_id,
                        "date": 1767225600,
                        "chat": {"id": chat_id, "type": "private"},
                    },
                    TELEGRAM_BOT,
                )
            )
        return tuple(sent)


def method_name_for(method: str) -> str:
    """First media keyword of a recorded call (``send_photo`` → ``photo``)."""
    return {
        "send_message": "text",
        "send_photo": "photo",
        "send_video": "video",
        "send_animation": "animation",
        "send_document": "document",
        "send_audio": "audio",
        "send_voice": "voice",
        "send_video_note": "video_note",
        "send_sticker": "sticker",
    }[method]


def build_telegram(
    **kwargs: Any,
) -> tuple[TelegramAdapter, FakeMediaBot]:
    bot = FakeMediaBot(**kwargs)
    return TelegramAdapter(TELEGRAM_TOKEN, bot=bot), bot


def spooled(tmp_path: Path, name: str = "media.bin", data: bytes = b"payload") -> Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


# ------------------------------------------------------------- telegram parse


def test_photo_message_picks_the_largest_size() -> None:
    message = to_unified_message(
        telegram_update(
            caption="کپشن",
            photo=[
                {"file_id": "small", "file_unique_id": "a", "width": 90, "height": 60, "file_size": 10},
                {"file_id": "big", "file_unique_id": "b", "width": 1280, "height": 720, "file_size": 90},
            ],
            media_group_id="group-1",
        )
    )

    assert message is not None
    assert message.text == "کپشن"
    assert message.media_group_id == "group-1"
    (photo,) = message.media
    assert photo.kind is MediaKind.PHOTO
    assert photo.file_id == "big"
    assert (photo.width, photo.height) == (1280, 720)


@pytest.mark.parametrize(
    ("field", "payload", "kind"),
    [
        (
            "video",
            {
                "file_id": "v",
                "file_unique_id": "u",
                "width": 640,
                "height": 480,
                "duration": 12,
                "mime_type": "video/mp4",
                "file_size": 2000,
            },
            MediaKind.VIDEO,
        ),
        (
            "animation",
            {
                "file_id": "a",
                "file_unique_id": "u",
                "width": 320,
                "height": 240,
                "duration": 3,
                "file_name": "fun.mp4",
                "mime_type": "video/mp4",
                "file_size": 300,
            },
            MediaKind.ANIMATION,
        ),
        (
            "document",
            {
                "file_id": "d",
                "file_unique_id": "u",
                "file_name": "notes.pdf",
                "mime_type": "application/pdf",
                "file_size": 400,
            },
            MediaKind.DOCUMENT,
        ),
        (
            "audio",
            {
                "file_id": "au",
                "file_unique_id": "u",
                "duration": 60,
                "performer": "P",
                "title": "T",
                "mime_type": "audio/mpeg",
                "file_size": 500,
            },
            MediaKind.AUDIO,
        ),
        (
            "voice",
            {"file_id": "vo", "file_unique_id": "u", "duration": 5, "mime_type": "audio/ogg", "file_size": 600},
            MediaKind.VOICE,
        ),
        (
            "video_note",
            {"file_id": "vn", "file_unique_id": "u", "length": 240, "duration": 6, "file_size": 700},
            MediaKind.VIDEO_NOTE,
        ),
    ],
)
def test_telegram_media_kinds_are_parsed(field: str, payload: dict[str, Any], kind: MediaKind) -> None:
    message = to_unified_message(telegram_update(**{field: payload}))

    assert message is not None
    (item,) = message.media
    assert item.kind is kind
    assert item.file_id == payload["file_id"]
    assert item.file_size == payload["file_size"]


@pytest.mark.parametrize(
    ("flags", "mime"),
    [
        ({"is_animated": False, "is_video": False}, "image/webp"),
        ({"is_animated": True, "is_video": False}, "application/x-tgsticker"),
        ({"is_animated": False, "is_video": True}, "video/webm"),
    ],
)
def test_sticker_flavour_selects_its_mime(flags: dict[str, Any], mime: str) -> None:
    message = to_unified_message(
        telegram_update(
            sticker={
                "file_id": "s",
                "file_unique_id": "u",
                "type": "regular",
                "width": 512,
                "height": 512,
                "emoji": "🙂",
                "file_size": 900,
                **flags,
            }
        )
    )

    assert message is not None
    (sticker,) = message.media
    assert sticker.mime_type == mime
    assert sticker.emoji == "🙂"


def test_location_and_contact_become_text() -> None:
    location = to_unified_message(
        telegram_update(location={"longitude": 51.4, "latitude": 35.7})
    )
    contact = to_unified_message(
        telegram_update(contact={"phone_number": "0912", "first_name": "Ali", "last_name": "B"})
    )

    assert location is not None and "35.70000" in (location.text or "")
    assert location.media == ()
    assert contact is not None and "0912" in (contact.text or "")


def test_unsupported_message_type_is_named() -> None:
    message = to_unified_message(
        telegram_update(
            venue={
                "location": {"longitude": 1.0, "latitude": 2.0},
                "title": "T",
                "address": "A",
            }
        )
    )

    assert message is not None
    assert message.unsupported == "venue"
    assert message.media == ()
    assert message.is_relayable is True


def test_message_without_content_is_still_skipped() -> None:
    assert to_unified_message(telegram_update()) is None


# ---------------------------------------------------------- telegram download


async def test_telegram_download_via_get_file(tmp_path: Path) -> None:
    file = FakeTelegramFile(b"telegram-bytes")
    adapter, bot = build_telegram(file=file)
    destination = tmp_path / "spool.bin"

    written = await adapter.download_media(
        MediaItem(kind=MediaKind.PHOTO, file_id="x"), destination
    )

    assert written == len(b"telegram-bytes")
    assert destination.read_bytes() == b"telegram-bytes"
    assert bot.get_file_calls == ["x"]


async def test_telegram_download_refuses_an_oversized_handle(tmp_path: Path) -> None:
    file = FakeTelegramFile(b"x", file_size=50 * MEGABYTE)
    adapter, bot = build_telegram(file=file)

    with pytest.raises(MediaTooLargeError) as excinfo:
        await adapter.download_media(
            MediaItem(kind=MediaKind.DOCUMENT, file_id="x"), tmp_path / "spool.bin"
        )

    assert excinfo.value.limit_bytes == 20 * MEGABYTE
    assert file.downloaded_to is None, "nothing is downloaded past the cap"


async def test_telegram_download_of_in_memory_and_url_sources(tmp_path: Path) -> None:
    adapter, _ = build_telegram()

    written = await adapter.download_media(
        MediaItem(kind=MediaKind.PHOTO, data=b"inline"), tmp_path / "inline.bin"
    )
    assert written == 6

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=b"remote-bytes")
    )
    async with httpx.AsyncClient(transport=transport) as client:
        url_written = await download_url_to_file(
            "https://example.test/file",
            tmp_path / "remote.bin",
            max_bytes=MEGABYTE,
            client=client,
        )
    assert url_written == 12
    assert (tmp_path / "remote.bin").read_bytes() == b"remote-bytes"


async def test_download_url_aborts_when_the_body_exceeds_the_limit(tmp_path: Path) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=b"x" * 5000)
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(MediaTooLargeError):
            await download_url_to_file(
                "https://example.test/big",
                tmp_path / "big.bin",
                max_bytes=1000,
                client=client,
            )


# ------------------------------------------------------------ telegram send


@pytest.mark.parametrize(
    ("kind", "method"),
    [
        (MediaKind.PHOTO, "send_photo"),
        (MediaKind.VIDEO, "send_video"),
        (MediaKind.ANIMATION, "send_animation"),
        (MediaKind.DOCUMENT, "send_document"),
        (MediaKind.AUDIO, "send_audio"),
        (MediaKind.VOICE, "send_voice"),
        (MediaKind.VIDEO_NOTE, "send_video_note"),
        (MediaKind.STICKER, "send_sticker"),
    ],
)
async def test_telegram_sends_each_kind_with_its_method(
    tmp_path: Path, kind: MediaKind, method: str
) -> None:
    adapter, bot = build_telegram()
    item = OutboundMedia(
        kind=kind,
        path=spooled(tmp_path),
        file_name="f.bin",
        mime_type="application/octet-stream",
        file_size=7,
        caption="کپشن",
        emoji="🙂",
    )

    sent = await adapter.send_media(5001, item)

    assert bot.media_calls[0]["method"] == method
    assert sent.chat_id == 5001
    kwargs = bot.media_calls[0]["kind"]
    if kind in {MediaKind.STICKER, MediaKind.VIDEO_NOTE}:
        assert "caption" not in kwargs, "stickers and video notes take no caption"
    else:
        assert kwargs["caption"] == "کپشن"


async def test_telegram_send_media_clamps_a_long_caption(tmp_path: Path) -> None:
    adapter, bot = build_telegram()
    item = OutboundMedia(
        kind=MediaKind.PHOTO, path=spooled(tmp_path), caption="x" * 2000
    )

    await adapter.send_media(5001, item)

    assert len(bot.media_calls[0]["kind"]["caption"]) == 1024


async def test_telegram_send_media_replies_when_asked(tmp_path: Path) -> None:
    adapter, bot = build_telegram()
    item = OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path))

    await adapter.send_media(5001, item, reply_to_message_id=77)

    assert bot.media_calls[0]["kind"]["reply_parameters"] == ReplyParameters(message_id=77)


async def test_telegram_send_media_maps_errors(tmp_path: Path) -> None:
    from telegram.error import Forbidden

    adapter, _ = build_telegram(send_error=Forbidden("bot was blocked"))
    item = OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path))

    with pytest.raises(DeliveryError):
        await adapter.send_media(5001, item)


async def test_telegram_album_keeps_order_and_returns_one_id_per_item(
    tmp_path: Path,
) -> None:
    adapter, bot = build_telegram()
    items = (
        OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path, "a.jpg"), caption="۱"),
        OutboundMedia(kind=MediaKind.VIDEO, path=spooled(tmp_path, "b.mp4"), caption="۲"),
    )

    sent = await adapter.send_media_group(5001, items, reply_to_message_id=9)

    assert len(sent) == 2
    call = bot.group_calls[0]
    assert [type(media).__name__ for media in call["media"]] == [
        "InputMediaPhoto",
        "InputMediaVideo",
    ]
    assert [media.caption for media in call["media"]] == ["۱", "۲"]
    assert call["reply_parameters"] == ReplyParameters(message_id=9)


def track_opened_handles(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Record every ``Path.open`` result so a test can assert it was closed."""
    opened: list[Any] = []
    real_open = Path.open

    def tracking_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        handle = real_open(self, *args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", tracking_open)
    return opened


async def test_telegram_send_media_closes_its_file_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    item = OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path))
    opened = track_opened_handles(monkeypatch)
    adapter, _ = build_telegram()

    await adapter.send_media(5001, item)

    assert len(opened) == 1, "exactly one handle for one attachment"
    assert opened[0].closed is True


async def test_telegram_album_closes_every_file_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    items = (
        OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path, "a.jpg")),
        OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path, "b.jpg")),
    )
    opened = track_opened_handles(monkeypatch)
    adapter, _ = build_telegram()

    await adapter.send_media_group(5001, items)

    assert len(opened) == 2, "one handle per album item"
    assert all(handle.closed for handle in opened)


async def test_telegram_album_needs_two_items(tmp_path: Path) -> None:
    adapter, _ = build_telegram()

    with pytest.raises(ApiError):
        await adapter.send_media_group(
            5001, (OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path)),)
        )


# ---------------------------------------------------------------- bale parse


def bale_raw(**message: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "message_id": 21,
        "chat": {"id": 7001, "type": "private"},
        "date": 1767225600,
        "from": {"id": 7001, "is_bot": False, "first_name": "Ali"},
    }
    payload.update(message)
    return {"update_id": 1, "message": payload}


def test_bale_photo_picks_the_largest_size() -> None:
    message = parse_update(
        bale_raw(
            caption="کپشن",
            media_group_id="g-1",
            photo=[
                {"file_id": "small", "file_size": 5, "width": 90, "height": 60},
                {"file_id": "big", "file_size": 50, "width": 1280, "height": 720},
            ],
        )
    )

    assert message is not None
    assert message.text == "کپشن"
    assert message.media_group_id == "g-1"
    (photo,) = message.media
    assert photo.file_id == "big"
    assert photo.mime_type == "image/jpeg"


@pytest.mark.parametrize(
    ("field", "kind"),
    [
        ("video", MediaKind.VIDEO),
        ("animation", MediaKind.ANIMATION),
        ("document", MediaKind.DOCUMENT),
        ("audio", MediaKind.AUDIO),
        ("voice", MediaKind.VOICE),
        ("sticker", MediaKind.STICKER),
        ("video_note", MediaKind.VIDEO_NOTE),
    ],
)
def test_bale_media_fields_are_parsed(field: str, kind: MediaKind) -> None:
    message = parse_update(
        bale_raw(**{field: {"file_id": "f1", "file_size": 42, "file_name": "x.bin"}})
    )

    assert message is not None
    (item,) = message.media
    assert item.kind is kind
    assert item.file_id == "f1"
    assert item.file_size == 42


def test_bale_document_prefers_animation_over_document() -> None:
    message = parse_update(
        bale_raw(
            animation={"file_id": "anim", "file_size": 10},
            document={"file_id": "doc", "file_size": 10},
        )
    )

    assert message is not None
    assert message.media[0].file_id == "anim"


def test_bale_location_and_contact_become_text() -> None:
    location = parse_update(bale_raw(location={"longitude": 51.4, "latitude": 35.7}))
    contact = parse_update(
        bale_raw(contact={"phone_number": "0912", "first_name": "Ali"})
    )

    assert location is not None and "35.70000" in (location.text or "")
    assert contact is not None and "0912" in (contact.text or "")


def test_bale_unsupported_channel_post_is_skipped() -> None:
    raw = bale_raw()
    del raw["message"]["from"]

    assert parse_update(raw) is None
    assert parse_update(bale_raw()) is None  # no text, no media


# ----------------------------------------------------------- bale http helper


def bale_transport(
    *, methods: dict[str, Any] | None = None, file_bytes: bytes = b"bale-file"
) -> httpx.MockTransport:
    """Route Bale API calls and the ``/file`` download endpoint."""
    scripted = dict(methods or {})
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        path = request.url.path
        if request.method == "GET":
            return httpx.Response(200, content=file_bytes)
        method = path.rsplit("/", 1)[-1]
        result = scripted.get(method)
        if isinstance(result, Exception):
            raise result
        if result is None:
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 900}})
        return httpx.Response(200, json=result)

    transport = httpx.MockTransport(handler)
    transport.calls = calls  # type: ignore[attr-defined]
    return transport


def build_bale(transport: httpx.MockTransport) -> BaleAdapter:
    client = httpx.AsyncClient(transport=transport)
    return BaleAdapter(BALE_TOKEN, base_url=BALE_BASE_URL, client=client)


def calls_of(transport: httpx.MockTransport) -> list[httpx.Request]:
    return transport.calls  # type: ignore[attr-defined]


# -------------------------------------------------------------- bale download


async def test_bale_download_resolves_get_file_then_streams(tmp_path: Path) -> None:
    transport = bale_transport(
        methods={
            "getFile": {
                "ok": True,
                "result": {"file_id": "f", "file_path": "files/a.jpg", "file_size": 9},
            }
        }
    )
    adapter = build_bale(transport)
    destination = tmp_path / "spool.jpg"

    written = await adapter.download_media(
        MediaItem(kind=MediaKind.PHOTO, file_id="f"), destination
    )

    assert written == len(b"bale-file")
    assert destination.read_bytes() == b"bale-file"
    urls = [str(request.url) for request in calls_of(transport)]
    assert urls[0].endswith("/getFile")
    assert urls[1] == f"{BALE_BASE_URL}/file/bot{BALE_TOKEN}/files/a.jpg"


async def test_bale_download_refuses_an_oversized_declared_size(tmp_path: Path) -> None:
    transport = bale_transport(
        methods={
            "getFile": {
                "ok": True,
                "result": {"file_id": "f", "file_path": "a", "file_size": 30 * MEGABYTE},
            }
        }
    )
    adapter = build_bale(transport)

    with pytest.raises(MediaTooLargeError):
        await adapter.download_media(
            MediaItem(kind=MediaKind.DOCUMENT, file_id="f"), tmp_path / "spool.bin"
        )
    assert len(calls_of(transport)) == 1, "the body is never fetched"


async def test_bale_download_without_a_file_path_is_an_api_error(tmp_path: Path) -> None:
    transport = bale_transport(methods={"getFile": {"ok": True, "result": {"file_id": "f"}}})
    adapter = build_bale(transport)

    with pytest.raises(ApiError):
        await adapter.download_media(
            MediaItem(kind=MediaKind.PHOTO, file_id="f"), tmp_path / "spool.bin"
        )


async def test_bale_download_error_never_leaks_the_token(tmp_path: Path) -> None:
    transport = bale_transport(methods={"getFile": httpx.ConnectError("nope")})
    adapter = build_bale(transport)

    with pytest.raises(Exception) as excinfo:
        await adapter.download_media(
            MediaItem(kind=MediaKind.PHOTO, file_id="f"), tmp_path / "spool.bin"
        )

    assert BALE_TOKEN not in str(excinfo.value)
    assert excinfo.value.__cause__ is None


# ------------------------------------------------------------------ bale send


@pytest.mark.parametrize(
    ("kind", "method", "field"),
    [
        (MediaKind.PHOTO, "sendPhoto", "photo"),
        (MediaKind.VIDEO, "sendVideo", "video"),
        (MediaKind.ANIMATION, "sendAnimation", "animation"),
        (MediaKind.DOCUMENT, "sendDocument", "document"),
        (MediaKind.AUDIO, "sendAudio", "audio"),
        (MediaKind.VOICE, "sendVoice", "voice"),
    ],
)
async def test_bale_sends_each_documented_kind(
    tmp_path: Path, kind: MediaKind, method: str, field: str
) -> None:
    transport = bale_transport()
    adapter = build_bale(transport)
    item = OutboundMedia(
        kind=kind,
        path=spooled(tmp_path, "clip.mp4"),
        file_name="clip.mp4",
        mime_type="video/mp4",
        file_size=7,
        caption="کپشن",
    )

    sent = await adapter.send_media(7001, item, reply_to_message_id=5)

    request = calls_of(transport)[0]
    assert request.url.path.endswith(f"/{method}")
    body = request.content.decode("utf-8", errors="ignore")
    assert f'name="{field}"' in body
    assert 'filename="clip.mp4"' in body
    assert "کپشن" in body
    assert sent.message_id == 900


@pytest.mark.parametrize("kind", [MediaKind.STICKER, MediaKind.VIDEO_NOTE])
async def test_bale_refuses_kinds_it_does_not_document(
    tmp_path: Path, kind: MediaKind
) -> None:
    adapter = build_bale(bale_transport())

    with pytest.raises(MediaUnsupportedError):
        await adapter.send_media(7001, OutboundMedia(kind=kind, path=spooled(tmp_path)))


async def test_bale_send_media_needs_a_message_id_back(tmp_path: Path) -> None:
    transport = bale_transport(methods={"sendPhoto": {"ok": True, "result": {}}})
    adapter = build_bale(transport)

    with pytest.raises(ApiError):
        await adapter.send_media(
            7001, OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path))
        )


async def test_bale_album_uses_attach_references_and_returns_every_id(
    tmp_path: Path,
) -> None:
    transport = bale_transport(
        methods={
            "sendMediaGroup": {
                "ok": True,
                "result": [{"message_id": 11}, {"message_id": 12}],
            }
        }
    )
    adapter = build_bale(transport)
    items = (
        OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path, "a.jpg"), caption="۱"),
        OutboundMedia(kind=MediaKind.VIDEO, path=spooled(tmp_path, "b.mp4")),
    )

    sent = await adapter.send_media_group(7001, items)

    assert [message.message_id for message in sent] == [11, 12]
    body = calls_of(transport)[0].content.decode("utf-8", errors="ignore")
    media_json = body.split('name="media"')[1]
    payload = json.loads(media_json.split("\r\n\r\n", 1)[1].split("\r\n--", 1)[0])
    assert [entry["type"] for entry in payload] == ["photo", "video"]
    assert payload[0]["media"] == "attach://item0"
    assert payload[0]["caption"] == "۱"
    assert 'name="item1"' in body


async def test_bale_album_rejects_an_unexpected_result(tmp_path: Path) -> None:
    transport = bale_transport(methods={"sendMediaGroup": {"ok": True, "result": "nope"}})
    adapter = build_bale(transport)
    items = (
        OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path, "a.jpg")),
        OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path, "b.jpg")),
    )

    with pytest.raises(ApiError):
        await adapter.send_media_group(7001, items)


async def test_bale_media_errors_are_classified(tmp_path: Path) -> None:
    transport = bale_transport(
        methods={
            "sendPhoto": {
                "ok": False,
                "error_code": 403,
                "description": "bot was blocked by the user",
            }
        }
    )
    adapter = build_bale(transport)

    with pytest.raises(DeliveryError):
        await adapter.send_media(
            7001, OutboundMedia(kind=MediaKind.PHOTO, path=spooled(tmp_path))
        )


def test_bale_adapter_reports_its_platform() -> None:
    adapter = build_bale(bale_transport())

    assert adapter.platform is Platform.BALE
