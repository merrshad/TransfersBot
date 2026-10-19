"""Media relaying: platform limits, delivery planning, temp files and albums.

Files cannot be forwarded by handle: a Telegram ``file_id`` means nothing to
Bale (and vice versa), so every attachment is resolved on the source platform,
spooled to a temporary file, and uploaded again on the target. This module owns
the parts of that pipeline that are pure policy — the numbers, the capability
table, the plan, the spool and the album collector — so the adapters only do
I/O and the delivery worker only orchestrates.

**Documented limits** (see ``README.md`` for the full citation list):

* Telegram — ``getFile`` downloads at most **20 MB**; multipart uploads are
  capped at **10 MB** for ``sendPhoto`` and **50 MB** for the other media
  methods; ``sendMediaGroup`` holds **2–10** items; ``sendSticker`` only accepts
  static ``.WEBP`` (≤ 512 KB), animated ``.TGS`` (≤ 64 KB) and video ``.WEBM``
  (≤ 256 KB).
* Bale — ``getFile`` documents the same **20 MB** download limit and serves
  ``/file/bot<TOKEN>/<file_path>`` links valid for one hour; multipart uploads
  are capped at **10 MB** for photos and **50 MB** otherwise; ``sendMediaGroup``
  is documented. ``sendSticker`` and ``sendVideoNote`` are **not** documented,
  so stickers are converted to a photo (Pillow, when installed) or replaced by a
  text placeholder, and video notes travel as videos.

Sizes are enforced twice: the plan rejects what the target cannot represent, and
the worker checks the *actual* downloaded size against the target's upload cap,
notifying the sender instead of sending when it does not fit.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import shutil
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from core.errors import MediaTooLargeError, MediaUnsupportedError
from core.models import MediaItem, MediaKind, Platform, UnifiedMessage

MEGABYTE = 1024 * 1024

#: ``outbox.payload["kind"]`` discriminator written by the bridge for media.
RELAY_MEDIA_KIND = "relay_media"

#: Placeholder relayed when a message type cannot be represented at all.
UNSUPPORTED_TEMPLATE = "[unsupported message type: {kind}]"

#: Placeholder relayed for a sticker we could neither forward nor convert.
STICKER_PLACEHOLDER = "[sticker{emoji}]"

#: Telegram sticker constraints (https://core.telegram.org/stickers).
STICKER_MAX_BYTES = 512 * 1024
ANIMATED_STICKER_MAX_BYTES = 64 * 1024
VIDEO_STICKER_MAX_BYTES = 256 * 1024

#: Sticker mime types Telegram accepts, with their own size ceiling.
STICKER_MIME_LIMITS: Mapping[str, int] = {
    "image/webp": STICKER_MAX_BYTES,
    "application/x-tgsticker": ANIMATED_STICKER_MAX_BYTES,
    "image/tgs": ANIMATED_STICKER_MAX_BYTES,
    "video/webm": VIDEO_STICKER_MAX_BYTES,
}

#: Mime types a raster converter can turn into a PNG photo.
RASTERIZABLE_MIME_TYPES = frozenset({"image/webp", "image/png", "image/jpeg", "image/jpg"})

#: Major mime type each kind must declare (documents/stickers accept anything).
_EXPECTED_MAJOR_TYPES: Mapping[MediaKind, frozenset[str]] = {
    MediaKind.PHOTO: frozenset({"image"}),
    MediaKind.VIDEO: frozenset({"video"}),
    MediaKind.ANIMATION: frozenset({"video"}),
    MediaKind.VIDEO_NOTE: frozenset({"video"}),
    MediaKind.AUDIO: frozenset({"audio"}),
    MediaKind.VOICE: frozenset({"audio"}),
}

#: Persian labels used in notices and placeholders (never file names or content).
KIND_LABELS: Mapping[MediaKind, str] = {
    MediaKind.PHOTO: "عکس",
    MediaKind.VIDEO: "ویدیو",
    MediaKind.ANIMATION: "انیمیشن",
    MediaKind.DOCUMENT: "فایل",
    MediaKind.AUDIO: "صوت",
    MediaKind.VOICE: "پیام صوتی",
    MediaKind.STICKER: "استیکر",
    MediaKind.VIDEO_NOTE: "ویدیو نوت",
}


def kind_label(kind: MediaKind) -> str:
    """Persian label for a media kind."""
    return KIND_LABELS.get(kind, kind.value)


# ------------------------------------------------------------------- limits


@dataclass(frozen=True, slots=True)
class PlatformMediaLimits:
    """Documented byte limits and capabilities of one platform.

    Capability flags (stickers, video notes, albums) are API facts, not
    operator tunables, so they live here rather than in the settings.
    """

    max_download_bytes: int
    max_upload_bytes: int
    max_photo_upload_bytes: int
    supports_stickers: bool = False
    supports_video_notes: bool = False
    supports_albums: bool = False
    album_min_items: int = 2
    album_max_items: int = 10

    def upload_limit_for(self, kind: MediaKind) -> int:
        """Upload ceiling for ``kind`` on this platform."""
        if kind is MediaKind.PHOTO:
            return self.max_photo_upload_bytes
        return self.max_upload_bytes

    def sticker_limit_for(self, mime_type: str | None) -> int:
        """Sticker ceiling for a mime type (falls back to the static cap)."""
        if mime_type is None:
            return STICKER_MAX_BYTES
        return STICKER_MIME_LIMITS.get(mime_type.lower(), STICKER_MAX_BYTES)


#: Defaults straight from the official documentation.
TELEGRAM_MEDIA_LIMITS = PlatformMediaLimits(
    max_download_bytes=20 * MEGABYTE,
    max_upload_bytes=50 * MEGABYTE,
    max_photo_upload_bytes=10 * MEGABYTE,
    supports_stickers=True,
    supports_video_notes=True,
    supports_albums=True,
)

BALE_MEDIA_LIMITS = PlatformMediaLimits(
    max_download_bytes=20 * MEGABYTE,
    max_upload_bytes=50 * MEGABYTE,
    max_photo_upload_bytes=10 * MEGABYTE,
    supports_stickers=False,
    supports_video_notes=False,
    supports_albums=True,
)

DEFAULT_MEDIA_LIMITS: Mapping[Platform, PlatformMediaLimits] = {
    Platform.TELEGRAM: TELEGRAM_MEDIA_LIMITS,
    Platform.BALE: BALE_MEDIA_LIMITS,
}


@dataclass(frozen=True, slots=True)
class MediaPolicy:
    """Everything the relay needs to know to move media to a platform."""

    limits: Mapping[Platform, PlatformMediaLimits] = field(
        default_factory=lambda: dict(DEFAULT_MEDIA_LIMITS)
    )
    can_convert_stickers: bool = False

    def for_platform(self, platform: Platform) -> PlatformMediaLimits:
        """Limits of ``platform`` (falling back to the Telegram shape)."""
        return self.limits.get(platform, TELEGRAM_MEDIA_LIMITS)


def policy_from_settings(settings: object) -> MediaPolicy:
    """Build a :class:`MediaPolicy` from the ``Settings`` object.

    Only the byte ceilings are configurable; the capability flags stay tied to
    what each platform actually documents.
    """
    telegram = PlatformMediaLimits(
        max_download_bytes=int(getattr(settings, "telegram_max_download_mb")) * MEGABYTE,
        max_upload_bytes=int(getattr(settings, "telegram_max_upload_mb")) * MEGABYTE,
        max_photo_upload_bytes=(
            int(getattr(settings, "telegram_max_photo_upload_mb")) * MEGABYTE
        ),
        supports_stickers=TELEGRAM_MEDIA_LIMITS.supports_stickers,
        supports_video_notes=TELEGRAM_MEDIA_LIMITS.supports_video_notes,
        supports_albums=TELEGRAM_MEDIA_LIMITS.supports_albums,
    )
    bale = PlatformMediaLimits(
        max_download_bytes=int(getattr(settings, "bale_max_download_mb")) * MEGABYTE,
        max_upload_bytes=int(getattr(settings, "bale_max_upload_mb")) * MEGABYTE,
        max_photo_upload_bytes=int(getattr(settings, "bale_max_photo_upload_mb")) * MEGABYTE,
        supports_stickers=BALE_MEDIA_LIMITS.supports_stickers,
        supports_video_notes=BALE_MEDIA_LIMITS.supports_video_notes,
        supports_albums=BALE_MEDIA_LIMITS.supports_albums,
    )
    return MediaPolicy(
        limits={Platform.TELEGRAM: telegram, Platform.BALE: bale},
        can_convert_stickers=can_convert_stickers(),
    )


# ------------------------------------------------------------------- plans

PlanAction = Literal["relay", "convert", "placeholder"]


@dataclass(frozen=True, slots=True)
class MediaDeliveryPlan:
    """How one attachment travels to the target platform.

    Attributes:
        action: ``relay`` (send as-is), ``convert`` (rasterize first) or
            ``placeholder`` (send :attr:`text` instead of the file).
        kind: The ``MediaKind`` to send at the target (``None`` for a
            placeholder).
        text: Placeholder body when ``action == "placeholder"``.
        reason: Machine-readable explanation, for logs only.
    """

    action: PlanAction
    kind: MediaKind | None = None
    text: str | None = None
    reason: str = "ok"

    @property
    def is_placeholder(self) -> bool:
        """Whether the plan delivers text instead of a file."""
        return self.action == "placeholder"


def mime_matches_kind(kind: MediaKind, mime_type: str | None) -> bool:
    """Whether ``mime_type`` can legitimately be ``kind``.

    An unknown or absent mime type is accepted (the platform's own validation
    still applies); a contradicting one — a ``text/plain`` "photo", say — is
    rejected so the relay degrades to a placeholder instead of an API error.
    """
    if not mime_type:
        return True
    expected = _EXPECTED_MAJOR_TYPES.get(kind)
    if expected is None:
        return True
    major = mime_type.split("/", 1)[0].strip().lower()
    return major in expected


def _sticker_plan(
    item: MediaItem,
    limits: PlatformMediaLimits,
    policy: MediaPolicy,
) -> MediaDeliveryPlan:
    """Decide how a sticker travels to a platform that may not accept one."""
    mime = (item.mime_type or "").lower() or None
    size = item.file_size
    if limits.supports_stickers and (mime is None or mime in STICKER_MIME_LIMITS):
        if size is None or size <= limits.sticker_limit_for(mime):
            return MediaDeliveryPlan(action="relay", kind=MediaKind.STICKER)
    if policy.can_convert_stickers and (mime is None or mime in RASTERIZABLE_MIME_TYPES):
        return MediaDeliveryPlan(
            action="convert",
            kind=MediaKind.PHOTO,
            reason="sticker_as_photo",
        )
    return MediaDeliveryPlan(
        action="placeholder",
        kind=None,
        text=STICKER_PLACEHOLDER.format(emoji=f" {item.emoji}" if item.emoji else ""),
        reason="sticker_unsupported",
    )


def plan_media_delivery(
    item: MediaItem,
    target_platform: Platform,
    policy: MediaPolicy,
) -> MediaDeliveryPlan:
    """Capability plan for one attachment bound to ``target_platform``.

    Size enforcement is deliberately *not* done here: the worker checks the
    real downloaded size against :meth:`PlatformMediaLimits.upload_limit_for`,
    because a platform handle usually arrives without a trustworthy size.
    """
    limits = policy.for_platform(target_platform)
    if not mime_matches_kind(item.kind, item.mime_type):
        return MediaDeliveryPlan(
            action="placeholder",
            kind=None,
            text=(
                f"[unsupported media type: {kind_label(item.kind)} "
                f"({item.mime_type})]"
            ),
            reason="mime_mismatch",
        )
    if item.kind is MediaKind.STICKER:
        return _sticker_plan(item, limits, policy)
    if item.kind is MediaKind.VIDEO_NOTE and not limits.supports_video_notes:
        return MediaDeliveryPlan(
            action="relay", kind=MediaKind.VIDEO, reason="video_note_as_video"
        )
    return MediaDeliveryPlan(action="relay", kind=item.kind)


def plan_album(
    plans: Sequence[MediaDeliveryPlan],
) -> MediaKind | None:
    """The album kind for a homogeneous group, or ``None`` when it cannot group.

    Only groups that a platform accepts as one album qualify: mixed photos and
    videos, documents only with documents, audio only with audio.
    """
    kinds = {plan.kind for plan in plans}
    if not kinds or None in kinds:
        return None
    if kinds <= {MediaKind.PHOTO, MediaKind.VIDEO} and MediaKind.DOCUMENT not in kinds:
        return MediaKind.PHOTO if kinds == {MediaKind.PHOTO} else MediaKind.VIDEO
    if kinds == {MediaKind.DOCUMENT}:
        return MediaKind.DOCUMENT
    if kinds == {MediaKind.AUDIO}:
        return MediaKind.AUDIO
    return None


def can_send_album(
    plans: Sequence[MediaDeliveryPlan],
    target_platform: Platform,
    policy: MediaPolicy,
) -> bool:
    """Whether ``plans`` may travel as one album to ``target_platform``."""
    limits = policy.for_platform(target_platform)
    if not limits.supports_albums:
        return False
    if not limits.album_min_items <= len(plans) <= limits.album_max_items:
        return False
    return plan_album(plans) is not None


def check_upload_size(
    *,
    size_bytes: int,
    kind: MediaKind,
    target_platform: Platform,
    policy: MediaPolicy,
) -> None:
    """Raise :class:`MediaTooLargeError` when ``size_bytes`` exceeds the cap."""
    limit = policy.for_platform(target_platform).upload_limit_for(kind)
    if size_bytes > limit:
        raise MediaTooLargeError(
            size_bytes=size_bytes, limit_bytes=limit, kind=kind_label(kind)
        )


def check_download_size(
    *,
    size_bytes: int | None,
    source_platform: Platform,
    policy: MediaPolicy,
) -> None:
    """Reject a source handle that is bigger than the platform's ``getFile`` cap."""
    if size_bytes is None:
        return
    limit = policy.for_platform(source_platform).max_download_bytes
    if size_bytes > limit:
        raise MediaTooLargeError(
            size_bytes=size_bytes, limit_bytes=limit, kind="فایل"
        )


# --------------------------------------------------------------- placeholders


def unsupported_placeholder(kind: str) -> str:
    """Placeholder text for a message type the bridge cannot represent."""
    return UNSUPPORTED_TEMPLATE.format(kind=kind)


def describe_location(*, latitude: float, longitude: float) -> str:
    """Text rendering of a location pin (numbers only)."""
    return (
        "📍 موقعیت مکانی\n"
        f"https://maps.google.com/?q={latitude:.5f},{longitude:.5f}"
    )


def describe_contact(
    *, name: str, phone_number: str, username: str | None = None
) -> str:
    """Text rendering of a shared contact."""
    lines = ["👤 مخاطب", name.strip() or "بدون نام", phone_number.strip()]
    if username:
        lines.append(f"@{username.lstrip('@')}")
    return "\n".join(line for line in lines if line)


def describe_poll(
    *, question: str, options: Sequence[str], poll_type: str | None = None
) -> str:
    """Text rendering of a poll (question and options only)."""
    lines = [f"📊 نظرسنجی: {question.strip()}", *(f"• {option}" for option in options)]
    if poll_type:
        lines.append(f"(نوع: {poll_type})")
    return "\n".join(lines)


# ------------------------------------------------------------- serialization


def media_item_payload(item: MediaItem) -> dict[str, Any]:
    """Serialize one attachment for ``outbox.payload``.

    Only ids and metadata are written; an in-memory payload (produced by test
    doubles, never by the adapters) is base64-encoded so the payload stays
    JSON-serializable.
    """
    payload: dict[str, Any] = {
        "kind": item.kind.value,
        "file_id": item.file_id,
        "url": item.url,
        "file_name": item.file_name,
        "mime_type": item.mime_type,
        "file_size": item.file_size,
        "duration": item.duration,
        "width": item.width,
        "height": item.height,
        "emoji": item.emoji,
        "caption": item.caption,
    }
    if item.data is not None:
        payload["data"] = base64.b64encode(item.data).decode("ascii")
    return payload


def media_item_from_payload(raw: object) -> MediaItem:
    """Rebuild a :class:`MediaItem` from a stored payload item.

    Raises:
        ValueError: The payload is malformed (unknown kind, no source, bad
            base64) — the caller marks the entry terminally failed.
    """
    if not isinstance(raw, Mapping):
        raise ValueError("media payload item is not an object")
    kind_value = raw.get("kind")
    if not isinstance(kind_value, str):
        raise ValueError("media payload item has no kind")
    try:
        kind = MediaKind(kind_value)
    except ValueError as exc:
        raise ValueError(f"unknown media kind={kind_value}") from exc

    data: bytes | None = None
    raw_data = raw.get("data")
    if isinstance(raw_data, str) and raw_data:
        try:
            data = base64.b64decode(raw_data, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("media payload item has invalid base64 data") from exc

    file_name = raw.get("file_name")
    mime_type = raw.get("mime_type")
    caption = raw.get("caption")
    emoji = raw.get("emoji")
    return MediaItem(
        kind=kind,
        data=data,
        url=_optional_str(raw.get("url")),
        file_id=_optional_str(raw.get("file_id")),
        file_name=_optional_str(file_name),
        mime_type=_optional_str(mime_type),
        file_size=_optional_int(raw.get("file_size")),
        duration=_optional_float(raw.get("duration")),
        width=_optional_int(raw.get("width")),
        height=_optional_int(raw.get("height")),
        emoji=_optional_str(emoji),
        caption=_optional_str(caption),
    )


def _optional_str(value: object) -> str | None:
    """``value`` when it is a non-blank string, else ``None``."""
    return value if isinstance(value, str) and value.strip() else None


def _optional_int(value: object) -> int | None:
    """``value`` when it is an int (bools excluded), else ``None``."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_float(value: object) -> float | None:
    """``value`` as a float when numeric, else ``None``."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


# -------------------------------------------------------------- conversions


StickerConverter = Callable[[Path, Path], None]


def can_convert_stickers() -> bool:
    """Whether Pillow is importable, i.e. stickers can be rasterized."""
    try:  # pragma: no cover - exercised by the Pillow-installed environment
        import PIL  # noqa: F401
    except Exception:  # noqa: BLE001 - any import failure means "no converter"
        return False
    return True


def convert_sticker_to_png(source: Path, destination: Path) -> None:
    """Rasterize a webp/other sticker into a PNG photo.

    Pillow is an optional dependency (``pip install .[media]``); without it this
    raises :class:`MediaUnsupportedError` so the caller can fall back to a text
    placeholder instead of failing the relay. A file the converter cannot open
    (a truncated or mislabelled sticker) is reported the same way — a bad
    attachment must never crash the delivery loop.
    """
    converter = sticker_converter()
    if converter is None:
        raise MediaUnsupportedError(
            "sticker conversion requires Pillow", kind="استیکر"
        )
    try:
        converter(source, destination)
    except Exception as exc:  # noqa: BLE001 - any converter failure degrades to text
        raise MediaUnsupportedError(
            f"استیکر قابل تبدیل نبود ({type(exc).__name__})", kind="استیکر"
        ) from None


def sticker_converter() -> StickerConverter | None:
    """A Pillow-based converter, or ``None`` when Pillow is not installed."""
    try:
        from PIL import Image
    except Exception:  # noqa: BLE001 - optional dependency
        return None

    def convert(source: Path, destination: Path) -> None:
        with Image.open(source) as image:
            converted = image.convert("RGBA")
            try:
                converted.save(destination, format="PNG")
            finally:
                converted.close()

    return convert


# -------------------------------------------------------------- temp spool


class TempMediaStore:
    """Per-user temporary files with a hard quota and guaranteed cleanup.

    Layout: ``<root>/u<user_id>/<entry_id>-<index><suffix>`` — the name is
    derived from the outbox entry, so a retry can reuse the file it already
    downloaded and a crash leaves only files the startup sweep can age out.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        quota_bytes: int,
        logger: logging.Logger | None = None,
    ) -> None:
        if quota_bytes < 1:
            raise ValueError("quota_bytes must be >= 1.")
        self._root = Path(root)
        self._quota_bytes = quota_bytes
        self._log = logger or logging.getLogger("bridge.media")

    @property
    def root(self) -> Path:
        """Directory every spooled file lives under."""
        return self._root

    @property
    def quota_bytes(self) -> int:
        """Per-user ceiling for spooled bytes."""
        return self._quota_bytes

    def user_dir(self, user_id: int) -> Path:
        """Directory holding ``user_id``'s spooled files."""
        return self._root / f"u{user_id}"

    def path_for(
        self, *, user_id: int, entry_id: int, index: int, suffix: str = ""
    ) -> Path:
        """Deterministic spool path for one item of one outbox entry."""
        return self.user_dir(user_id) / f"{entry_id}-{index}{suffix}"

    def prepare(self, path: Path) -> Path:
        """Create the parent directory and return ``path``."""
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def usage(self, user_id: int) -> int:
        """Bytes currently spooled for ``user_id``."""
        directory = self.user_dir(user_id)
        if not directory.is_dir():
            return 0
        return sum(
            entry.stat().st_size for entry in directory.iterdir() if entry.is_file()
        )

    def has_room_for(self, user_id: int, size_bytes: int) -> bool:
        """Whether ``size_bytes`` more may be spooled for ``user_id``."""
        return self.usage(user_id) + max(size_bytes, 0) <= self._quota_bytes

    def discard(self, path: Path) -> bool:
        """Delete one spooled file (and its now-empty directory)."""
        try:
            if not path.exists():
                return False
            path.unlink()
        except OSError:
            self._log.warning("could not remove a spooled media file")
            return False
        parent = path.parent
        try:
            if parent.is_dir() and not any(parent.iterdir()):
                parent.rmdir()
        except OSError:  # pragma: no cover - best effort
            pass
        return True

    def discard_all(self, user_id: int) -> int:
        """Delete every spooled file of ``user_id``; returns the file count."""
        directory = self.user_dir(user_id)
        if not directory.is_dir():
            return 0
        files = [entry for entry in directory.iterdir() if entry.is_file()]
        for entry in files:
            self.discard(entry)
        shutil.rmtree(directory, ignore_errors=True)
        return len(files)

    def sweep(self, *, max_age_seconds: float, now: float | None = None) -> int:
        """Delete files older than ``max_age_seconds`` (startup crash cleanup)."""
        import time

        reference = time.time() if now is None else now
        removed = 0
        if not self._root.is_dir():
            return 0
        for directory in self._root.iterdir():
            if not directory.is_dir():
                continue
            for entry in directory.iterdir():
                if not entry.is_file():
                    continue
                try:
                    age = reference - entry.stat().st_mtime
                except OSError:  # pragma: no cover - race with the OS
                    continue
                if age >= max_age_seconds and self.discard(entry):
                    removed += 1
        return removed

    def files(self) -> tuple[Path, ...]:
        """Every spooled file (used by the acceptance test and the sweep)."""
        if not self._root.is_dir():
            return ()
        return tuple(
            entry
            for directory in sorted(self._root.iterdir())
            if directory.is_dir()
            for entry in sorted(directory.iterdir())
            if entry.is_file()
        )

    def total_bytes(self) -> int:
        """Bytes currently spooled across all users."""
        return sum(path.stat().st_size for path in self.files())


# ----------------------------------------------------------------- albums


class MediaGroupCollector:
    """Collects the members of one album before relaying them together.

    Telegram and Bale deliver every album item as its own update sharing a
    ``media_group_id``. A short window (``MEDIA_GROUP_WINDOW_SECONDS``) lets the
    bridge relay the group as one album — or, when the target has no albums, in
    the original order. Messages without a group are flushed immediately, so
    the window never delays ordinary traffic.
    """

    def __init__(
        self,
        *,
        window: float,
        flush: Callable[[tuple[UnifiedMessage, ...]], Awaitable[None]],
        sleep: Callable[[float], Awaitable[None]] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if window < 0:
            raise ValueError("window must be >= 0.")
        self._window = window
        self._flush = flush
        self._sleep = sleep or asyncio.sleep
        self._log = logger or logging.getLogger("bridge.media.groups")
        self._pending: dict[tuple[str, int, str], list[UnifiedMessage]] = {}
        self._tasks: dict[tuple[str, int, str], asyncio.Task[None]] = {}

    @property
    def pending_groups(self) -> int:
        """Number of albums still waiting for their window to close."""
        return len(self._pending)

    @staticmethod
    def key(message: UnifiedMessage) -> tuple[str, int, str] | None:
        """Group key of ``message``, or ``None`` when it is not an album item."""
        if not message.media_group_id:
            return None
        return (message.source.value, message.chat_id, message.media_group_id)

    async def add(self, message: UnifiedMessage) -> None:
        """Buffer an album item, or flush a standalone message right away."""
        key = self.key(message)
        if key is None:
            await self._flush((message,))
            return
        self._pending.setdefault(key, []).append(message)
        if key not in self._tasks:
            self._tasks[key] = asyncio.create_task(self._delayed_flush(key))

    async def _delayed_flush(self, key: tuple[str, int, str]) -> None:
        """Wait out the window, then hand the whole group to the bridge."""
        try:
            if self._window:
                await self._sleep(self._window)
        finally:
            self._tasks.pop(key, None)
        batch = self._pending.pop(key, [])
        if not batch:
            return
        self._log.info(
            "album flushed platform=%s chat_id=%s items=%s",
            key[0],
            key[1],
            len(batch),
        )
        await self._flush(tuple(batch))

    async def drain(self) -> None:
        """Flush every buffered group now (shutdown and tests)."""
        for key in list(self._pending):
            task = self._tasks.pop(key, None)
            if task is not None:
                task.cancel()
            batch = self._pending.pop(key, [])
            if batch:
                await self._flush(tuple(batch))


__all__ = [
    "ANIMATED_STICKER_MAX_BYTES",
    "BALE_MEDIA_LIMITS",
    "DEFAULT_MEDIA_LIMITS",
    "KIND_LABELS",
    "MEGABYTE",
    "MediaDeliveryPlan",
    "MediaGroupCollector",
    "MediaPolicy",
    "PlatformMediaLimits",
    "RASTERIZABLE_MIME_TYPES",
    "RELAY_MEDIA_KIND",
    "STICKER_MIME_LIMITS",
    "STICKER_PLACEHOLDER",
    "TELEGRAM_MEDIA_LIMITS",
    "TempMediaStore",
    "UNSUPPORTED_TEMPLATE",
    "can_convert_stickers",
    "can_send_album",
    "check_download_size",
    "check_upload_size",
    "convert_sticker_to_png",
    "media_item_from_payload",
    "media_item_payload",
    "mime_matches_kind",
    "describe_contact",
    "describe_location",
    "describe_poll",
    "kind_label",
    "plan_album",
    "plan_media_delivery",
    "policy_from_settings",
    "sticker_converter",
    "unsupported_placeholder",
]
