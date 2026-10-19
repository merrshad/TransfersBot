"""Shared adapter interface used by the Telegram and Bale adapters.

Both adapters expose the same three operations — long-poll for updates, send a
text message, and release resources — plus the text-length guard that keeps us
inside the 4096-character API limit of both messengers.
"""

from __future__ import annotations

import logging
import shutil
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import httpx

from core.errors import ApiError, MediaTooLargeError
from core.models import (
    InlineButton,
    MediaItem,
    OutboundMedia,
    Platform,
    SentMessage,
    UnifiedMessage,
)

#: Read chunk size used while streaming a download to disk.
DOWNLOAD_CHUNK_BYTES = 64 * 1024
#: Timeout for downloading one attachment (connect, read).
DOWNLOAD_TIMEOUT_SECONDS = 60.0

#: Maximum text length accepted by both Telegram and Bale ``sendMessage``.
MAX_MESSAGE_TEXT_LENGTH = 4096
TRUNCATION_MARKER = "…"


def clamp_message_text(
    text: str,
    *,
    limit: int = MAX_MESSAGE_TEXT_LENGTH,
    logger: logging.Logger | None = None,
) -> str:
    """Trim ``text`` to ``limit`` characters, never logging the content itself.

    Only the before/after lengths are logged, so user content stays out of the
    log stream.
    """
    if len(text) <= limit:
        return text
    if logger is not None:
        logger.warning(
            "outgoing text truncated from %d to %d characters", len(text), limit
        )
    keep = max(limit - len(TRUNCATION_MARKER), 1)
    return text[:keep] + TRUNCATION_MARKER


@dataclass(frozen=True, slots=True)
class PollResult:
    """Outcome of one long-polling ``getUpdates`` call.

    Attributes:
        messages: Update payloads we understand and will hand to the bridge.
        next_offset: Offset to persist and send next time, or ``None`` when the
            batch was empty. It is derived from *all* received updates (not just
            the parsed ones), so filtered updates are still acknowledged.
        received: Number of raw updates the platform returned.
    """

    messages: tuple[UnifiedMessage, ...]
    next_offset: int | None
    received: int


async def download_url_to_file(
    url: str,
    destination: Path,
    *,
    max_bytes: int,
    client: httpx.AsyncClient | None = None,
    timeout: float = DOWNLOAD_TIMEOUT_SECONDS,
) -> int:
    """Stream ``url`` into ``destination``, refusing anything over ``max_bytes``.

    The file is written incrementally so a 20 MB attachment never has to sit in
    memory, and an oversized body is aborted as soon as the limit is crossed
    (the partial file is the caller's to delete).

    Raises:
        core.errors.MediaTooLargeError: The body exceeded ``max_bytes``.
        core.errors.MessagingError: A typed transport error.
    """
    owns_client = client is None
    http = client if client is not None else httpx.AsyncClient()
    try:
        try:
            async with http.stream("GET", url, timeout=timeout) as response:
                response.raise_for_status()
                written = 0
                with destination.open("wb") as handle:
                    async for chunk in response.aiter_bytes(DOWNLOAD_CHUNK_BYTES):
                        written += len(chunk)
                        if written > max_bytes:
                            raise MediaTooLargeError(
                                size_bytes=written, limit_bytes=max_bytes, kind="فایل"
                            )
                        handle.write(chunk)
                return written
        except httpx.HTTPError as exc:
            raise ApiError(
                f"media download failed ({type(exc).__name__})"
            ) from None
    finally:
        if owns_client:
            await http.aclose()


def remove_partial(path: Path) -> None:
    """Delete a partially written download, ignoring a missing file."""
    try:
        path.unlink(missing_ok=True)
    except OSError:  # pragma: no cover - best effort cleanup
        pass


def guess_suffix(item: OutboundMedia | MediaItem, *, default: str = ".bin") -> str:
    """Pick a file suffix for an attachment (its name wins, then its mime)."""
    if item.file_name:
        suffix = Path(item.file_name).suffix
        if suffix:
            return suffix
    mime = (item.mime_type or "").split(";", 1)[0].strip().lower()
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "video/mp4": ".mp4",
        "video/webm": ".webm",
        "audio/ogg": ".ogg",
        "audio/mpeg": ".mp3",
        "application/pdf": ".pdf",
        "application/zip": ".zip",
        "application/x-tgsticker": ".tgs",
    }.get(mime, default)


def safe_file_name(item: OutboundMedia | MediaItem, *, fallback: str = "file") -> str:
    """A basename safe to put in a multipart header (no traversal, no blanks)."""
    name = item.file_name or fallback
    name = Path(name).name.strip() or fallback
    return name


def is_within(path: Path, root: Path) -> bool:
    """Whether ``path`` resolves inside ``root`` (spool safety)."""
    try:
        return path.resolve().is_relative_to(root.resolve())
    except OSError:  # pragma: no cover - defensive
        return False


def prune_empty_dirs(root: Path) -> None:
    """Remove now-empty spool subdirectories under ``root``."""
    if not root.is_dir():
        return
    for child in root.iterdir():
        if child.is_dir() and not any(child.iterdir()):
            shutil.rmtree(child, ignore_errors=True)


class BaseAdapter(ABC):
    """Interface implemented by :class:`~adapters.telegram.TelegramAdapter`
    and :class:`~adapters.bale.BaleAdapter`."""

    @property
    @abstractmethod
    def platform(self) -> Platform:
        """Messenger this adapter talks to."""
        ...

    @abstractmethod
    async def poll_once(self, offset: int | None, *, timeout: int) -> PollResult:
        """Fetch pending updates once, using long polling for up to ``timeout``.

        Raises:
            core.errors.MessagingError: ``RateLimitError``, ``TransportError``,
                ``DeliveryError`` or ``ApiError`` instead of leaking SDK errors.
        """
        ...

    @abstractmethod
    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage:
        """Deliver ``text`` to ``chat_id``, optionally with inline buttons.

        Raises:
            core.errors.MessagingError: A typed messaging error when the
                platform refuses the request.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release HTTP resources held by this adapter."""
        ...

    async def download_media(self, item: MediaItem, destination: Path) -> int:
        """Fetch one of this platform's attachments into ``destination``.

        Concrete adapters override this; the default exists so the polling
        contract can be faked without the media contract.
        """
        raise ApiError(
            f"media download is not supported for platform={self.platform.value}"
        )

    async def send_media(
        self,
        chat_id: int,
        item: OutboundMedia,
        *,
        reply_to_message_id: int | None = None,
    ) -> SentMessage:
        """Send one attachment; overridden by concrete adapters."""
        raise ApiError(f"media sending is not supported for platform={self.platform.value}")

    async def send_media_group(
        self,
        chat_id: int,
        items: Sequence[OutboundMedia],
        *,
        reply_to_message_id: int | None = None,
    ) -> tuple[SentMessage, ...]:
        """Send an album; overridden by concrete adapters."""
        raise ApiError(f"albums are not supported for platform={self.platform.value}")

    async def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> SentMessage | None:
        """Edit an existing text message; overridden by concrete adapters."""
        raise ApiError(f"message editing is not supported for platform={self.platform.value}")

    async def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> None:
        """Acknowledge an inline-button press; overridden by concrete adapters."""
        return None


__all__ = [
    "DOWNLOAD_CHUNK_BYTES",
    "DOWNLOAD_TIMEOUT_SECONDS",
    "MAX_MESSAGE_TEXT_LENGTH",
    "TRUNCATION_MARKER",
    "BaseAdapter",
    "PollResult",
    "clamp_message_text",
    "download_url_to_file",
    "guess_suffix",
    "is_within",
    "prune_empty_dirs",
    "remove_partial",
    "safe_file_name",
]
