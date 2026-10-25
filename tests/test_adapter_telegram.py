"""Tests for :mod:`adapters.telegram` — all offline, no real bot or token."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import pytest
from telegram import (
    Bot,
    InlineKeyboardMarkup,
    Message,
    ReplyParameters,
    Update,
)
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut

from adapters import telegram as telegram_module
from adapters.base import MAX_MESSAGE_TEXT_LENGTH
from adapters.telegram import (
    TelegramAdapter,
    retry_after_seconds,
    to_unified_message,
    translate_telegram_error,
)
from core.errors import ApiError, DeliveryError, RateLimitError, TransportError
from core.models import InlineButton, Platform

# A syntactically valid dummy token; never used against the network.
DUMMY_TOKEN = "123456:TESTTOKEN"
BOT = Bot(DUMMY_TOKEN)


def message_update(
    *,
    update_id: int,
    text: str | None = "/start",
    chat_type: str = "private",
    with_sender: bool = True,
    chat_id: int = 5001,
    message_id: int = 11,
    reply: dict[str, Any] | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> Update:
    """Build a real ``telegram.Update`` from a raw payload."""
    message: dict[str, Any] = {
        "message_id": message_id,
        "chat": {"id": chat_id, "type": chat_type},
        "date": 1767225600,
    }
    if text is not None:
        message["text"] = text
    if with_sender:
        message["from"] = {"id": chat_id, "is_bot": False, "first_name": "Ali"}
    if reply is not None:
        message["reply_to_message"] = reply
    if entities is not None:
        message["entities"] = entities
    return Update.de_json({"update_id": update_id, "message": message}, BOT)


def callback_update(
    *,
    update_id: int = 20,
    data: str = "menu:main",
    chat_id: int = 5001,
    message_id: int = 11,
    with_message: bool = True,
) -> Update:
    """Build a real Telegram callback-query update."""
    callback: dict[str, Any] = {
        "id": "callback-1",
        "chat_instance": "chat-instance-1",
        "from": {"id": chat_id, "is_bot": False, "first_name": "Ali"},
        "data": data,
    }
    if with_message:
        callback["message"] = {
            "message_id": message_id,
            "chat": {"id": chat_id, "type": "private"},
            "date": 1767225600,
        }
    return Update.de_json({"update_id": update_id, "callback_query": callback}, BOT)


class FakeTelegramBot:
    """Minimal :class:`TelegramBotPort` implementation that records calls."""

    def __init__(
        self,
        *,
        updates: Sequence[Update] = (),
        poll_error: Exception | None = None,
        send_error: Exception | None = None,
    ) -> None:
        self.updates = tuple(updates)
        self.poll_error = poll_error
        self.send_error = send_error
        self.poll_calls: list[dict[str, Any]] = []
        self.send_calls: list[dict[str, Any]] = []
        self.edit_calls: list[dict[str, Any]] = []
        self.answer_calls: list[tuple[str, str | None]] = []
        self.shutdown_called = False

    async def get_updates(
        self,
        offset: int | None = None,
        limit: int | None = None,
        timeout: int | None = None,
        allowed_updates: Sequence[str] | None = None,
    ) -> tuple[Update, ...]:
        self.poll_calls.append(
            {
                "offset": offset,
                "limit": limit,
                "timeout": timeout,
                "allowed_updates": list(allowed_updates or ()),
            }
        )
        if self.poll_error is not None:
            raise self.poll_error
        return self.updates

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_parameters: ReplyParameters | None = None,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> Message:
        call: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "reply_parameters": reply_parameters,
        }
        if reply_markup is not None:
            call["reply_markup"] = reply_markup
        self.send_calls.append(call)
        if self.send_error is not None:
            raise self.send_error
        return Message.de_json(
            {"message_id": 900, "date": 1767225600, "chat": {"id": chat_id, "type": "private"}, "text": text},
            BOT,
        )

    async def edit_message_text(
        self,
        text: str,
        chat_id: int,
        message_id: int,
        *,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> Message:
        self.edit_calls.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "reply_markup": reply_markup,
            }
        )
        return Message.de_json(
            {"message_id": message_id, "date": 1767225600, "chat": {"id": chat_id, "type": "private"}, "text": text},
            BOT,
        )

    async def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> bool:
        self.answer_calls.append((callback_query_id, text))
        return True

    async def shutdown(self) -> None:
        self.shutdown_called = True


def build_adapter(**kwargs: Any) -> tuple[TelegramAdapter, FakeTelegramBot]:
    """Adapter wired to a fake bot."""
    bot = kwargs.pop("bot", None) or FakeTelegramBot(**kwargs)
    return TelegramAdapter(DUMMY_TOKEN, bot=bot), bot


# ------------------------------------------------------------- conversions


def test_private_text_message_is_converted() -> None:
    update = message_update(
        update_id=1,
        text="سلام",
        entities=[{"type": "bold", "offset": 0, "length": 3}],
    )

    message = to_unified_message(update)

    assert message is not None
    assert message.source is Platform.TELEGRAM
    assert message.sender_id == 5001
    assert message.chat_id == 5001
    assert message.message_id == 11
    assert message.text == "سلام"
    assert message.entities[0]["type"] == "bold"
    assert message.media == ()
    assert message.timestamp.tzinfo is not None
    assert message.key == ("telegram", 5001, 11)


def test_reply_is_captured_as_message_reference() -> None:
    update = message_update(
        update_id=2,
        reply={"message_id": 5, "date": 1767225600, "chat": {"id": 5001, "type": "private"}},
    )

    message = to_unified_message(update)

    assert message is not None
    assert message.reply_to is not None
    assert message.reply_to.message_id == 5
    assert message.reply_to.chat_id == 5001


@pytest.mark.parametrize(
    "kwargs",
    [
        {"text": None},  # media-only message
        {"chat_type": "channel"},
        {"with_sender": False},  # channel post
    ],
)
def test_unhandled_updates_are_skipped(kwargs: dict[str, Any]) -> None:
    assert to_unified_message(message_update(update_id=3, **kwargs)) is None


def test_group_message_is_converted_with_sender_label() -> None:
    message = to_unified_message(
        message_update(
            update_id=4,
            text="سلام گروه",
            chat_type="supergroup",
            chat_id=-100123,
            message_id=77,
        )
    )

    assert message is not None
    assert message.chat_type == "supergroup"
    assert message.is_group is True
    assert message.chat_id == -100123
    assert message.sender_name == "Ali"
    assert message.is_bot_sender is False


def test_callback_without_data_is_skipped() -> None:
    update = Update.de_json(
        {
            "update_id": 4,
            "callback_query": {
                "id": "1",
                "from": {"id": 1, "is_bot": False, "first_name": "A"},
                "chat_instance": "x",
            },
        },
        BOT,
    )

    assert to_unified_message(update) is None


# ------------------------------------------------------------------ polling


async def test_poll_once_passes_offset_and_computes_next_offset() -> None:
    adapter, bot = build_adapter(
        updates=(message_update(update_id=10), message_update(update_id=11, message_id=12))
    )

    result = await adapter.poll_once(9, timeout=15)

    assert [message.message_id for message in result.messages] == [11, 12]
    assert result.next_offset == 12
    assert result.received == 2
    assert bot.poll_calls == [
        {
            "offset": 9,
            "limit": 100,
            "timeout": 15,
            "allowed_updates": ["message", "callback_query"],
        }
    ]


async def test_poll_once_without_updates_keeps_offset_none() -> None:
    adapter, _ = build_adapter(updates=())

    result = await adapter.poll_once(None, timeout=5)

    assert result.messages == ()
    assert result.next_offset is None
    assert result.received == 0


async def test_filtered_updates_still_advance_the_offset() -> None:
    adapter, _ = build_adapter(
        updates=(
            message_update(update_id=5),
            message_update(update_id=6, text=None),  # photo only
            message_update(update_id=7, chat_type="channel"),
        )
    )

    result = await adapter.poll_once(None, timeout=1)

    assert len(result.messages) == 1
    assert result.next_offset == 8
    assert result.received == 3


async def test_poll_error_is_mapped_to_typed_error() -> None:
    adapter, _ = build_adapter(poll_error=Forbidden("bot was blocked by the user"))

    with pytest.raises(DeliveryError) as excinfo:
        await adapter.poll_once(None, timeout=1)

    assert excinfo.value.permanent is True


async def test_platform_property() -> None:
    adapter, _ = build_adapter()
    assert adapter.platform is Platform.TELEGRAM


# -------------------------------------------------------------------- sending


async def test_send_message_returns_sent_message() -> None:
    adapter, bot = build_adapter()

    sent = await adapter.send_message(5001, "سلام")

    assert sent.platform is Platform.TELEGRAM
    assert sent.chat_id == 5001
    assert sent.message_id == 900
    assert bot.send_calls == [
        {"chat_id": 5001, "text": "سلام", "reply_parameters": None}
    ]


async def test_send_message_with_reply_uses_reply_parameters() -> None:
    adapter, bot = build_adapter()

    await adapter.send_message(5001, "پاسخ", reply_to_message_id=42)

    assert bot.send_calls[0]["reply_parameters"] == ReplyParameters(message_id=42)


async def test_send_message_truncates_over_long_text() -> None:
    adapter, bot = build_adapter()

    await adapter.send_message(5001, "x" * (MAX_MESSAGE_TEXT_LENGTH + 100))

    assert len(bot.send_calls[0]["text"]) == MAX_MESSAGE_TEXT_LENGTH


async def test_send_message_builds_an_inline_keyboard() -> None:
    adapter, bot = build_adapter()
    buttons = ((InlineButton("وضعیت", "menu:status"),),)

    await adapter.send_message(5001, "منو", buttons=buttons)

    markup = bot.send_calls[0]["reply_markup"]
    assert isinstance(markup, InlineKeyboardMarkup)
    assert markup.inline_keyboard[0][0].text == "وضعیت"
    assert markup.inline_keyboard[0][0].callback_data == "menu:status"


async def test_edit_message_and_answer_callback_are_recorded() -> None:
    adapter, bot = build_adapter()

    await adapter.edit_message_text(
        5001, 77, "منوی جدید", buttons=((InlineButton("خانه", "menu:main"),),)
    )
    await adapter.answer_callback_query("callback-1", text="انجام شد")

    assert bot.edit_calls[0]["message_id"] == 77
    assert bot.edit_calls[0]["text"] == "منوی جدید"
    assert bot.answer_calls == [("callback-1", "انجام شد")]


def test_callback_query_is_converted_to_a_command_event() -> None:
    message = to_unified_message(callback_update(data="menu:pair"))

    assert message is not None
    assert message.source is Platform.TELEGRAM
    assert message.sender_id == 5001
    assert message.chat_id == 5001
    assert message.message_id == 11
    assert message.callback_data == "menu:pair"
    assert message.callback_id == "callback-1"
    assert message.is_relayable is False


def test_callback_without_message_falls_back_to_sender_chat() -> None:
    message = to_unified_message(callback_update(with_message=False))

    assert message is not None
    assert message.chat_id == 5001
    assert message.message_id == 0
    assert message.callback_data == "menu:main"


def test_default_http_timeouts_are_generous_for_media(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class CapturingBot:
        def __init__(self, token: str, request: Any = None) -> None:
            captured["request"] = request

    monkeypatch.setattr(telegram_module, "Bot", CapturingBot)

    TelegramAdapter(
        DUMMY_TOKEN,
        connect_timeout=30,
        read_timeout=300,
        write_timeout=600,
    )

    request = captured["request"]
    assert request.read_timeout == 300
    assert request._media_write_timeout == 600


# --------------------------------------------------------------- error mapping


@pytest.mark.parametrize(
    ("error", "expected_type"),
    [
        (Forbidden("bot was blocked by the user"), DeliveryError),
        (BadRequest("chat not found"), DeliveryError),
        (BadRequest("message is too long"), ApiError),
        (RetryAfter(9), RateLimitError),
        (TimedOut(), TransportError),
        (NetworkError("connection reset"), TransportError),
    ],
)
def test_translate_telegram_error(error: Exception, expected_type: type[Exception]) -> None:
    assert isinstance(translate_telegram_error(error), expected_type)  # type: ignore[arg-type]


def test_translate_retry_after_keeps_wait_time() -> None:
    mapped = translate_telegram_error(RetryAfter(9))

    assert isinstance(mapped, RateLimitError)
    assert mapped.retry_after == 9.0
    assert mapped.platform == "telegram"


def test_retry_after_seconds_accepts_both_shapes() -> None:
    """PTB types ``retry_after`` as ``int | timedelta``; both must work."""
    assert retry_after_seconds(9) == 9.0
    assert retry_after_seconds(timedelta(seconds=4)) == 4.0


async def test_send_error_is_mapped() -> None:
    adapter, _ = build_adapter(send_error=Forbidden("bot was blocked by the user"))

    with pytest.raises(DeliveryError):
        await adapter.send_message(5001, "hello")


async def test_rate_limit_error_is_mapped_on_send() -> None:
    adapter, _ = build_adapter(send_error=RetryAfter(3))

    with pytest.raises(RateLimitError) as excinfo:
        await adapter.send_message(5001, "hello")

    assert excinfo.value.retry_after == 3.0


# ----------------------------------------------------------------------- close


async def test_close_shuts_down_a_bot_the_adapter_created() -> None:
    adapter = TelegramAdapter(DUMMY_TOKEN)
    bot = FakeTelegramBot()
    adapter._bot = bot  # type: ignore[attr-defined]  # simulate an owned bot

    await adapter.close()

    assert bot.shutdown_called is True


async def test_close_leaves_an_injected_bot_alone() -> None:
    adapter, bot = build_adapter()

    await adapter.close()

    assert bot.shutdown_called is False
