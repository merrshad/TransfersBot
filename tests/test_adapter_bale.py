"""Tests for :mod:`adapters.bale` — httpx is driven by MockTransport, no network."""

from __future__ import annotations

import io
import json
import logging
from typing import Any

import httpx
import pytest

from adapters.bale import BaleAdapter, parse_api_response, parse_update
from core.errors import ApiError, DeliveryError, RateLimitError, TransportError
from core.models import InlineButton, Platform, SentMessage
from logging_setup import configure_logging

TOKEN = "SECRET-BALE-TOKEN-123"
BASE_URL = "https://tapi.bale.ai"


class Transport(httpx.MockTransport):
    """MockTransport that records every request and replays scripted responses."""

    def __init__(self, responses: list[httpx.Response | Exception]) -> None:
        self.requests: list[httpx.Request] = []
        self._responses = list(responses)

        def handler(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            item = self._responses.pop(0) if self._responses else httpx.Response(200, json={"ok": True, "result": []})
            if isinstance(item, Exception):
                raise item
            return item

        super().__init__(handler)

    def last_body(self) -> dict[str, Any]:
        request = self.requests[-1]
        body = json.loads(request.content.decode("utf-8"))
        assert isinstance(body, dict)
        return body


def build_adapter(*responses: httpx.Response | Exception) -> tuple[BaleAdapter, Transport]:
    """Bale adapter wired to a scripted mock transport."""
    transport = Transport(list(responses))
    client = httpx.AsyncClient(transport=transport)
    return BaleAdapter(TOKEN, base_url=BASE_URL, client=client), transport


def ok(result: Any) -> httpx.Response:
    return httpx.Response(200, json={"ok": True, "result": result})


def bale_message(
    *,
    update_id: int = 1,
    text: str | None = "سلام",
    chat_type: str = "private",
    with_sender: bool = True,
    chat_id: int = 7001,
    message_id: int = 21,
    reply_to_message: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Raw Bale update payload as documented."""
    message: dict[str, Any] = {
        "message_id": message_id,
        "chat": {"id": chat_id, "type": chat_type},
        "date": 1767225600,
    }
    if text is not None:
        message["text"] = text
    if with_sender:
        message["from"] = {"id": chat_id, "is_bot": False, "first_name": "Ali"}
    if reply_to_message is not None:
        message["reply_to_message"] = reply_to_message
    return {"update_id": update_id, "message": message}


def bale_callback(
    *,
    update_id: int = 30,
    data: str = "menu:main",
    chat_id: int = 7001,
    message_id: int = 21,
) -> dict[str, Any]:
    """Raw Bale callback-query update as documented."""
    return {
        "update_id": update_id,
        "callback_query": {
            "id": "callback-1",
            "from": {"id": chat_id, "is_bot": False, "first_name": "Ali"},
            "message": {
                "message_id": message_id,
                "chat": {"id": chat_id, "type": "private"},
                "date": 1767225600,
            },
            "data": data,
        },
    }


# ------------------------------------------------------------------ parsing


def test_parse_update_converts_private_text_message() -> None:
    message = parse_update(bale_message())

    assert message is not None
    assert message.source is Platform.BALE
    assert message.sender_id == 7001
    assert message.chat_id == 7001
    assert message.message_id == 21
    assert message.text == "سلام"
    assert message.timestamp.tzinfo is not None
    assert message.key == ("bale", 7001, 21)


def test_parse_update_converts_group_message_with_sender_label() -> None:
    raw = bale_message(chat_type="group", chat_id=4497595490, message_id=33)
    raw["message"]["from"]["first_name"] = "مهدی"

    message = parse_update(raw)

    assert message is not None
    assert message.chat_type == "group"
    assert message.is_group is True
    assert message.chat_id == 4497595490
    assert message.sender_name == "مهدی"


def test_parse_update_converts_callback_query() -> None:
    message = parse_update(bale_callback(data="menu:pair"))

    assert message is not None
    assert message.source is Platform.BALE
    assert message.sender_id == 7001
    assert message.chat_id == 7001
    assert message.message_id == 21
    assert message.callback_data == "menu:pair"
    assert message.callback_id == "callback-1"
    assert message.is_relayable is False


def test_callback_without_message_falls_back_to_sender_chat() -> None:
    raw = bale_callback()
    del raw["callback_query"]["message"]

    message = parse_update(raw)

    assert message is not None
    assert message.chat_id == 7001
    assert message.message_id == 0
    assert message.callback_data == "menu:main"


def test_parse_update_keeps_reply_reference_and_entities() -> None:
    raw = bale_message(
        reply_to_message={"message_id": 4, "chat": {"id": 7001, "type": "private"}}
    )
    raw["message"]["entities"] = [{"type": "bold", "offset": 0, "length": 2}]

    message = parse_update(raw)

    assert message is not None
    assert message.reply_to is not None and message.reply_to.message_id == 4
    assert message.entities[0]["type"] == "bold"


@pytest.mark.parametrize(
    "raw",
    [
        {"update_id": 1},  # non-message update
        bale_message(text=None),  # media only
        bale_message(chat_type="channel"),
        bale_message(with_sender=False),  # Bale documents "from" as optional
        {"update_id": 1, "message": {"message_id": 1, "chat": {"id": 1, "type": "private"}, "text": "x"}},
    ],
)
def test_parse_update_skips_unsupported_payloads(raw: dict[str, Any]) -> None:
    assert parse_update(raw) is None


def test_parse_update_handles_missing_date() -> None:
    raw = bale_message()
    del raw["message"]["date"]

    message = parse_update(raw)

    assert message is not None
    assert message.timestamp.tzinfo is not None


# ------------------------------------------------------------ envelope handling


def test_ok_envelope_returns_result() -> None:
    assert parse_api_response(200, {"ok": True, "result": {"message_id": 5}}) == {"message_id": 5}


def test_double_wrapped_error_envelope_is_detected() -> None:
    body = {
        "ok": True,
        "result": {
            "ok": False,
            "error_code": 403,
            "description": "Forbidden: bot was blocked by the user",
        },
    }

    with pytest.raises(DeliveryError) as excinfo:
        parse_api_response(200, body)

    assert excinfo.value.permanent is True


def test_double_wrapped_success_envelope_is_unwrapped() -> None:
    body = {"ok": True, "result": {"ok": True, "result": {"message_id": 7}}}

    assert parse_api_response(200, body) == {"message_id": 7}


def test_error_envelope_on_http_200_is_raised() -> None:
    body = {"ok": False, "error_code": 400, "description": "Bad Request: message is too long"}

    with pytest.raises(ApiError) as excinfo:
        parse_api_response(200, body)

    assert excinfo.value.permanent is False
    assert excinfo.value.error_code == 400


def test_bare_result_without_envelope_is_tolerated() -> None:
    assert parse_api_response(200, {"message_id": 3}) == {"message_id": 3}


def test_http_429_bodyless_response_becomes_rate_limit() -> None:
    with pytest.raises(RateLimitError) as excinfo:
        parse_api_response(429, None)

    assert excinfo.value.retry_after > 0
    assert excinfo.value.permanent is False


def test_http_500_becomes_transport_error() -> None:
    with pytest.raises(TransportError):
        parse_api_response(500, None)


def test_http_403_without_body_is_a_delivery_error() -> None:
    with pytest.raises(DeliveryError):
        parse_api_response(403, None)


# -------------------------------------------------------------------- requests


async def test_send_message_uses_token_path_and_minimal_payload() -> None:
    adapter, transport = build_adapter(ok({"message_id": 42, "date": 1}))

    sent = await adapter.send_message(7001, "سلام")

    assert transport.requests[-1].url.path == f"/bot{TOKEN}/sendMessage"
    assert transport.last_body() == {"chat_id": 7001, "text": "سلام"}
    assert "parse_mode" not in transport.last_body()
    assert sent == SentMessage(platform=Platform.BALE, chat_id=7001, message_id=42)


async def test_send_message_adds_inline_keyboard() -> None:
    adapter, transport = build_adapter(ok({"message_id": 42}))

    await adapter.send_message(
        7001,
        "منو",
        buttons=((InlineButton("وضعیت", "menu:status"),),),
    )

    markup = transport.last_body()["reply_markup"]
    assert markup == {
        "inline_keyboard": [[{"text": "وضعیت", "callback_data": "menu:status"}]]
    }


async def test_edit_message_and_answer_callback_call_bale_methods() -> None:
    adapter, transport = build_adapter(ok({"message_id": 42}), ok(True))

    await adapter.edit_message_text(
        7001,
        21,
        "منوی جدید",
        buttons=((InlineButton("خانه", "menu:main"),),),
    )
    assert transport.requests[-1].url.path == f"/bot{TOKEN}/editMessageText"
    body = transport.last_body()
    assert body["message_id"] == 21
    assert body["reply_markup"]["inline_keyboard"][0][0]["callback_data"] == "menu:main"

    await adapter.answer_callback_query("callback-1", text="انجام شد")
    assert transport.requests[-1].url.path == f"/bot{TOKEN}/answerCallbackQuery"
    assert transport.last_body() == {
        "callback_query_id": "callback-1",
        "text": "انجام شد",
    }


async def test_send_message_includes_reply_to_message_id_when_replying() -> None:
    adapter, transport = build_adapter(ok({"message_id": 43}))

    await adapter.send_message(7001, "پاسخ", reply_to_message_id=21)

    assert transport.last_body()["reply_to_message_id"] == 21


async def test_send_message_truncates_over_long_text() -> None:
    adapter, transport = build_adapter(ok({"message_id": 44}))

    await adapter.send_message(7001, "x" * 5000)

    assert len(transport.last_body()["text"]) == 4096


async def test_send_message_requires_message_id_in_result() -> None:
    adapter, _ = build_adapter(ok({"date": 1}))

    with pytest.raises(ApiError):
        await adapter.send_message(7001, "hi")


async def test_get_me_returns_bot_user() -> None:
    adapter, transport = build_adapter(ok({"id": 1, "username": "bridge_bot"}))

    bot_user = await adapter.get_me()

    assert bot_user["username"] == "bridge_bot"
    assert transport.requests[-1].url.path == f"/bot{TOKEN}/getMe"


async def test_get_updates_sends_offset_limit_and_timeout() -> None:
    adapter, transport = build_adapter(ok([]))

    await adapter.poll_once(9, timeout=25)

    assert transport.requests[-1].url.path == f"/bot{TOKEN}/getUpdates"
    assert transport.last_body() == {"limit": 100, "timeout": 25, "offset": 9}
    assert "allowed_updates" not in transport.last_body()


async def test_get_updates_omits_offset_on_first_poll() -> None:
    adapter, transport = build_adapter(ok([]))

    await adapter.poll_once(None, timeout=5)

    assert "offset" not in transport.last_body()


async def test_get_updates_parses_and_advances_offset() -> None:
    updates = [
        bale_message(update_id=3),
        bale_message(update_id=4, text=None),
        bale_message(update_id=5, chat_type="channel"),
    ]
    adapter, _ = build_adapter(ok(updates))

    result = await adapter.poll_once(None, timeout=5)

    assert len(result.messages) == 1
    assert result.next_offset == 6
    assert result.received == 3


async def test_get_updates_with_empty_batch_keeps_offset() -> None:
    adapter, _ = build_adapter(ok([]))

    result = await adapter.poll_once(11, timeout=5)

    assert result.messages == ()
    assert result.next_offset is None


async def test_get_updates_rejects_unexpected_payload_shape() -> None:
    adapter, _ = build_adapter(ok({"unexpected": True}))

    with pytest.raises(ApiError):
        await adapter.poll_once(None, timeout=5)


async def test_base_url_trailing_slash_is_normalised() -> None:
    transport = Transport([ok([])])
    client = httpx.AsyncClient(transport=transport)
    adapter = BaleAdapter(TOKEN, base_url="https://tapi.bale.ai/", client=client)

    await adapter.poll_once(None, timeout=1)

    assert adapter.base_url == BASE_URL
    assert transport.requests[-1].url.host == "tapi.bale.ai"


# ------------------------------------------------------------- transport errors


async def test_timeout_becomes_transport_error_without_leaking_token() -> None:
    adapter, _ = build_adapter(httpx.ReadTimeout("read timed out"))

    with pytest.raises(TransportError) as excinfo:
        await adapter.send_message(7001, "hi")

    assert TOKEN not in str(excinfo.value)
    assert excinfo.value.permanent is False


async def test_connection_error_becomes_transport_error_without_leaking_token() -> None:
    adapter, _ = build_adapter(httpx.ConnectError("no route to host"))

    with pytest.raises(TransportError) as excinfo:
        await adapter.poll_once(None, timeout=1)

    assert TOKEN not in str(excinfo.value)
    assert TOKEN not in repr(excinfo.value)


async def test_non_json_body_is_reported_as_api_error() -> None:
    adapter, _ = build_adapter(httpx.Response(200, text="<html>hi</html>"))

    with pytest.raises(ApiError):
        await adapter.send_message(7001, "hi")


# ------------------------------------------------------------------ logging


async def test_token_never_reaches_the_log_stream() -> None:
    stream = io.StringIO()
    handler = configure_logging("DEBUG", secrets=[TOKEN], stream=stream)
    transport = Transport([httpx.ReadTimeout("timed out")])
    client = httpx.AsyncClient(transport=transport)
    adapter = BaleAdapter(TOKEN, base_url=BASE_URL, client=client)

    # Positive control: the filter must redact a token that is logged on purpose.
    logging.getLogger("tests.token").warning("url=%s", f"{BASE_URL}/bot{TOKEN}/getUpdates")
    with pytest.raises(TransportError):
        await adapter.poll_once(None, timeout=1)

    output = stream.getvalue()
    assert handler.stream is stream
    assert "***" in output, "the redaction filter must actually run"
    assert TOKEN not in output
    assert logging.getLogger("httpx").level == logging.WARNING
