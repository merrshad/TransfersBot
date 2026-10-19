"""Tests for shared adapter helpers and messaging-error classification."""

from __future__ import annotations

import logging

import pytest

from adapters.base import MAX_MESSAGE_TEXT_LENGTH, clamp_message_text
from core.errors import (
    ApiError,
    DeliveryError,
    DomainError,
    MessagingError,
    RateLimitError,
    TransportError,
    classify_messaging_error,
    looks_unreachable,
)


def test_short_text_is_returned_unchanged() -> None:
    assert clamp_message_text("سلام") == "سلام"
    assert clamp_message_text("x" * MAX_MESSAGE_TEXT_LENGTH) == "x" * MAX_MESSAGE_TEXT_LENGTH


def test_long_text_is_truncated_to_the_platform_limit() -> None:
    clamped = clamp_message_text("x" * (MAX_MESSAGE_TEXT_LENGTH + 500))

    assert len(clamped) == MAX_MESSAGE_TEXT_LENGTH
    assert clamped.endswith("…")


def test_truncation_logs_lengths_but_not_content() -> None:
    records: list[str] = []

    class Recorder(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    logger = logging.getLogger("tests.truncation")
    handler = Recorder()
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    try:
        clamp_message_text("secret-payload-" * 400, logger=logger)
    finally:
        logger.removeHandler(handler)

    assert records
    assert "secret-payload" not in "\n".join(records)
    assert "truncated" in records[0]


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"status_code": 403}, True),
        ({"status_code": 404}, True),
        ({"error_code": 403}, True),
        ({"status_code": 400, "description": "Bad Request: bot was blocked by the user"}, True),
        ({"status_code": 400, "description": "chat not found"}, True),
        ({"description": "user is deactivated"}, True),
        ({"status_code": 400, "description": "message is too long"}, False),
        ({"status_code": 500, "description": "internal"}, False),
        ({"description": "unknown"}, False),
    ],
)
def test_looks_unreachable(kwargs: dict[str, object], expected: bool) -> None:
    assert looks_unreachable(**kwargs) is expected  # type: ignore[arg-type]


def test_rate_limit_wins_over_everything_else() -> None:
    error = classify_messaging_error(
        description="Too Many Requests",
        status_code=429,
        error_code=429,
        retry_after=7.5,
        platform="bale",
    )

    assert isinstance(error, RateLimitError)
    assert error.retry_after == 7.5
    assert error.permanent is False
    assert error.platform == "bale"


def test_rate_limit_without_retry_after_uses_default() -> None:
    error = classify_messaging_error(description="slow down", error_code=429)

    assert isinstance(error, RateLimitError)
    assert error.retry_after > 0


def test_server_error_is_transient_transport_error() -> None:
    error = classify_messaging_error(description="boom", status_code=503)

    assert isinstance(error, TransportError)
    assert error.permanent is False


def test_forbidden_status_is_permanent_delivery_error() -> None:
    error = classify_messaging_error(
        description="Forbidden: bot was blocked by the user", status_code=403
    )

    assert isinstance(error, DeliveryError)
    assert error.permanent is True
    assert error.status_code == 403


def test_unknown_chat_is_permanent_delivery_error() -> None:
    error = classify_messaging_error(
        description="Bad Request: chat not found", status_code=400, error_code=400
    )

    assert isinstance(error, DeliveryError)


def test_other_client_error_is_non_permanent_api_error() -> None:
    error = classify_messaging_error(
        description="Bad Request: message is too long", status_code=400, error_code=400
    )

    assert isinstance(error, ApiError)
    assert error.permanent is False


def test_messaging_errors_are_not_domain_errors() -> None:
    for error in (
        MessagingError("x"),
        ApiError("x"),
        DeliveryError("x"),
        TransportError("x"),
        RateLimitError("x", retry_after=1),
    ):
        assert not isinstance(error, DomainError)
        assert isinstance(error, MessagingError)


def test_permanent_flags_are_class_level_contract() -> None:
    assert DeliveryError.permanent is True
    assert ApiError.permanent is False
    assert TransportError.permanent is False
    assert RateLimitError.permanent is False
