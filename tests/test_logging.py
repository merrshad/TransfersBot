"""Tests for :mod:`logging_setup`: no token, no message content, no leak paths."""

from __future__ import annotations

import io
import logging

import pytest

from logging_setup import (
    NOISY_LOGGER_LEVEL,
    NOISY_LOGGERS,
    REDACTED,
    SecretRedactingFilter,
    configure_logging,
    redact_secrets,
)

TOKEN = "123456789:AAHsecretTOKENvalue"


def test_redact_secrets_replaces_every_occurrence_in_strings() -> None:
    text = f"POST https://tapi.bale.ai/bot{TOKEN}/getUpdates and again {TOKEN}"

    redacted = redact_secrets(text, [TOKEN])

    assert TOKEN not in redacted
    assert redacted.count(REDACTED) == 2


def test_redact_secrets_walks_tuples_lists_and_mappings() -> None:
    args = (f"token={TOKEN}", [f"x{TOKEN}"], {"url": f"bot{TOKEN}"}, 42)

    redacted = redact_secrets(args, [TOKEN])

    assert TOKEN not in repr(redacted)
    assert redacted[3] == 42


def test_redact_secrets_passes_through_non_strings_and_ignores_empty_secrets() -> None:
    assert redact_secrets(42, [TOKEN]) == 42
    assert redact_secrets(None, [TOKEN]) is None
    assert redact_secrets(TOKEN, [""]) == TOKEN


def test_filter_scrubs_message_and_arguments() -> None:
    filter_ = SecretRedactingFilter([TOKEN])
    record = logging.LogRecord(
        name="t",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="url=%s",
        args=(f"https://tapi.bale.ai/bot{TOKEN}/sendMessage",),
        exc_info=None,
    )

    assert filter_.filter(record) is True
    assert TOKEN not in record.getMessage()


def test_filter_scrubs_preformatted_tracebacks() -> None:
    filter_ = SecretRedactingFilter([TOKEN])
    record = logging.LogRecord(
        name="t",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="boom",
        args=(),
        exc_info=None,
    )
    record.exc_text = f"Traceback ... bot{TOKEN}/getUpdates"

    filter_.filter(record)

    assert TOKEN not in (record.exc_text or "")


def test_filter_longest_secret_is_replaced_first() -> None:
    filter_ = SecretRedactingFilter(["abc", "abcdef"])

    assert filter_.secrets == ("abcdef", "abc")


def test_configure_logging_installs_exactly_one_redacting_handler() -> None:
    stream = io.StringIO()

    first = configure_logging("DEBUG", secrets=[TOKEN], stream=stream)
    second = configure_logging("INFO", secrets=[TOKEN], stream=stream)

    root = logging.getLogger()
    assert root.handlers == [second]
    assert logging.getLogger().level == logging.INFO
    assert first not in root.handlers

    logging.getLogger("tests.logging").warning("token=%s", TOKEN)
    output = stream.getvalue()
    assert REDACTED in output
    assert TOKEN not in output


def test_configure_logging_silences_noisy_libraries() -> None:
    configure_logging("DEBUG", stream=io.StringIO())

    for name in NOISY_LOGGERS:
        assert logging.getLogger(name).level == NOISY_LOGGER_LEVEL


def test_configure_logging_keeps_message_content_out_of_info_output() -> None:
    stream = io.StringIO()
    configure_logging("INFO", secrets=[TOKEN], stream=stream)

    logging.getLogger("bridge.core").debug("body=%s", "متن کاربر")
    logging.getLogger("bridge.core").info("bridge started")

    output = stream.getvalue()
    assert "bridge started" in output
    assert "متن کاربر" not in output


def test_invalid_level_names_are_rejected() -> None:
    with pytest.raises(ValueError):
        configure_logging("chatty", stream=io.StringIO())
