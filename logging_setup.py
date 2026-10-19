"""Central logging configuration.

Two hard requirements drive this module:

1. **No bot token may ever reach a log sink.** Bot tokens live in request URLs
   (``…/bot<TOKEN>/sendMessage``), which httpx logs at INFO, so that logger is
   capped at WARNING *and* a redacting filter scrubs every record as a second
   line of defence.
2. **No user message content at INFO.** Bridge and adapters log identifiers and
   command *names*, never message bodies; the filter additionally guards against
   accidental inclusions.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
REDACTED = "***"

#: Loggers whose INFO/DEBUG output would leak tokens (URLs) or message bodies.
NOISY_LOGGERS: tuple[str, ...] = ("httpx", "httpcore", "telegram", "telegram.ext")
NOISY_LOGGER_LEVEL = logging.WARNING


def redact_secrets(value: Any, secrets: Sequence[str]) -> Any:
    """Return ``value`` with every occurrence of each secret replaced.

    Handles the shapes a :class:`logging.LogRecord` can hold in ``msg``/``args``:
    strings, tuples, lists and mappings. Other types pass through untouched.
    """
    active = [secret for secret in secrets if secret]
    if not active:
        return value
    if isinstance(value, str):
        redacted = value
        for secret in active:
            redacted = redacted.replace(secret, REDACTED)
        return redacted
    if isinstance(value, tuple):
        return tuple(redact_secrets(item, active) for item in value)
    if isinstance(value, list):
        return [redact_secrets(item, active) for item in value]
    if isinstance(value, Mapping):
        return {key: redact_secrets(item, active) for key, item in value.items()}
    return value


class SecretRedactingFilter(logging.Filter):
    """Scrubs known secrets from a log record's message, arguments and traceback.

    Longest secrets are replaced first so a token that contains a shorter
    configured string is still removed completely.
    """

    def __init__(self, secrets: Iterable[str] = ()) -> None:
        super().__init__()
        self._secrets: tuple[str, ...] = tuple(
            sorted({secret for secret in secrets if secret}, key=len, reverse=True)
        )

    @property
    def secrets(self) -> tuple[str, ...]:
        """Configured secrets (values are returned; never log this)."""
        return self._secrets

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact the record in place and always allow it through."""
        if not self._secrets:
            return True
        if isinstance(record.msg, str):
            record.msg = redact_secrets(record.msg, self._secrets)
        if record.args:
            record.args = redact_secrets(record.args, self._secrets)
        if record.exc_text:
            record.exc_text = redact_secrets(record.exc_text, self._secrets)
        return True


def configure_logging(
    level: str = "INFO",
    *,
    secrets: Sequence[str] = (),
    stream: Any | None = None,
    log_format: str = DEFAULT_LOG_FORMAT,
) -> logging.Handler:
    """Install a single redacting handler on the root logger.

    Existing root handlers are removed first, both to make the call idempotent
    and to guarantee that no unfiltered handler can print a token.

    Args:
        level: Root log level name.
        secrets: Values that must never appear in logs (bot tokens).
        stream: Output stream; defaults to ``sys.stderr`` as usual.
        log_format: Formatter pattern.

    Returns:
        The installed :class:`logging.Handler`.
    """
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)

    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(log_format))
    handler.addFilter(SecretRedactingFilter(secrets))
    root.addHandler(handler)
    root.setLevel(level.upper())

    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(NOISY_LOGGER_LEVEL)
    return handler


__all__ = [
    "DEFAULT_LOG_FORMAT",
    "NOISY_LOGGERS",
    "NOISY_LOGGER_LEVEL",
    "REDACTED",
    "SecretRedactingFilter",
    "configure_logging",
    "redact_secrets",
]
