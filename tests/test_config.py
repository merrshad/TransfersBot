"""Tests for :mod:`config`: defaults, validation, secret hygiene."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from config import Settings, get_settings

REQUIRED_ENV = {
    "TELEGRAM_BOT_TOKEN": "telegram-token-value",
    "BALE_BOT_TOKEN": "bale-token-value",
    "TELEGRAM_OWNER_ID": "111",
    "BALE_OWNER_ID": "222",
}


def make_settings(**overrides: object) -> Settings:
    """Build Settings from explicit values, never from a real ``.env`` file."""
    values: dict[str, object] = {
        "telegram_bot_token": "telegram-token-value",
        "bale_bot_token": "bale-token-value",
        "telegram_owner_id": 111,
        "bale_owner_id": 222,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[arg-type]


def test_defaults_match_specification() -> None:
    settings = make_settings()

    assert settings.bale_api_base_url == "https://tapi.bale.ai"
    assert settings.max_users == 0
    assert settings.max_pairs_per_user == 1
    assert settings.pair_request_ttl_hours == 24
    assert settings.db_path == Path("data/bridge.db")
    assert settings.log_level == "INFO"
    assert settings.outbox_max_attempts == 5
    assert settings.outbox_retry_base_seconds == 10.0
    assert settings.outbox_retry_jitter == 0.2
    assert settings.outbox_media_timeout_terminal is True
    assert settings.outbox_sent_retention_days == 7
    assert settings.telegram_bot_username == "Tele_to_Bale_RoBot"
    assert settings.bale_bot_username == "bale2telrobot"
    assert settings.pair_invite_ttl_minutes == 15
    assert settings.telegram_write_timeout_seconds == 600.0
    assert settings.user_messages_per_minute == 30
    assert settings.user_rate_limit_action == "queue"
    assert settings.rate_limit_enabled is True
    assert settings.healthcheck_file is None
    assert settings.healthcheck_max_age_seconds == 90


@pytest.mark.parametrize("missing", sorted(REQUIRED_ENV))
def test_required_variables_are_mandatory(monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv(missing, raising=False)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_environment_variables_are_read_case_insensitively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("MAX_PAIRS_PER_USER", "3")
    get_settings.cache_clear()
    try:
        settings = get_settings()
        assert settings.max_pairs_per_user == 3
        assert settings.telegram_owner_id == 111
    finally:
        get_settings.cache_clear()


def test_tokens_are_secret_and_never_rendered_in_repr() -> None:
    settings = make_settings(telegram_bot_token="super-secret-token")

    assert "super-secret-token" not in repr(settings)
    assert "super-secret-token" not in str(settings.telegram_bot_token)
    assert settings.telegram_bot_token.get_secret_value() == "super-secret-token"


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_tokens_are_rejected(blank: str) -> None:
    with pytest.raises(ValidationError):
        make_settings(telegram_bot_token=blank)


def test_bot_usernames_are_normalised() -> None:
    settings = make_settings(
        telegram_bot_username="@MyTelegramBot",
        bale_bot_username=" MyBaleBot ",
    )

    assert settings.telegram_bot_username == "MyTelegramBot"
    assert settings.bale_bot_username == "MyBaleBot"


def test_base_url_trailing_slash_is_stripped() -> None:
    settings = make_settings(bale_api_base_url="https://tapi.bale.ai/")
    assert settings.bale_api_base_url == "https://tapi.bale.ai"


@pytest.mark.parametrize("bad_url", ["tapi.bale.ai", "ftp://tapi.bale.ai", "/relative"])
def test_base_url_must_be_http(bad_url: str) -> None:
    with pytest.raises(ValidationError):
        make_settings(bale_api_base_url=bad_url)


def test_max_users_rejects_negative_values() -> None:
    with pytest.raises(ValidationError):
        make_settings(max_users=-1)


def test_max_users_zero_means_unlimited() -> None:
    assert make_settings(max_users=0).max_users == 0


def test_pair_limits_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        make_settings(max_pairs_per_user=0)
    with pytest.raises(ValidationError):
        make_settings(pair_request_ttl_hours=0)


def test_invalid_log_level_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_settings(log_level="VERBOSE")


def test_owner_ids_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        make_settings(telegram_owner_id=0)


def test_reliability_settings_are_validated() -> None:
    with pytest.raises(ValidationError):
        make_settings(outbox_max_attempts=0)
    with pytest.raises(ValidationError):
        make_settings(outbox_retry_jitter=1.5)
    with pytest.raises(ValidationError):
        make_settings(user_rate_limit_action="drop")
    with pytest.raises(ValidationError):
        make_settings(telegram_chat_messages_per_second=-1)


def test_env_example_documents_every_variable() -> None:
    example = Path(__file__).resolve().parents[1] / ".env.example"
    content = example.read_text(encoding="utf-8")

    for variable in [
        "TELEGRAM_BOT_TOKEN",
        "BALE_BOT_TOKEN",
        "BALE_API_BASE_URL",
        "TELEGRAM_OWNER_ID",
        "BALE_OWNER_ID",
        "MAX_USERS",
        "MAX_PAIRS_PER_USER",
        "PAIR_REQUEST_TTL_HOURS",
        "DB_PATH",
        "LOG_LEVEL",
        "OUTBOX_MAX_ATTEMPTS",
        "OUTBOX_RETRY_BASE_SECONDS",
        "OUTBOX_RETRY_JITTER",
        "SHUTDOWN_TIMEOUT_SECONDS",
        "TELEGRAM_GLOBAL_MESSAGES_PER_SECOND",
        "BALE_GLOBAL_MESSAGES_PER_SECOND",
        "TELEGRAM_CHAT_MESSAGES_PER_SECOND",
        "BALE_CHAT_MESSAGES_PER_SECOND",
        "USER_MESSAGES_PER_MINUTE",
        "USER_RATE_LIMIT_ACTION",
        "HEALTHCHECK_FILE",
    ]:
        assert variable in content, f"{variable} missing from .env.example"


def test_env_example_contains_no_real_looking_token() -> None:
    content = (Path(__file__).resolve().parents[1] / ".env.example").read_text("utf-8")
    # A real Telegram/Bale token looks like ``123456789:AA...``.
    assert re.search(r"\d{6,}:[A-Za-z0-9_-]{20,}", content) is None
