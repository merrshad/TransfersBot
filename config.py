"""Application settings loaded from environment variables and an optional ``.env`` file.

Secrets are wrapped in :class:`pydantic.SecretStr` so they are never echoed by
``repr()`` or accidental logging. Importing this module has no side effects;
settings are only read when :func:`get_settings` is called.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    """Runtime configuration for both bots and the persistence layer.

    Environment variables are case-insensitive, so field ``telegram_bot_token``
    is populated from ``TELEGRAM_BOT_TOKEN``.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    telegram_bot_token: SecretStr
    bale_bot_token: SecretStr
    bale_api_base_url: str = "https://tapi.bale.ai"
    telegram_owner_id: int = Field(gt=0)
    bale_owner_id: int = Field(gt=0)
    max_users: int = Field(default=0, ge=0, description="0 means unlimited.")
    max_pairs_per_user: int = Field(default=1, ge=1)
    pair_request_ttl_hours: int = Field(default=24, ge=1)
    # Public bot usernames used to build t.me / ble.ir deep links.
    telegram_bot_username: str = "Tele_to_Bale_RoBot"
    bale_bot_username: str = "bale2telrobot"
    pair_invite_ttl_minutes: int = Field(default=15, ge=1)

    # --- group-to-group bridging ---------------------------------------
    # Multiple independent Telegram↔Bale links are stored in SQLite and are
    # created from inside a group by the owner with /groupsync. Both public and
    # private groups use the same code path; the bot must be a member/admin in
    # both. The two ids below are an optional first-run bootstrap only.
    group_sync_enabled: bool = False
    telegram_group_id: int | None = None
    bale_group_id: int | None = None
    group_invite_ttl_minutes: int = Field(default=15, ge=1)
    db_path: Path = Path("data/bridge.db")
    log_level: LogLevel = "INFO"
    poll_timeout_seconds: int = Field(default=25, ge=1, le=300)

    # --- delivery reliability -------------------------------------------
    # A delivery is leased (pending -> sending), retried with exponential
    # backoff and jitter, and only marked failed after MAX_ATTEMPTS.
    outbox_poll_interval_seconds: float = Field(default=2.0, gt=0)
    outbox_max_attempts: int = Field(default=5, ge=1)
    outbox_retry_base_seconds: float = Field(default=10.0, gt=0)
    outbox_retry_max_seconds: float = Field(default=300.0, gt=0)
    outbox_retry_jitter: float = Field(default=0.2, ge=0, le=1)
    outbox_stale_sending_seconds: int = Field(default=300, ge=1)
    outbox_media_timeout_terminal: bool = True
    # Delivered payloads are scrubbed immediately. These content-free sent
    # tombstones are deleted after this many days (0 disables the purge).
    outbox_sent_retention_days: int = Field(default=7, ge=0)
    shutdown_timeout_seconds: float = Field(default=30.0, gt=0)

    # Telegram uploads used to inherit PTB's 5s/20s defaults, which is far
    # below the time a real 20–50 MB upload needs. A timeout after the upload
    # was accepted made the outbox retry and duplicate the file.
    telegram_connect_timeout_seconds: float = Field(default=30.0, gt=0)
    telegram_read_timeout_seconds: float = Field(default=300.0, gt=0)
    telegram_write_timeout_seconds: float = Field(default=600.0, gt=0)

    # --- send-rate limits ----------------------------------------------
    # 0 disables a limit. Per-chat/per-user values prevent flooding one
    # recipient and one spammer; global values keep the bot below the
    # platform ceiling.
    rate_limit_enabled: bool = True
    telegram_global_messages_per_second: int = Field(default=25, ge=0)
    bale_global_messages_per_second: int = Field(default=20, ge=0)
    telegram_chat_messages_per_second: float = Field(default=1.0, ge=0)
    bale_chat_messages_per_second: float = Field(default=1.0, ge=0)
    user_messages_per_minute: int = Field(default=30, ge=0)
    user_rate_limit_action: Literal["queue", "reject"] = "queue"
    user_rate_limit_notice_cooldown_seconds: int = Field(default=60, ge=0)

    # --- health check ---------------------------------------------------
    # When set, the outbox worker rewrites this file after every pass. Docker
    # can health-check it with ``test -f`` and a freshness window.
    healthcheck_file: Path | None = None
    healthcheck_max_age_seconds: int = Field(default=90, ge=1)

    # --- media relaying -------------------------------------------------
    # The byte limits mirror the official API documentation (Telegram Bot API
    # ``getFile``/``sendPhoto``/… and https://docs.bale.ai): both platforms
    # allow a 20 MB ``getFile`` download, photos upload up to 10 MB and every
    # other file up to 50 MB. They are settings so an operator can tighten them
    # (or raise them if a platform lifts its cap) without touching the code.
    media_enabled: bool = True
    media_temp_dir: Path = Path("data/tmp")
    media_user_quota_mb: int = Field(default=200, ge=1)
    media_group_window_seconds: float = Field(default=0.8, ge=0, le=30)
    telegram_max_download_mb: int = Field(default=20, ge=1)
    telegram_max_upload_mb: int = Field(default=50, ge=1)
    telegram_max_photo_upload_mb: int = Field(default=10, ge=1)
    bale_max_download_mb: int = Field(default=20, ge=1)
    bale_max_upload_mb: int = Field(default=50, ge=1)
    bale_max_photo_upload_mb: int = Field(default=10, ge=1)

    @field_validator("telegram_bot_token", "bale_bot_token")
    @classmethod
    def _token_must_not_be_blank(cls, value: SecretStr) -> SecretStr:
        """Reject blank or whitespace-only tokens early."""
        if not value.get_secret_value().strip():
            raise ValueError("bot token must not be empty")
        return value

    @field_validator("bale_api_base_url")
    @classmethod
    def _normalise_base_url(cls, value: str) -> str:
        """Require an HTTP(S) URL and drop any trailing slash."""
        candidate = value.strip().rstrip("/")
        if not candidate.startswith(("http://", "https://")):
            raise ValueError("BALE_API_BASE_URL must start with http:// or https://")
        return candidate

    @field_validator("telegram_bot_username", "bale_bot_username")
    @classmethod
    def _normalise_bot_username(cls, value: str) -> str:
        """Accept ``@Name`` or ``Name`` and store the bare username."""
        candidate = value.strip().lstrip("@")
        if not candidate:
            raise ValueError("bot username must not be empty")
        return candidate


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton.

    The result is cached, so tests that monkeypatch the environment must call
    ``get_settings.cache_clear()`` afterwards.
    """
    return Settings()
