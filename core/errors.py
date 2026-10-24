"""Domain-level errors shared by the service and command layers.

Every error carries a stable machine-readable :attr:`DomainError.code` and a
human-readable (Persian) message suitable for sending back to a messenger user.
Unexpected programming errors are intentionally *not* subclasses of
:class:`DomainError`; only rule violations are caught and turned into replies.
"""

from __future__ import annotations

from typing import ClassVar


class DomainError(Exception):
    """Base class for expected, user-facing rule violations."""

    code: ClassVar[str] = "domain_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class UserNotFoundError(DomainError):
    """Raised when a user id does not exist in the bridge database."""

    code = "user_not_found"

    def __init__(self, message: str = "کاربر موردنظر پیدا نشد.") -> None:
        super().__init__(message)


class UserLimitReachedError(DomainError):
    """Raised when ``MAX_USERS`` would be exceeded by a new registration."""

    code = "user_limit_reached"

    def __init__(self, message: str = "ظرفیت کاربران ربات تکمیل شده است.") -> None:
        super().__init__(message)


class UserBannedError(DomainError):
    """Raised when a banned user attempts a restricted action."""

    code = "user_banned"

    def __init__(self, message: str = "دسترسی شما به ربات مسدود شده است.") -> None:
        super().__init__(message)


class PermissionDeniedError(DomainError):
    """Raised when a user is not allowed to perform an action."""

    code = "permission_denied"

    def __init__(self, message: str = "شما اجازه انجام این کار را ندارید.") -> None:
        super().__init__(message)


class CannotBanOwnerError(DomainError):
    """Raised when an owner tries to ban an owner account."""

    code = "cannot_ban_owner"

    def __init__(self, message: str = "امکان مسدود کردن مالک وجود ندارد.") -> None:
        super().__init__(message)


class SelfPairingError(DomainError):
    """Raised when a user tries to pair with themselves."""

    code = "self_pairing"

    def __init__(self, message: str = "نمی‌توانید با خودتان جفت شوید.") -> None:
        super().__init__(message)


class AlreadyPairedError(DomainError):
    """Raised when an active pair already exists between two users."""

    code = "already_paired"

    def __init__(self, message: str = "این دو کاربر از قبل جفت هستند.") -> None:
        super().__init__(message)


class DuplicatePairRequestError(DomainError):
    """Raised when a pending request already exists between two users."""

    code = "duplicate_pair_request"

    def __init__(self, message: str = "یک درخواست جفت‌سازی در انتظار پاسخ وجود دارد.") -> None:
        super().__init__(message)


class PairLimitReachedError(DomainError):
    """Raised when a user already has the maximum number of active pairs."""

    code = "pair_limit_reached"

    def __init__(self, message: str = "تعداد جفت‌های فعال به سقف مجاز رسیده است.") -> None:
        super().__init__(message)


class PairRequestNotFoundError(DomainError):
    """Raised when a pairing request id is unknown or invisible to the actor."""

    code = "pair_request_not_found"

    def __init__(self, message: str = "درخواست جفت‌سازی پیدا نشد.") -> None:
        super().__init__(message)


class PairNotFoundError(DomainError):
    """Raised when a pair id is unknown or the actor is not a member."""

    code = "pair_not_found"

    def __init__(self, message: str = "جفت موردنظر پیدا نشد.") -> None:
        super().__init__(message)


class PairInviteInvalidError(DomainError):
    """Raised when a deep-link pairing invite is unknown, used or expired."""

    code = "pair_invite_invalid"

    def __init__(
        self,
        message: str = "لینک جفت‌سازی معتبر نیست یا منقضی شده است.",
    ) -> None:
        super().__init__(message)


class NotRequestTargetError(DomainError):
    """Raised when someone other than the target accepts/rejects a request."""

    code = "not_request_target"

    def __init__(self, message: str = "فقط دریافت‌کننده درخواست می‌تواند آن را بپذیرد یا رد کند.") -> None:
        super().__init__(message)


class NotRequesterError(DomainError):
    """Raised when someone other than the requester cancels a request."""

    code = "not_requester"

    def __init__(self, message: str = "فقط فرستنده درخواست می‌تواند آن را لغو کند.") -> None:
        super().__init__(message)


class RequestExpiredError(DomainError):
    """Raised when an operation targets a pending request past its TTL."""

    code = "request_expired"

    def __init__(self, message: str = "مهلت این درخواست به پایان رسیده است.") -> None:
        super().__init__(message)


class RequestNotPendingError(DomainError):
    """Raised when a request is no longer pending (accepted/rejected/...)."""

    code = "request_not_pending"

    def __init__(self, message: str = "این درخواست دیگر در وضعیت انتظار نیست.") -> None:
        super().__init__(message)


class PairNotActiveError(DomainError):
    """Raised when an operation requires an active pair but it is not active."""

    code = "pair_not_active"

    def __init__(self, message: str = "این جفت در وضعیت فعال نیست.") -> None:
        super().__init__(message)


class InvalidTransitionError(DomainError):
    """Raised when a state machine transition is not allowed."""

    code = "invalid_transition"

    def __init__(self, message: str = "تغییر وضعیت درخواستی مجاز نیست.") -> None:
        super().__init__(message)


class DuplicateMessageError(DomainError):
    """Raised when a source message was already stored for another pair."""

    code = "duplicate_message"

    def __init__(self, message: str = "این پیام قبلاً ثبت شده است.") -> None:
        super().__init__(message)


class MediaTooLargeError(DomainError):
    """Raised when a file exceeds the limit of the platform it must go to.

    Only the sizes are named — never the content or the file name.
    """

    code = "media_too_large"

    def __init__(
        self,
        message: str | None = None,
        *,
        size_bytes: int = 0,
        limit_bytes: int = 0,
        kind: str = "فایل",
    ) -> None:
        self.size_bytes = size_bytes
        self.limit_bytes = limit_bytes
        self.kind = kind
        super().__init__(
            message
            or (
                f"این {kind} ارسال نشد: حجم آن ({_megabytes(size_bytes)}) از حد مجاز "
                f"مقصد ({_megabytes(limit_bytes)}) بیشتر است."
            )
        )


class MediaUnsupportedError(DomainError):
    """Raised when a media kind cannot be delivered to the target platform."""

    code = "media_unsupported"

    def __init__(self, message: str | None = None, *, kind: str = "رسانه") -> None:
        self.kind = kind
        super().__init__(message or f"ارسال این نوع رسانه پشتیبانی نمی‌شود: {kind}")


def _megabytes(size_bytes: int) -> str:
    """Human-readable size with one decimal, for user-facing notices."""
    return f"{size_bytes / (1024 * 1024):.1f} MB"


class InvalidCommandError(DomainError):
    """Raised when a command is malformed or unknown."""

    code = "invalid_command"

    def __init__(self, message: str = "دستور نامعتبر است. برای راهنما /help را بفرستید.") -> None:
        super().__init__(message)


class InvalidArgumentError(DomainError):
    """Raised when a command has a missing or malformed argument."""

    code = "invalid_argument"

    def __init__(self, message: str = "آرگومان دستور نامعتبر است.") -> None:
        super().__init__(message)


# ---------------------------------------------------------------------------
# Messaging failures. These are deliberately *not* ``DomainError`` subclasses:
# the command router turns ``DomainError`` into a reply to the user, whereas a
# failed delivery must be handled by the delivery layer (retry, abort, notify).
# ---------------------------------------------------------------------------


class MessagingError(Exception):
    """Base class for failures while talking to a messenger platform."""

    permanent: ClassVar[bool] = False

    def __init__(
        self,
        message: str,
        *,
        platform: str | None = None,
        status_code: int | None = None,
        error_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.platform = platform
        self.status_code = status_code
        self.error_code = error_code


class TransportError(MessagingError):
    """Network-level failure: no usable API response was received.

    Always worth retrying later; never a reason to abandon a recipient.
    """

    permanent = False


class ApiError(MessagingError):
    """The messenger API answered with an error that is neither retryable nor
    a known unreachable-recipient condition."""

    permanent = False


class DeliveryError(MessagingError):
    """The recipient cannot be reached (blocked the bot, deactivated, bad id).

    ``permanent`` is ``True``: retrying the same chat id will never succeed.
    """

    permanent = True


class RateLimitError(MessagingError):
    """The messenger throttled us; wait ``retry_after`` seconds before retrying."""

    permanent = False

    def __init__(
        self,
        message: str,
        *,
        retry_after: float,
        platform: str | None = None,
        status_code: int | None = None,
        error_code: int | None = None,
    ) -> None:
        super().__init__(
            message, platform=platform, status_code=status_code, error_code=error_code
        )
        self.retry_after = float(retry_after)


DEFAULT_RETRY_AFTER_SECONDS = 5.0

#: Lower-cased fragments that identify an unreachable recipient.
UNREACHABLE_MARKERS: tuple[str, ...] = (
    "blocked",
    "chat not found",
    "user not found",
    "user is deactivated",
    "peer id invalid",
    "peer_id_invalid",
    "bot can't initiate conversation",
    "bot was kicked",
    "not enough rights",
    "forbidden",
)


def looks_unreachable(
    *,
    status_code: int | None = None,
    error_code: int | None = None,
    description: str | None = None,
) -> bool:
    """Heuristically decide whether an API error means “cannot deliver, ever”.

    Telegram uses HTTP/error code 403 for blocked/deactivated chats and 404 for
    unknown chats. Bale's documentation does not pin down its codes or wording,
    so the description is also inspected (see ``README.md``).
    """
    if status_code in {403, 404}:
        return True
    if error_code in {403, 404}:
        return True
    text = (description or "").lower()
    return any(marker in text for marker in UNREACHABLE_MARKERS)


def classify_messaging_error(
    *,
    description: str,
    status_code: int | None = None,
    error_code: int | None = None,
    retry_after: float | None = None,
    platform: str | None = None,
) -> MessagingError:
    """Map a platform error response to the right :class:`MessagingError`.

    Precedence: rate limit → server/transport → unreachable recipient → other.
    """
    if retry_after is not None or error_code == 429 or status_code == 429:
        return RateLimitError(
            f"{description} (rate limited)",
            retry_after=retry_after if retry_after is not None else DEFAULT_RETRY_AFTER_SECONDS,
            platform=platform,
            status_code=status_code,
            error_code=error_code,
        )
    if status_code is not None and status_code >= 500:
        return TransportError(
            f"{description} (HTTP {status_code})", platform=platform, status_code=status_code
        )
    if looks_unreachable(
        status_code=status_code, error_code=error_code, description=description
    ):
        return DeliveryError(
            description,
            platform=platform,
            status_code=status_code,
            error_code=error_code,
        )
    return ApiError(
        description, platform=platform, status_code=status_code, error_code=error_code
    )
