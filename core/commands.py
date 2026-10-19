"""Command parsing and routing, independent of any messenger SDK.

The router never sends anything. It takes a registered actor id plus raw text
(or inline-button callback data) and returns a tuple of :class:`Action` objects
describing what an adapter should deliver. Actions carry optional inline
keyboards; the bridge turns actor-facing replies to a callback into an
:class:`EditMessageAction` so menus update in place.

Plain text that is not a command yields an empty action tuple, because in that
case the caller forwards it to the actor's paired partner instead.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from core.errors import (
    DomainError,
    InvalidArgumentError,
    InvalidCommandError,
    PairNotFoundError,
    PermissionDeniedError,
    UserNotFoundError,
)
from core.ids import normalise_public_id
from core.models import InlineButton, OutboxStats
from core.pairing import Pair, PairingService, PairRequest
from core.users import User, UserService

#: Supplies a point-in-time outbox snapshot for the owner's ``/status``.
OutboxStatsProvider = Callable[[datetime], OutboxStats]

# --------------------------------------------------------------------- actions


@dataclass(frozen=True, slots=True)
class Action:
    """Marker base class for outbound side effects requested by a command."""


@dataclass(frozen=True, slots=True)
class SendMessageAction(Action):
    """Ask the adapter to deliver ``text`` to the internal user ``recipient_id``.

    ``related_request_id`` carries the pairing request this notification belongs
    to, so the delivery layer can react when the message cannot be delivered
    (for example the target blocked the bot).
    """

    recipient_id: int
    text: str
    reply_to_message_id: int | None = None
    related_request_id: int | None = None
    buttons: tuple[tuple[InlineButton, ...], ...] = ()


@dataclass(frozen=True, slots=True)
class EditMessageAction(Action):
    """Replace the text and keyboard of an existing message (callback menus)."""

    recipient_id: int
    message_id: int
    text: str
    buttons: tuple[tuple[InlineButton, ...], ...] = ()


# -------------------------------------------------------------------- parsing


@dataclass(frozen=True, slots=True)
class Command:
    """A parsed ``/command arg1 arg2`` line."""

    name: str
    args: tuple[str, ...]
    raw: str

    def int_arg(self, index: int, *, usage: str) -> int:
        """Return argument ``index`` as an int.

        Raises:
            InvalidArgumentError: If the argument is missing or not an integer.
        """
        if index >= len(self.args):
            raise InvalidArgumentError(f"استفاده: {usage}")
        raw = self.args[index]
        try:
            return int(raw)
        except ValueError as exc:
            raise InvalidArgumentError(
                f"«{raw}» عدد معتبر نیست.\nاستفاده: {usage}"
            ) from exc

    def public_id_arg(self, index: int, *, usage: str) -> str:
        """Return a normalised user public id.

        Raises:
            InvalidArgumentError: If the argument is missing or malformed.
        """
        if index >= len(self.args):
            raise InvalidArgumentError(f"استفاده: {usage}")
        raw = self.args[index]
        public_id = normalise_public_id(raw)
        if public_id is None:
            raise InvalidArgumentError(
                f"«{raw}» شناسهٔ کاربر معتبر نیست.\nاستفاده: {usage}"
            )
        return public_id


HandlerFn = Callable[[User, Command], "tuple[Action, ...]"]


def parse_command(text: str | None) -> Command | None:
    """Parse raw text into a :class:`Command`.

    Returns:
        ``None`` when the text is not a command (empty, or not starting with
        ``/``), so callers can treat it as a regular message.
    """
    if not text:
        return None
    stripped = text.strip()
    if not stripped.startswith("/"):
        return None
    parts = stripped.split()
    if not parts:
        return None
    head = parts[0][1:]
    head = head.split("@", 1)[0].lower()
    if not head:
        return None
    return Command(name=head, args=tuple(parts[1:]), raw=stripped)


# -------------------------------------------------------------------- routing


class CommandRouter:
    """Maps commands to :class:`Action` tuples for a registered actor.

    Args:
        users: User service used for lookups and owner checks.
        pairing: Pairing service implementing every pairing rule.
        stats: Optional outbox snapshot provider (queue length, 24-hour errors,
            last relay) used by the owner's ``/status``.
        clock: Injectable UTC clock for the stats snapshot.
    """

    HELP_TEXT = (
        "📚 راهنمای پل بله ↔ تلگرام\n"
        "\n"
        "🔹 /start – شروع و منوی اصلی\n"
        "🔹 /whoami – شناسه و وضعیت شما\n"
        "🔹 /status – جفت‌ها و درخواست‌ها\n"
        "🔹 /pair <شناسه کاربر> – درخواست جفت‌سازی\n"
        "🔹 /pending – درخواست‌های دریافتی\n"
        "🔹 /accept <شناسه درخواست> – پذیرش\n"
        "🔹 /reject <شناسه درخواست> – رد\n"
        "🔹 /cancel <شناسه درخواست> – لغو درخواست ارسالی\n"
        "🔹 /unpair [شناسه جفت] – پایان دادن به جفت\n"
        "🔹 /help – همین راهنما\n"
        "\n"
        "برای بیشتر کارها می‌توانید از دکمه‌های شیشه‌ای زیر پیام‌ها هم استفاده کنید."
    )
    UNREGISTERED_TEXT = "شما هنوز ثبت‌نام نکرده‌اید. لطفاً /start را بفرستید."
    OWNER_HELP_TEXT = (
        "/users [تعداد] – فهرست کاربران\n"
        "/ban <شناسه کاربر> – مسدود کردن کاربر\n"
        "/unban <شناسه کاربر> – رفع مسدودی کاربر\n"
        "/status – وضعیت کلی پل\n"
    )
    USER_LIST_LIMIT = 50

    def __init__(
        self,
        users: UserService,
        pairing: PairingService,
        stats: OutboxStatsProvider | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._users = users
        self._pairing = pairing
        self._stats = stats
        self._clock = clock or (lambda: datetime.now(UTC))

    # ------------------------------------------------------------- public API

    def handle(self, actor_id: int, text: str | None) -> tuple[Action, ...]:
        """Route ``text`` from ``actor_id`` and return the actions to perform."""
        command = parse_command(text)
        if command is None:
            return ()

        actor = self._users.get(actor_id)
        if actor is None:
            return (self._send(actor_id, self.UNREGISTERED_TEXT),)

        handler = self._handlers().get(command.name)
        if handler is None:
            return (
                self._send(
                    actor_id, str(InvalidCommandError()), buttons=self._main_buttons(actor)
                ),
            )

        try:
            return self._decorate(actor, handler(actor, command))
        except DomainError as exc:
            return (
                self._send(actor_id, str(exc), buttons=self._main_buttons(actor)),
            )

    def handle_callback(self, actor_id: int, data: str) -> tuple[Action, ...]:
        """Route one inline-button press.

        Callback data is deliberately a small, closed vocabulary; anything
        unknown falls back to the main menu. Parameterised actions reuse the
        same handlers (and therefore the same permission checks) as commands.
        """
        actor = self._users.get(actor_id)
        if actor is None:
            return (self._send(actor_id, self.UNREGISTERED_TEXT),)

        if data in {"menu:main", "menu:start"}:
            return (
                self._send(
                    actor.id,
                    "🏠 منوی اصلی\n\nیکی از گزینه‌های زیر را انتخاب کنید.",
                    buttons=self._main_buttons(actor),
                ),
            )
        if data == "menu:help":
            return self.handle(actor.id, "/help")
        if data == "menu:whoami":
            return self.handle(actor.id, "/whoami")
        if data == "menu:status":
            return self.handle(actor.id, "/status")
        if data == "menu:pending":
            return self.handle(actor.id, "/pending")
        if data == "menu:unpair":
            return self.handle(actor.id, "/status")
        if data == "menu:pair":
            return (self._pair_prompt(actor),)
        if data == "menu:owner":
            return (self._owner_menu(actor),)
        if data == "menu:users":
            return self.handle(actor.id, f"/users {min(self.USER_LIST_LIMIT, 10)}")
        if data == "menu:ban":
            return (self._owner_prompt(actor, "ban"),)
        if data == "menu:unban":
            return (self._owner_prompt(actor, "unban"),)

        for prefix, command in (
            ("accept", "accept"),
            ("reject", "reject"),
            ("cancel", "cancel"),
            ("unpair", "unpair"),
        ):
            value = self._callback_int(data, prefix)
            if value is not None:
                return self.handle(actor.id, f"/{command} {value}")

        for prefix in ("ban", "unban"):
            if data.startswith(prefix + ":"):
                public_id = normalise_public_id(data.split(":", 1)[1])
                if public_id is not None:
                    return self.handle(actor.id, f"/{prefix} {public_id}")

        return (
            self._send(
                actor.id,
                "این دکمه دیگر معتبر نیست.\nاز منوی اصلی ادامه دهید.",
                buttons=self._main_buttons(actor),
            ),
        )

    # ---------------------------------------------------------------- helpers

    def _send(
        self,
        recipient_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        related_request_id: int | None = None,
        buttons: tuple[tuple[InlineButton, ...], ...] = (),
    ) -> SendMessageAction:
        """Build a :class:`SendMessageAction`."""
        return SendMessageAction(
            recipient_id=recipient_id,
            text=text,
            reply_to_message_id=reply_to_message_id,
            related_request_id=related_request_id,
            buttons=buttons,
        )

    @staticmethod
    def _callback_int(data: str, prefix: str) -> int | None:
        """Parse ``prefix:<int>`` callback data, rejecting anything else."""
        if not data.startswith(prefix + ":"):
            return None
        try:
            return int(data.split(":", 1)[1])
        except ValueError:
            return None

    @staticmethod
    def _back_row() -> tuple[InlineButton, ...]:
        """The universal return-to-menu row."""
        return (InlineButton("🏠 منوی اصلی", "menu:main"),)

    def _main_buttons(self, actor: User) -> tuple[tuple[InlineButton, ...], ...]:
        """The primary inline menu shown under welcome/help/status messages."""
        rows: list[tuple[InlineButton, ...]] = [
            (
                InlineButton("📊 وضعیت", "menu:status"),
                InlineButton("🔗 جفت‌سازی", "menu:pair"),
            ),
            (
                InlineButton("📥 درخواست‌ها", "menu:pending"),
                InlineButton("👤 پروفایل", "menu:whoami"),
            ),
            (InlineButton("❓ راهنما", "menu:help"),),
        ]
        if actor.is_owner:
            rows.append((InlineButton("🛠 مدیریت", "menu:owner"),))
        return tuple(rows)

    def _owner_buttons(self, actor: User) -> tuple[tuple[InlineButton, ...], ...]:
        """Owner control panel."""
        return (
            (
                InlineButton("👥 کاربران", "menu:users"),
                InlineButton("📊 وضعیت", "menu:status"),
            ),
            (
                InlineButton("🚫 مسدود کردن", "menu:ban"),
                InlineButton("✅ رفع مسدودی", "menu:unban"),
            ),
            self._back_row(),
        )

    def _pair_prompt(self, actor: User) -> SendMessageAction:
        """Explain the only step a button cannot do: entering the partner id."""
        return self._send(
            actor.id,
            "🔗 جفت‌سازی\n\n"
            "۱) طرف مقابل با /start وارد ربات شود.\n"
            "۲) او با /whoami شناسهٔ کوتاه و تصادفی‌اش را ببیند.\n"
            "۳) شما دستور زیر را بفرستید:\n"
            "/pair <شناسه طرف مقابل>\n\n"
            "پس از ارسال، طرف مقابل می‌تواند با دکمه‌های زیر پیام، درخواست را بپذیرد.",
            buttons=(
                (
                    InlineButton("📥 درخواست‌های من", "menu:pending"),
                    InlineButton("🏠 منوی اصلی", "menu:main"),
                ),
            ),
        )

    def _owner_menu(self, actor: User) -> SendMessageAction:
        """Owner control panel or a permission error."""
        if not actor.is_owner:
            return self._send(
                actor.id,
                str(PermissionDeniedError()),
                buttons=self._main_buttons(actor),
            )
        return self._send(
            actor.id,
            "🛠 پنل مدیریت\n\nیکی از گزینه‌های زیر را انتخاب کنید.",
            buttons=self._owner_buttons(actor),
        )

    def _owner_prompt(self, actor: User, action: str) -> SendMessageAction:
        """Prompt for the id required by ban/unban (a button cannot type it)."""
        if not actor.is_owner:
            return self._send(
                actor.id,
                str(PermissionDeniedError()),
                buttons=self._main_buttons(actor),
            )
        verb = "مسدود کردن" if action == "ban" else "رفع مسدودی"
        command = "/ban" if action == "ban" else "/unban"
        return self._send(
            actor.id,
            f"{'🚫' if action == 'ban' else '✅'} {verb}\n\n"
            f"دستور را با شناسهٔ کاربر بفرستید:\n"
            f"{command} <شناسه کاربر>\n\n"
            "برای دیدن شناسه‌ها از «👥 کاربران» استفاده کنید.",
            buttons=(
                (
                    InlineButton("👥 کاربران", "menu:users"),
                    InlineButton("🛠 پنل مدیریت", "menu:owner"),
                ),
            ),
        )

    def _decorate(
        self, actor: User, actions: tuple[Action, ...]
    ) -> tuple[Action, ...]:
        """Give actor-facing messages a menu when a handler forgot one."""
        decorated: list[Action] = []
        for action in actions:
            if (
                isinstance(action, SendMessageAction)
                and action.recipient_id == actor.id
                and not action.buttons
            ):
                action = SendMessageAction(
                    recipient_id=action.recipient_id,
                    text=action.text,
                    reply_to_message_id=action.reply_to_message_id,
                    related_request_id=action.related_request_id,
                    buttons=self._main_buttons(actor),
                )
            decorated.append(action)
        return tuple(decorated)

    def _handlers(self) -> dict[str, HandlerFn]:
        """Command name → handler mapping (rebuilt per call to stay stateless)."""
        return {
            "start": self._cmd_start,
            "help": self._cmd_help,
            "whoami": self._cmd_whoami,
            "status": self._cmd_status,
            "pair": self._cmd_pair,
            "pending": self._cmd_pending,
            "requests": self._cmd_pending,
            "users": self._cmd_users,
            "accept": self._cmd_accept,
            "reject": self._cmd_reject,
            "cancel": self._cmd_cancel,
            "unpair": self._cmd_unpair,
            "ban": self._cmd_ban,
            "unban": self._cmd_unban,
        }

    def _user_label(self, user_id: int) -> str:
        """Public id for display, falling back to an internal id for legacy rows."""
        user = self._users.get(user_id)
        return user.public_id if user is not None and user.public_id else str(user_id)

    def _describe_pair(self, pair: Pair, viewer_id: int) -> str:
        """Human-readable pair description from ``viewer_id``'s perspective."""
        partner_id = pair.other(viewer_id)
        return f"جفت شماره {pair.id} با کاربر {self._user_label(partner_id)}"

    def _describe_request(self, request: PairRequest) -> str:
        """One-line description of a pending request."""
        return (
            f"درخواست {request.id}: از کاربر {self._user_label(request.requester_id)} "
            f"(مهلت تا {request.expires_at:%Y-%m-%d %H:%M} UTC)"
        )

    # --------------------------------------------------------------- handlers

    def _cmd_start(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/start``: greet and show the actor's internal id."""
        return (
            self._send(
                actor.id,
                "🌟 به پل بله ↔ تلگرام خوش آمدید\n\n"
                f"شناسهٔ شما: {actor.public_id}\n\n"
                "با گزینه‌های زیر شروع کنید. برای اتصال به طرف مقابل، "
                "ابتدا هر دو نفر /start بزنید و سپس از «🔗 جفت‌سازی» استفاده کنید.\n"
                "اگر دکمه‌ها را نمی‌بینید: /pair <شناسه طرف مقابل>",
                buttons=self._main_buttons(actor),
            ),
        )

    def _cmd_help(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/help``."""
        text = self.HELP_TEXT
        if actor.is_owner:
            text = f"{text}\n\n👑 دستورهای مالک:\n{self.OWNER_HELP_TEXT}"
        return (self._send(actor.id, text, buttons=self._main_buttons(actor)),)

    def _cmd_whoami(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/whoami``."""
        return (
            self._send(
                actor.id,
                "👤 پروفایل شما\n\n"
                f"شناسه: {actor.public_id}\n"
                f"پلتفرم: {actor.platform.value}\n"
                f"نقش: {actor.role.value}\n"
                f"وضعیت: {actor.status.value}",
                buttons=self._main_buttons(actor),
            ),
        )

    def _overview_lines(self) -> list[str]:
        """Owner-facing aggregate counters (counts only, never content)."""
        stats = self._users.stats()
        overview = self._pairing.overview()
        lines = [
            "وضعیت کلی پل:",
            f"  کاربران: {stats.total} (فعال {stats.active}، مسدود {stats.banned}، مالک {stats.owners})",
            f"  کاربران فعال: {stats.active}",
            f"  جفت‌های فعال: {overview.active_pairs}",
            f"  درخواست‌های در انتظار: {overview.pending_requests}",
        ]
        if self._stats is not None:
            outbox = self._stats(self._clock())
            last_relay = (
                outbox.last_relay_at.strftime("%Y-%m-%d %H:%M UTC")
                if outbox.last_relay_at is not None
                else "هنوز رله‌ای ارسال نشده"
            )
            lines.extend(
                [
                    f"  صف ارسال: {outbox.queue_length} "
                    f"(در انتظار {outbox.pending}، در حال ارسال {outbox.sending}، "
                    f"ناموفق {outbox.failed})",
                    f"  خطاهای ۲۴ ساعت گذشته: {outbox.errors_24h}",
                    f"  آخرین رله: {last_relay}",
                ]
            )
        return lines

    def _cmd_status(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/status``: personal state, plus bridge-wide stats for owners."""
        pairs = self._pairing.active_pairs(actor.id)
        incoming = self._pairing.incoming_requests(actor.id)
        outgoing = self._pairing.outgoing_requests(actor.id)

        lines: list[str] = []
        if actor.is_owner:
            lines.extend(self._overview_lines())
            lines.append("")
        lines.extend(
            [
                f"وضعیت: {actor.status.value}",
                f"نقش: {actor.role.value}",
            ]
        )
        if pairs:
            lines.append("جفت‌های فعال:")
            lines.extend(f"  - {self._describe_pair(pair, actor.id)}" for pair in pairs)
        else:
            lines.append("جفت فعالی ندارید.")
        if incoming:
            lines.append("درخواست‌های دریافتی:")
            lines.extend(f"  - {self._describe_request(req)}" for req in incoming)
        if outgoing:
            lines.append("درخواست‌های ارسالی:")
            lines.extend(f"  - {self._describe_request(req)}" for req in outgoing)

        rows: list[tuple[InlineButton, ...]] = [
            (InlineButton(f"🔓 پایان جفت {pair.id}", f"unpair:{pair.id}"),)
            for pair in pairs
        ]
        rows.extend(
            (
                InlineButton(
                    f"🗑 لغو درخواست {req.id}", f"cancel:{req.id}"
                ),
            )
            for req in outgoing
        )
        if incoming:
            rows.append((InlineButton("📥 پاسخ به درخواست‌ها", "menu:pending"),))
        rows.append(self._back_row())
        return (self._send(actor.id, "\n".join(lines), buttons=tuple(rows)),)

    def _cmd_pair(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/pair <public_id>``: create a pending request."""
        public_id = command.public_id_arg(0, usage="/pair <شناسه کاربر>")
        target = self._users.get_by_public_id(public_id)
        if target is None:
            raise UserNotFoundError()
        request = self._pairing.request(actor.id, target.id)
        return (
            self._send(
                actor.id,
                f"✅ درخواست جفت‌سازی برای کاربر {target.public_id} ارسال شد.\n"
                f"شناسهٔ درخواست: {request.id}\n\n"
                "می‌توانید تا زمان پاسخ، آن را لغو کنید.",
                buttons=(
                    (
                        InlineButton("🗑 لغو درخواست", f"cancel:{request.id}"),
                        InlineButton("📥 درخواست‌های من", "menu:pending"),
                    ),
                    (InlineButton("🏠 منوی اصلی", "menu:main"),),
                ),
            ),
            self._send(
                target.id,
                f"🔔 درخواست جفت‌سازی از کاربر {actor.public_id} دریافت کردید.\n"
                f"شناسهٔ درخواست: {request.id}\n\n"
                f"برای پذیرش: /accept {request.id}\n"
                f"برای رد: /reject {request.id}\n\n"
                "یا از دکمه‌های زیر استفاده کنید:",
                related_request_id=request.id,
                buttons=(
                    (
                        InlineButton("✅ پذیرش", f"accept:{request.id}"),
                        InlineButton("❌ رد", f"reject:{request.id}"),
                    ),
                ),
            ),
        )

    def _cmd_pending(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/pending``: list incoming requests."""
        incoming = self._pairing.incoming_requests(actor.id)
        if not incoming:
            return (
                self._send(
                    actor.id,
                    "📭 درخواست دریافتی فعالی ندارید.",
                    buttons=self._main_buttons(actor),
                ),
            )
        lines = ["📥 درخواست‌های دریافتی:"]
        lines.extend(f"  - {self._describe_request(req)}" for req in incoming)
        rows: list[tuple[InlineButton, ...]] = [
            (
                InlineButton("✅ پذیرش", f"accept:{req.id}"),
                InlineButton("❌ رد", f"reject:{req.id}"),
            )
            for req in incoming
        ]
        rows.append(self._back_row())
        return (self._send(actor.id, "\n".join(lines), buttons=tuple(rows)),)

    def _cmd_accept(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/accept <request_id>``: accept and activate the pair."""
        request_id = command.int_arg(0, usage="/accept <شناسه درخواست>")
        pair = self._pairing.accept(request_id, actor.id)
        return (
            self._send(
                actor.id,
                f"🎉 {self._describe_pair(pair, actor.id)} فعال شد. "
                "از این پس پیام‌های شما به هم منتقل می‌شود.",
                buttons=(
                    (
                        InlineButton("📊 وضعیت", "menu:status"),
                        InlineButton("🏠 منوی اصلی", "menu:main"),
                    ),
                ),
            ),
            self._send(
                pair.other(actor.id),
                f"🎉 درخواست جفت‌سازی شما پذیرفته شد و "
                f"{self._describe_pair(pair, pair.other(actor.id))} فعال شد.",
                buttons=(
                    (
                        InlineButton("📊 وضعیت", "menu:status"),
                        InlineButton("🏠 منوی اصلی", "menu:main"),
                    ),
                ),
            ),
        )

    def _cmd_reject(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/reject <request_id>``."""
        request_id = command.int_arg(0, usage="/reject <شناسه درخواست>")
        request = self._pairing.reject(request_id, actor.id)
        return (
            self._send(
                actor.id,
                f"❌ درخواست {request.id} رد شد.",
                buttons=self._main_buttons(actor),
            ),
            self._send(
                request.requester_id,
                f"❌ درخواست جفت‌سازی شما (شماره {request.id}) رد شد.",
            ),
        )

    def _cmd_cancel(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/cancel <request_id>``."""
        request_id = command.int_arg(0, usage="/cancel <شناسه درخواست>")
        request = self._pairing.cancel(request_id, actor.id)
        return (
            self._send(
                actor.id,
                f"🗑 درخواست {request.id} لغو شد.",
                buttons=(
                    (
                        InlineButton("📥 درخواست‌ها", "menu:pending"),
                        InlineButton("🏠 منوی اصلی", "menu:main"),
                    ),
                ),
            ),
            self._send(
                request.target_id,
                f"🔕 درخواست جفت‌سازی کاربر {actor.public_id} لغو شد.",
            ),
        )

    def _cmd_unpair(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/unpair [pair_id]``: end the actor's active pair."""
        if command.args:
            pair_id = command.int_arg(0, usage="/unpair [شناسه جفت]")
            pair = self._pairing.active_pair_for_member(pair_id, actor.id)
        else:
            active = self._pairing.active_pair(actor.id)
            if active is None:
                raise PairNotFoundError("جفت فعالی برای پایان دادن ندارید.")
            pair = self._pairing.active_pair_for_member(active.id, actor.id)

        ended = self._pairing.unpair(pair.id, actor.id)
        return (
            self._send(
                actor.id,
                f"🔓 {self._describe_pair(ended, actor.id)} پایان یافت.",
                buttons=self._main_buttons(actor),
            ),
            self._send(
                ended.other(actor.id),
                f"🔔 کاربر {actor.public_id} جفت {ended.id} را پایان داد. "
                "پیام‌های شما دیگر منتقل نمی‌شود.",
            ),
        )

    def _cmd_users(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/users [count]`` (owner only): list the newest users."""
        self._users.require_owner(actor)
        if command.args:
            limit = command.int_arg(0, usage="/users [تعداد]")
            if limit < 1 or limit > self.USER_LIST_LIMIT:
                raise InvalidArgumentError(
                    f"تعداد باید عددی بین ۱ و {self.USER_LIST_LIMIT} باشد."
                )
        else:
            limit = self.USER_LIST_LIMIT

        users = self._users.list_users(limit)
        total = self._users.stats().total
        lines = [f"👥 کاربران (آخرین {len(users)} از {total}):"]
        lines.extend(
            f"  {user.public_id} | {user.platform.value} | {user.role.value} | {user.status.value}"
            for user in users
        )
        rows: list[tuple[InlineButton, ...]] = []
        for user in users:
            if user.is_banned:
                rows.append(
                    (
                        InlineButton(
                            f"✅ رفع مسدودی {user.public_id}",
                            f"unban:{user.public_id}",
                        ),
                    )
                )
            else:
                rows.append(
                    (
                        InlineButton(
                            f"🚫 مسدود کردن {user.public_id}",
                            f"ban:{user.public_id}",
                        ),
                    )
                )
        rows.append(
            (
                InlineButton("🛠 پنل مدیریت", "menu:owner"),
                InlineButton("🏠 منوی اصلی", "menu:main"),
            )
        )
        return (self._send(actor.id, "\n".join(lines), buttons=tuple(rows)),)

    def _cmd_ban(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/ban <public_id>`` (owner only)."""
        public_id = command.public_id_arg(0, usage="/ban <شناسه کاربر>")
        target = self._users.get_by_public_id(public_id)
        if target is None:
            raise UserNotFoundError()
        self._users.ban(actor, target.id)
        return (
            self._send(
                actor.id,
                f"🚫 کاربر {target.public_id} مسدود شد.",
                buttons=self._owner_buttons(actor),
            ),
        )

    def _cmd_unban(self, actor: User, command: Command) -> tuple[Action, ...]:
        """Handle ``/unban <public_id>`` (owner only)."""
        public_id = command.public_id_arg(0, usage="/unban <شناسه کاربر>")
        target = self._users.get_by_public_id(public_id)
        if target is None:
            raise UserNotFoundError()
        self._users.unban(actor, target.id)
        return (
            self._send(
                actor.id,
                f"✅ مسدودی کاربر {target.public_id} برداشته شد.",
                buttons=self._owner_buttons(actor),
            ),
        )


__all__ = [
    "Action",
    "Command",
    "CommandRouter",
    "EditMessageAction",
    "SendMessageAction",
    "parse_command",
]
