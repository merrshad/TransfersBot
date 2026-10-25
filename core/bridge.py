"""Bridge between the messenger adapters and the domain services.

Responsibilities of this layer:

* **Registration** — every inbound message maps its platform identity to a row
  in ``users`` and refreshes the chat id used for system replies.
* **Command routing** — text is parsed by :mod:`core.commands`; the resulting
  actions are delivered to the right platform.
* **Message relaying** — plain text from an active member of a pair is recorded
  in ``messages`` and queued in ``outbox`` for the delivery loop; the actual
  send happens in :class:`core.delivery.OutboxWorker`, never inline.
* **Delivery failure handling** — when a pairing-request notification cannot be
  delivered (target never started the bot, blocked it, …), the request is
  cancelled and both the requester and the owner are informed.

Relay guarantees:

* only the sender's own active pair is ever consulted — messages never cross
  pairs, and a relay is always addressed to the partner, never back to the
  sender;
* an inbound message whose (platform, chat, message id) already exists in
  ``messages`` is skipped (loop/idempotency guard, backed by the table's unique
  index);
* reply threading is preserved in both directions via the stored
  ``target_message_id`` mapping.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta

from core.commands import (
    Action,
    Command,
    CommandRouter,
    EditMessageAction,
    SendMessageAction,
    parse_command,
)
from core.delivery import (
    GROUP_MEDIA_PAYLOAD_KIND,
    GROUP_TEXT_PAYLOAD_KIND,
    RELAY_PAYLOAD_KIND,
)
from core.errors import DomainError, DuplicateMessageError, MessagingError, UserBannedError
from core.formatting import render_relay_text
from core.groups import (
    GroupAlreadyLinkedError,
    GroupInviteInvalidError,
    GroupLink,
    GroupLinkInviteRepositoryPort,
    GroupLinkNotFoundError,
    GroupLinkRepositoryPort,
    GroupMessageRepositoryPort,
    GroupOutboxRepositoryPort,
    GroupOwnerOnlyError,
    KnownGroupRepositoryPort,
)
from core.ids import new_group_token
from core.media import RELAY_MEDIA_KIND, media_item_payload, unsupported_placeholder
from core.models import InlineButton, MessageRef, Platform, SentMessage, UnifiedMessage
from core.pairing import Pair, PairingService
from core.ports import (
    MessageRepositoryPort,
    MessageSenderPort,
    OutboxRepositoryPort,
    TransactionManager,
)
from core.users import User, UserRole, UserService


class Bridge:
    """Turns inbound messages into commands, relays, deliveries and notifications.

    Args:
        users: User service (registration, lookup, chat memory).
        pairing: Pairing service (rules and state machine).
        router: Command router producing :class:`~core.commands.Action` objects.
        messages: Message repository (relay records and id mappings).
        outbox: Outbox repository (durable delivery queue).
        tx: Transaction manager sharing the connection with the repositories.
        senders: One :class:`~core.ports.MessageSenderPort` per platform; in
            ``main`` these are the adapters themselves.
        owner_platform_ids: Owner's platform identity per messenger, used for
            operational warnings.
        clock: Injectable UTC clock used for relay bookkeeping timestamps.
        logger: Logger override.
    """

    NO_ACTIVE_PAIR_TEXT = (
        "شما هنوز جفت فعالی ندارید.\n"
        "برای ارسال درخواست جفت‌سازی: /pair <شناسه کاربر>\n"
        "راهنمای کامل: /help"
    )

    BANNED_TEXT = UserBannedError().message

    def __init__(
        self,
        *,
        users: UserService,
        pairing: PairingService,
        router: CommandRouter,
        messages: MessageRepositoryPort,
        outbox: OutboxRepositoryPort,
        tx: TransactionManager,
        senders: Mapping[Platform, MessageSenderPort],
        owner_platform_ids: Mapping[Platform, int],
        group_links: GroupLinkRepositoryPort | None = None,
        group_messages: GroupMessageRepositoryPort | None = None,
        group_outbox: GroupOutboxRepositoryPort | None = None,
        group_invites: GroupLinkInviteRepositoryPort | None = None,
        known_groups: KnownGroupRepositoryPort | None = None,
        group_invite_ttl: timedelta = timedelta(minutes=15),
        clock: Callable[[], datetime] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._users = users
        self._pairing = pairing
        self._router = router
        self._messages = messages
        self._outbox = outbox
        self._tx = tx
        self._senders = dict(senders)
        self._owner_platform_ids = dict(owner_platform_ids)
        self._group_links = group_links
        self._group_messages = group_messages
        self._group_outbox = group_outbox
        self._group_invites = group_invites
        self._known_groups = known_groups
        self._group_invite_ttl = group_invite_ttl
        self._clock = clock or (lambda: datetime.now(UTC))
        self._log = logger or logging.getLogger("bridge.core")

    # ------------------------------------------------------------- entrypoint

    async def handle_message(self, message: UnifiedMessage) -> None:
        """Process one inbound message end to end."""
        await self.handle_messages((message,))

    async def handle_messages(self, messages: Sequence[UnifiedMessage]) -> None:
        """Process one message, or the members of one album, end to end.

        Album members arrive as separate updates sharing a ``media_group_id``;
        they are registered and relayed together so the pair receives the album
        (or, when the target has no albums, the same order as the sender).
        """
        if not messages:
            return
        leader = messages[0]
        if leader.is_group:
            await self._handle_group_messages(messages)
            return
        user = await self._register(leader)
        if user is None:
            if leader.callback_id is not None:
                await self._answer_callback(leader)
            return

        if leader.callback_data is not None:
            # Button presses are command-like: never relayed to the partner.
            self._log.info(
                "callback=%s platform=%s user_id=%s",
                leader.callback_data.split(":", 1)[0],
                user.platform.value,
                user.id,
            )
            try:
                actions = self._router.handle_callback(user.id, leader.callback_data)
                if actions:
                    await self._deliver_actions(user, actions, callback=leader)
            finally:
                await self._answer_callback(leader)
            return

        batch = self._same_album_members(leader, messages)
        # Commands are only recognised in a body-only message: a caption that
        # happens to start with '/' is still just a caption.
        if not leader.media and len(batch) == 1 and leader.text:
            command = parse_command(leader.text)
            if command is not None:
                # Only the command name is logged at INFO; arguments and message
                # bodies never are.
                self._log.info(
                    "command=%s platform=%s user_id=%s",
                    command.name,
                    user.platform.value,
                    user.id,
                )
                actions = self._router.handle(user.id, leader.text)
                if actions:
                    await self._deliver_actions(user, actions)
                return

        relayable = tuple(message for message in batch if message.is_relayable)
        if not relayable:
            return
        await self._handle_content(user, relayable)

    async def _handle_group_messages(
        self, messages: Sequence[UnifiedMessage]
    ) -> None:
        """Relay one group message/album through the group link queue."""
        if (
            self._group_links is None
            or self._group_messages is None
            or self._group_outbox is None
            or self._group_invites is None
        ):
            return

        leader = messages[0]
        if leader.callback_data is not None:
            if self._group_links is not None and self._known_groups is not None:
                self._record_known_group(leader)
                try:
                    await self._handle_group_callback(leader)
                except DomainError as exc:
                    await self._send_group_notice(leader, exc.message)
                except Exception as exc:  # noqa: BLE001 - never poison polling
                    self._log.error(
                        "group callback failed platform=%s chat_id=%s (%s)",
                        leader.source.value,
                        leader.chat_id,
                        type(exc).__name__,
                    )
                finally:
                    await self._answer_callback(leader)
            return
        if leader.is_bot_sender:
            return
        self._record_known_group(leader)
        if (
            not leader.media
            and leader.text
            and leader.text.lstrip().startswith("/")
        ):
            command = parse_command(leader.text)
            if command is not None and command.name == "groupsync":
                try:
                    await self._handle_groupsync_command(leader, command)
                except DomainError as exc:
                    await self._send_group_notice(leader, exc.message)
                except Exception as exc:  # noqa: BLE001 - never poison polling
                    self._log.error(
                        "group command failed platform=%s chat_id=%s (%s)",
                        leader.source.value,
                        leader.chat_id,
                        type(exc).__name__,
                    )
                    await self._send_group_notice(
                        leader,
                        "انجام دستور ناموفق بود. دوباره تلاش کنید.",
                    )
            # Group commands are setup/administration, never relayed.
            return

        link = self._group_links.get_active_for_source(
            leader.source, leader.chat_id
        )
        if link is None:
            return

        batch = tuple(
            message
            for message in self._same_album_members(leader, messages)
            if not message.is_bot_sender and message.is_relayable
        )
        if not batch:
            return

        fresh = tuple(
            message
            for message in batch
            if self._group_messages.find_by_source(
                message.source, message.chat_id, message.message_id
            )
            is None
        )
        if not fresh:
            self._log.info(
                "group relay skipped reason=duplicate link_id=%s chat_id=%s",
                link.id,
                leader.chat_id,
            )
            return

        target_platform, target_chat_id = link.target_for(leader.source)
        album_id = self._group_album_id(fresh)
        now = self._clock()
        queued = 0
        try:
            with self._tx.transaction():
                pending: list[tuple[UnifiedMessage, int]] = []
                for message in fresh:
                    record, created = self._group_messages.add(
                        group_link_id=link.id,
                        source_platform=message.source,
                        source_chat_id=message.chat_id,
                        source_message_id=message.message_id,
                        created_at=message.timestamp,
                    )
                    if created:
                        pending.append((message, record.id))
                for index, (message, record_id) in enumerate(pending):
                    self._group_outbox.enqueue(
                        group_link_id=link.id,
                        payload=self._group_payload(
                            link_id=link.id,
                            message=message,
                            record_id=record_id,
                            target_platform=target_platform,
                            target_chat_id=target_chat_id,
                            album_id=album_id,
                            album_index=index,
                            album_size=len(pending),
                        ),
                        next_attempt_at=now,
                        created_at=now,
                    )
                    queued += 1
        except Exception as exc:  # noqa: BLE001 - a poison group message must not kill polling
            self._log.error(
                "group relay failed reason=%s link_id=%s chat_id=%s",
                type(exc).__name__,
                link.id,
                leader.chat_id,
            )
            return

        if queued:
            self._log.info(
                "group relay queued link_id=%s from_chat=%s to_chat=%s messages=%s album=%s",
                link.id,
                leader.chat_id,
                target_chat_id,
                queued,
                album_id is not None,
            )

    @staticmethod
    def _group_platform_name(platform: Platform) -> str:
        return "تلگرام" if platform is Platform.TELEGRAM else "بله"

    @staticmethod
    def _groupsync_help_text() -> str:
        return (
            "🔗 مدیریت سینک گروه‌ها (فقط مالک)\n\n"
            "/groupsync\n"
            "باز کردن منوی دکمه‌ای سینک گروه.\n\n"
            "با دکمهٔ «اتصال به گروه»، گروه مقصد را از فهرست گروه‌های "
            "شناخته‌شدهٔ پلتفرم مقابل انتخاب کنید.\n\n"
            "اگر گروه مقصد در فهرست نبود، یک بار در آن گروه /groupsync "
            "بفرستید تا ربات آن را بشناسد.\n\n"
            "دکمه‌های «جفت‌ها» و «قطع سینک» وضعیت را نشان می‌دهند و "
            "لینک را قطع می‌کنند."
        )

    async def _send_group_notice(
        self,
        message: UnifiedMessage,
        text: str,
        *,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> None:
        """Reply in the same group without going through the outbox."""
        sender = self._senders.get(message.source)
        if sender is None:
            self._log.warning(
                "group command reply skipped platform=%s (no sender)",
                message.source.value,
            )
            return
        try:
            await sender.send_message(message.chat_id, text, buttons=buttons)
        except MessagingError as exc:
            self._log.warning(
                "group command reply failed platform=%s chat_id=%s (%s)",
                message.source.value,
                message.chat_id,
                type(exc).__name__,
            )

    async def _edit_group_notice(
        self,
        callback: UnifiedMessage,
        text: str,
        *,
        buttons: Sequence[Sequence[InlineButton]] = (),
    ) -> None:
        """Edit the menu message that a button belongs to, with fallback."""
        sender = self._senders.get(callback.source)
        if sender is None:
            return
        if callback.message_id is not None:
            try:
                await sender.edit_message_text(
                    callback.chat_id,
                    callback.message_id,
                    text,
                    buttons=buttons,
                )
                return
            except MessagingError as exc:
                self._log.warning(
                    "group menu edit failed platform=%s chat_id=%s (%s)",
                    callback.source.value,
                    callback.chat_id,
                    type(exc).__name__,
                )
        await self._send_group_notice(callback, text, buttons=buttons)

    def _record_known_group(self, message: UnifiedMessage) -> None:
        """Remember every group the bot sees so the owner can pick it later."""
        if self._known_groups is None:
            return
        try:
            self._known_groups.upsert(
                platform=message.source,
                chat_id=message.chat_id,
                title=message.chat_title,
                discovered_by=message.sender_id,
                now=self._clock(),
            )
        except Exception as exc:  # noqa: BLE001 - metadata must not stop relaying
            self._log.warning(
                "known group upsert failed platform=%s chat_id=%s (%s)",
                message.source.value,
                message.chat_id,
                type(exc).__name__,
            )

    def _known_group_name(self, platform: Platform, chat_id: int) -> str:
        group = (
            self._known_groups.get(platform, chat_id)
            if self._known_groups is not None
            else None
        )
        return self._group_sender_label(
            group.display_name if group is not None else str(chat_id)
        )

    @staticmethod
    def _opposite_platform(source: Platform) -> Platform:
        return Platform.BALE if source is Platform.TELEGRAM else Platform.TELEGRAM

    def _group_menu_text(
        self, message: UnifiedMessage, *, active: GroupLink | None
    ) -> str:
        name = self._known_group_name(message.source, message.chat_id)
        if active is None:
            status = "وضعیت: هنوز به گروهی وصل نیست."
        else:
            target_platform = self._opposite_platform(message.source)
            target_id = (
                active.bale_chat_id
                if target_platform is Platform.BALE
                else active.telegram_chat_id
            )
            status = (
                f"وضعیت: وصل به {self._known_group_name(target_platform, target_id)}"
            )
        return (
            "🔗 سینک گروه\n"
            f"گروه: {name}\n"
            f"{status}\n\n"
            "یکی از گزینه‌ها را انتخاب کنید:"
        )

    def _group_menu_buttons(
        self, *, active: bool
    ) -> tuple[tuple[InlineButton, ...], ...]:
        rows: list[tuple[InlineButton, ...]] = []
        if not active:
            rows.append((InlineButton("🔗 اتصال به گروه", "groupsync:pick"),))
        rows.append(
            (
                InlineButton("📋 جفت‌ها", "groupsync:list"),
                InlineButton("❓ راهنما", "groupsync:help"),
            )
        )
        if active:
            rows.append((InlineButton("✂️ قطع سینک", "groupsync:off"),))
        return tuple(rows)

    async def _show_group_menu(
        self, message: UnifiedMessage, *, edit: bool = False
    ) -> None:
        if self._group_links is None:
            return
        active = self._group_links.get_active_for_source(
            message.source, message.chat_id
        )
        text = self._group_menu_text(message, active=active)
        buttons = self._group_menu_buttons(active=active is not None)
        if edit:
            await self._edit_group_notice(message, text, buttons=buttons)
        else:
            await self._send_group_notice(message, text, buttons=buttons)

    async def _show_group_targets(self, callback: UnifiedMessage) -> None:
        if self._group_links is None or self._known_groups is None:
            return
        active = self._group_links.get_active_for_source(
            callback.source, callback.chat_id
        )
        if active is not None:
            await self._edit_group_notice(
                callback,
                "این گروه قبلاً وصل است. اول با «قطع سینک» آن را جدا کنید.",
                buttons=(
                    (
                        InlineButton("✂️ قطع سینک", "groupsync:off"),
                        InlineButton("🏠 منو", "groupsync:menu"),
                    ),
                ),
            )
            return
        target_platform = self._opposite_platform(callback.source)
        targets = [
            group
            for group in self._known_groups.list_by_platform(target_platform)
            if self._group_links.get_active_for_source(
                target_platform, group.chat_id
            )
            is None
        ]
        if not targets:
            await self._edit_group_notice(
                callback,
                f"هنوز گروهی از {self._group_platform_name(target_platform)} "
                "ثبت نشده است.\n"
                f"ربات را در گروه {self._group_platform_name(target_platform)} "
                "عضو/ادمین کنید و یک پیام یا /groupsync در آن گروه بفرستید؛ "
                "سپس دوباره تلاش کنید.",
                buttons=(
                    (
                        InlineButton("🔄 دوباره تلاش", "groupsync:pick"),
                        InlineButton("🏠 منو", "groupsync:menu"),
                    ),
                ),
            )
            return
        labels = [self._known_group_name(target_platform, group.chat_id) for group in targets]
        rows: list[tuple[InlineButton, ...]] = []
        for index, group in enumerate(targets[:8]):
            label = labels[index][:38]
            if labels.count(labels[index]) > 1:
                label = f"{label} ({group.chat_id})"
            rows.append(
                (
                    InlineButton(
                        label,
                        f"groupsync:link:{group.chat_id}",
                    ),
                )
            )
        rows.append((InlineButton("🏠 منو", "groupsync:menu"),))
        await self._edit_group_notice(
            callback,
            f"گروه {self._group_platform_name(target_platform)} مقصد را انتخاب کنید:",
            buttons=tuple(rows),
        )

    async def _link_group_by_button(
        self, callback: UnifiedMessage, target_chat_id: int
    ) -> None:
        if self._group_links is None or self._known_groups is None:
            return
        target_platform = self._opposite_platform(callback.source)
        target = self._known_groups.get(target_platform, target_chat_id)
        if target is None:
            await self._edit_group_notice(
                callback,
                "گروه مقصد پیدا نشد. یک پیام در آن گروه بفرستید و دوباره تلاش کنید.",
                buttons=((InlineButton("🏠 منو", "groupsync:menu"),),),
            )
            return
        if self._group_links.get_active_for_source(
            callback.source, callback.chat_id
        ) is not None or self._group_links.get_active_for_source(
            target_platform, target_chat_id
        ) is not None:
            await self._edit_group_notice(
                callback,
                GroupAlreadyLinkedError().message,
                buttons=((InlineButton("🏠 منو", "groupsync:menu"),),),
            )
            return
        if callback.source is Platform.TELEGRAM:
            telegram_chat_id = callback.chat_id
            bale_chat_id = target_chat_id
        else:
            telegram_chat_id = target_chat_id
            bale_chat_id = callback.chat_id
        try:
            link = self._group_links.link(
                telegram_chat_id=telegram_chat_id,
                bale_chat_id=bale_chat_id,
                now=self._clock(),
            )
        except GroupAlreadyLinkedError:
            await self._edit_group_notice(
                callback,
                GroupAlreadyLinkedError().message,
                buttons=((InlineButton("🏠 منو", "groupsync:menu"),),),
            )
            return
        await self._edit_group_notice(
            callback,
            "✅ سینک گروه‌ها فعال شد.\n"
            f"تلگرام: {link.telegram_chat_id}\n"
            f"بله: {link.bale_chat_id}",
            buttons=(
                (InlineButton("📋 جفت‌ها", "groupsync:list"),),
                (InlineButton("✂️ قطع سینک", "groupsync:off"),),
                (InlineButton("🏠 منو", "groupsync:menu"),),
            ),
        )
        self._log.info(
            "group link created link_id=%s telegram_chat=%s bale_chat=%s",
            link.id,
            link.telegram_chat_id,
            link.bale_chat_id,
        )

    async def _show_group_links(
        self, message: UnifiedMessage, *, edit: bool = False
    ) -> None:
        if self._group_links is None:
            return
        links = self._group_links.list_active()
        if not links:
            text = "هیچ جفت‌گروه فعالی وجود ندارد."
        else:
            lines = ["🔗 جفت‌گروه‌های فعال:"]
            for index, link in enumerate(links, start=1):
                lines.append(
                    f"{index}) {self._known_group_name(Platform.TELEGRAM, link.telegram_chat_id)}"
                    f" ↔ {self._known_group_name(Platform.BALE, link.bale_chat_id)}"
                )
            text = "\n".join(lines)
        buttons: tuple[tuple[InlineButton, ...], ...] = (
            ((InlineButton("🏠 منو", "groupsync:menu"),),) if edit else ()
        )
        if edit:
            await self._edit_group_notice(message, text, buttons=buttons)
        else:
            await self._send_group_notice(message, text)

    async def _disconnect_group_by_button(self, callback: UnifiedMessage) -> None:
        if self._group_links is None or self._group_outbox is None:
            return
        link = self._group_links.get_active_for_source(
            callback.source, callback.chat_id
        )
        if link is None:
            await self._edit_group_notice(
                callback,
                GroupLinkNotFoundError().message,
                buttons=((InlineButton("🏠 منو", "groupsync:menu"),),),
            )
            return
        self._group_links.deactivate(link.id, now=self._clock())
        removed = self._group_outbox.cancel_pending(link.id)
        await self._edit_group_notice(
            callback,
            "✅ سینک این گروه قطع شد.",
            buttons=((InlineButton("🏠 منو", "groupsync:menu"),),),
        )
        self._log.info(
            "group link disconnected link_id=%s chat_id=%s cancelled=%s",
            link.id,
            callback.chat_id,
            removed,
        )

    async def _show_group_help(self, callback: UnifiedMessage) -> None:
        await self._edit_group_notice(
            callback,
            self._groupsync_help_text(),
            buttons=((InlineButton("🏠 منو", "groupsync:menu"),),),
        )

    async def _handle_group_callback(self, callback: UnifiedMessage) -> None:
        """Handle the group pairing inline keyboard (owner only)."""
        owner_id = self._owner_platform_ids.get(callback.source)
        if owner_id != callback.sender_id:
            return
        data = callback.callback_data or ""
        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else "menu"
        if action == "pick":
            await self._show_group_targets(callback)
        elif action == "link" and len(parts) >= 3:
            try:
                target_chat_id = int(parts[2])
            except ValueError:
                await self._show_group_menu(callback, edit=True)
                return
            await self._link_group_by_button(callback, target_chat_id)
        elif action == "list":
            await self._show_group_links(callback, edit=True)
        elif action == "off":
            await self._disconnect_group_by_button(callback)
        elif action == "help":
            await self._show_group_help(callback)
        else:
            await self._show_group_menu(callback, edit=True)

    async def _handle_groupsync_command(
        self, message: UnifiedMessage, command: Command
    ) -> None:
        """Owner-only group link management from inside a group."""
        owner_id = self._owner_platform_ids.get(message.source)
        if owner_id != message.sender_id:
            await self._send_group_notice(message, GroupOwnerOnlyError().message)
            return

        args = command.args
        if not args:
            await self._show_group_menu(message)
            return
        if len(args) > 1:
            await self._send_group_notice(message, self._groupsync_help_text())
            return

        arg = str(args[0]).strip()
        lowered = arg.lower()
        if lowered in {"off", "stop", "unlink"}:
            await self._disconnect_group(message)
        elif lowered in {"list", "ls"}:
            await self._show_group_links(message)
        elif lowered in {"help", "?", "h"}:
            await self._send_group_notice(message, self._groupsync_help_text())
        else:
            await self._complete_group_invite(message, arg.upper())

    async def _issue_group_invite(self, message: UnifiedMessage) -> None:
        """Create a single-use code for this group."""
        if self._group_links is None or self._group_invites is None:
            return
        existing = self._group_links.get_active_for_source(
            message.source, message.chat_id
        )
        if existing is not None:
            await self._send_group_notice(
                message, GroupAlreadyLinkedError().message
            )
            return
        target = (
            Platform.BALE
            if message.source is Platform.TELEGRAM
            else Platform.TELEGRAM
        )
        now = self._clock()
        self._group_invites.revoke_pending_for_source(
            message.source, message.chat_id
        )
        token = new_group_token()
        try:
            invite = self._group_invites.create(
                token=token,
                source_platform=message.source,
                source_chat_id=message.chat_id,
                created_by=message.sender_id,
                created_at=now,
                expires_at=now + self._group_invite_ttl,
            )
        except Exception as exc:  # noqa: BLE001 - command must not kill polling
            self._log.error(
                "group invite create failed platform=%s chat_id=%s (%s)",
                message.source.value,
                message.chat_id,
                type(exc).__name__,
            )
            await self._send_group_notice(
                message, "ساخت کد اتصال ناموفق بود. دوباره تلاش کنید."
            )
            return

        minutes = max(1, int(self._group_invite_ttl.total_seconds() // 60))
        text = (
            "🔗 کد اتصال این گروه:\n"
            f"{invite.token}\n\n"
            f"این کد را در گروه {self._group_platform_name(target)} بفرستید:\n"
            f"/groupsync {invite.token}\n\n"
            f"اعتبار: {minutes} دقیقه"
        )
        await self._send_group_notice(message, text)
        self._log.info(
            "group invite created platform=%s chat_id=%s target=%s",
            message.source.value,
            message.chat_id,
            target.value,
        )

    async def _complete_group_invite(
        self, message: UnifiedMessage, token: str
    ) -> None:
        """Consume a code and create the link between the two groups."""
        if (
            self._group_links is None
            or self._group_invites is None
            or self._group_outbox is None
        ):
            return
        now = self._clock()
        invite = self._group_invites.get_pending(token, now=now)
        if invite is None:
            await self._send_group_notice(
                message, GroupInviteInvalidError().message
            )
            return
        if invite.source_platform is message.source:
            expected = (
                "بله"
                if invite.source_platform is Platform.TELEGRAM
                else "تلگرام"
            )
            await self._send_group_notice(
                message, f"این کد باید در گروه {expected} استفاده شود."
            )
            return
        if self._group_links.get_active_for_source(
            message.source, message.chat_id
        ) is not None or self._group_links.get_active_for_source(
            invite.source_platform, invite.source_chat_id
        ) is not None:
            await self._send_group_notice(
                message, GroupAlreadyLinkedError().message
            )
            return

        used = self._group_invites.mark_used(
            token, used_at=now, used_chat_id=message.chat_id
        )
        if used is None:
            await self._send_group_notice(
                message, GroupInviteInvalidError().message
            )
            return

        if invite.source_platform is Platform.TELEGRAM:
            telegram_chat_id = invite.source_chat_id
            bale_chat_id = message.chat_id
        else:
            telegram_chat_id = message.chat_id
            bale_chat_id = invite.source_chat_id
        try:
            link = self._group_links.link(
                telegram_chat_id=telegram_chat_id,
                bale_chat_id=bale_chat_id,
                now=now,
            )
        except GroupAlreadyLinkedError:
            await self._send_group_notice(
                message, GroupAlreadyLinkedError().message
            )
            return
        await self._send_group_notice(
            message,
            "✅ سینک گروه‌ها فعال شد.\n"
            f"تلگرام: {link.telegram_chat_id}\n"
            f"بله: {link.bale_chat_id}",
        )
        self._log.info(
            "group link created link_id=%s telegram_chat=%s bale_chat=%s",
            link.id,
            link.telegram_chat_id,
            link.bale_chat_id,
        )

    async def _disconnect_group(self, message: UnifiedMessage) -> None:
        """Disconnect this group and drop its unsent deliveries."""
        if self._group_links is None or self._group_outbox is None:
            return
        link = self._group_links.get_active_for_source(
            message.source, message.chat_id
        )
        if link is None:
            await self._send_group_notice(
                message, GroupLinkNotFoundError().message
            )
            return
        self._group_links.deactivate(link.id, now=self._clock())
        removed = self._group_outbox.cancel_pending(link.id)
        await self._send_group_notice(message, "✅ سینک این گروه قطع شد.")
        self._log.info(
            "group link disconnected link_id=%s chat_id=%s cancelled=%s",
            link.id,
            message.chat_id,
            removed,
        )

    async def _list_group_links(self, message: UnifiedMessage) -> None:
        """Show every active link to the owner."""
        if self._group_links is None:
            return
        links = self._group_links.list_active()
        if not links:
            await self._send_group_notice(
                message, "هیچ جفت‌گروه فعالی وجود ندارد."
            )
            return
        lines = ["🔗 جفت‌گروه‌های فعال:"]
        for index, link in enumerate(links, start=1):
            lines.append(
                f"{index}) تلگرام {link.telegram_chat_id} ↔ "
                f"بله {link.bale_chat_id}"
            )
        await self._send_group_notice(message, "\n".join(lines))

    def _group_album_id(
        self, messages: Sequence[UnifiedMessage]
    ) -> str | None:
        """Stable album key for group media groups."""
        if len(messages) < 2:
            return None
        group_ids = {message.media_group_id for message in messages}
        if len(group_ids) != 1 or None in group_ids:
            return None
        leader = messages[0]
        return f"{leader.source.value}:{leader.chat_id}:{leader.media_group_id}"

    @staticmethod
    def _group_sender_label(name: str | None) -> str:
        """Sanitise a group sender name for a safe one-line prefix."""
        raw = " ".join((name or "کاربر").split())
        cleaned = "".join(
            char for char in raw if char not in {"*", "_", "`", "[", "]"}
        )
        return (cleaned[:64] or "کاربر").strip()

    @classmethod
    def _group_text(cls, sender: str, body: str) -> str:
        """Prefix a text relay with its group sender."""
        return f"👤 {sender}:\n{body}" if body else f"👤 {sender}"

    @classmethod
    def _group_caption(cls, sender: str, caption: str) -> str:
        """Prefix a media caption with its group sender."""
        return f"👤 {sender}: {caption}" if caption else f"👤 {sender}"

    def _group_payload(
        self,
        *,
        link_id: int,
        message: UnifiedMessage,
        record_id: int,
        target_platform: Platform,
        target_chat_id: int,
        album_id: str | None,
        album_index: int,
        album_size: int,
    ) -> dict[str, object]:
        """Payload for one queued group message (ids + rendered content)."""
        sender = self._group_sender_label(message.sender_name)
        rendered = render_relay_text(
            message.text or "", message.entities, target_platform
        )
        payload: dict[str, object] = {
            "message_id": record_id,
            "group_link_id": link_id,
            "target_platform": target_platform.value,
            "target_chat_id": target_chat_id,
            "source_platform": message.source.value,
            "source_chat_id": message.chat_id,
            "source_user_id": message.sender_id,
            "source_message_id": message.message_id,
            "sender_name": sender,
            "reply_to_message_id": self._group_reply_reference(link_id, message),
        }
        if message.media:
            payload.update(
                kind=GROUP_MEDIA_PAYLOAD_KIND,
                text=self._group_caption(sender, rendered),
                items=[media_item_payload(item) for item in message.media],
            )
        elif message.unsupported is not None:
            payload.update(
                kind=GROUP_TEXT_PAYLOAD_KIND,
                text=self._group_text(
                    sender, unsupported_placeholder(message.unsupported)
                ),
            )
        else:
            payload.update(
                kind=GROUP_TEXT_PAYLOAD_KIND,
                text=self._group_text(sender, rendered),
            )
        if album_id is not None:
            payload["album_id"] = album_id
            payload["album_index"] = album_index
            payload["album_size"] = album_size
        return payload

    def _group_reply_reference(
        self, link_id: int, message: UnifiedMessage
    ) -> int | None:
        """Map a reply in one group to the anchor id in the other group."""
        if message.reply_to is None or self._group_messages is None:
            return None
        if message.source is Platform.TELEGRAM:
            partner_platform = Platform.BALE
        else:
            partner_platform = Platform.TELEGRAM
        link = self._group_links.get(link_id) if self._group_links else None
        if link is None:
            return None
        partner_chat = (
            link.bale_chat_id
            if partner_platform is Platform.BALE
            else link.telegram_chat_id
        )
        record = self._group_messages.find_by_target_in_link(
            link_id,
            source_platform=partner_platform,
            source_chat_id=partner_chat,
            target_message_id=message.reply_to.message_id,
        )
        return record.source_message_id if record is not None else None

    def _same_album_members(
        self, leader: UnifiedMessage, messages: Sequence[UnifiedMessage]
    ) -> tuple[UnifiedMessage, ...]:
        """Members of ``messages`` that really belong to the leader's album/chat.

        A batch is only ever assembled by the media-group collector, but the
        bridge still refuses to mix senders, chats or album ids — one odd member
        must never pull another user's message into a relay.
        """
        same = tuple(
            message
            for message in messages
            if message.source is leader.source
            and message.chat_id == leader.chat_id
            and message.sender_id == leader.sender_id
            and message.media_group_id == leader.media_group_id
        )
        if len(same) != len(messages):
            self._log.warning(
                "dropped %s out-of-group message(s) in one batch platform=%s",
                len(messages) - len(same),
                leader.source.value,
            )
        return same or (leader,)

    # ------------------------------------------------------------ registration

    async def _register(self, message: UnifiedMessage) -> User | None:
        """Register/refresh the sender and remember their chat id.

        Returns ``None`` when registration is refused (for example the
        ``MAX_USERS`` cap is reached); the sender is told why.
        """
        owner_id = self._owner_platform_ids.get(message.source)
        role = UserRole.OWNER if owner_id == message.sender_id else UserRole.USER
        try:
            user = self._users.get_or_register(
                message.source, message.sender_id, role=role
            )
        except DomainError as exc:
            self._log.info(
                "registration refused platform=%s sender_id=%s code=%s",
                message.source.value,
                message.sender_id,
                exc.code,
            )
            await self._send_direct(message.source, message.chat_id, str(exc))
            return None
        return self._users.remember_chat(user.id, message.chat_id)

    # ---------------------------------------------------------------- delivery

    async def _deliver_actions(
        self,
        actor: User,
        actions: Sequence[Action],
        *,
        callback: UnifiedMessage | None = None,
    ) -> None:
        """Deliver every action, keeping the loop alive on messaging failures.

        When the actions came from an inline button, replies addressed back to
        the actor edit the original menu message in place; notifications to the
        partner are still sent as new messages.
        """
        for action in actions:
            try:
                if isinstance(action, EditMessageAction):
                    await self._deliver_edit(action)
                elif isinstance(action, SendMessageAction):
                    if (
                        callback is not None
                        and callback.message_id is not None
                        and action.recipient_id == actor.id
                    ):
                        await self._deliver_edit(
                            EditMessageAction(
                                recipient_id=action.recipient_id,
                                message_id=callback.message_id,
                                text=action.text,
                                buttons=action.buttons,
                            )
                        )
                    else:
                        await self._deliver(action)
                else:
                    self._log.error(
                        "unsupported action type=%s", type(action).__name__
                    )
            except MessagingError as exc:
                if isinstance(action, SendMessageAction):
                    await self._handle_delivery_failure(action, exc)
                else:
                    self._log.warning(
                        "menu update failed recipient_id=%s error=%s",
                        getattr(action, "recipient_id", "?"),
                        exc.message,
                    )

    async def _deliver(self, action: SendMessageAction) -> SentMessage | None:
        """Send one action to its recipient; ``None`` when it cannot be resolved."""
        recipient = self._users.get(action.recipient_id)
        if recipient is None:
            self._log.warning("action recipient unknown recipient_id=%s", action.recipient_id)
            return None
        sender = self._senders.get(recipient.platform)
        if sender is None:
            self._log.warning(
                "no sender configured for platform=%s", recipient.platform.value
            )
            return None
        chat_id = recipient.last_chat_id or recipient.platform_user_id
        return await sender.send_message(
            chat_id,
            action.text,
            reply_to_message_id=action.reply_to_message_id,
            buttons=action.buttons,
        )

    async def _deliver_edit(self, action: EditMessageAction) -> SentMessage | None:
        """Edit a menu message in place, falling back to a fresh message."""
        recipient = self._users.get(action.recipient_id)
        if recipient is None:
            self._log.warning("edit recipient unknown recipient_id=%s", action.recipient_id)
            return None
        sender = self._senders.get(recipient.platform)
        if sender is None:
            self._log.warning(
                "no sender configured for platform=%s", recipient.platform.value
            )
            return None
        chat_id = recipient.last_chat_id or recipient.platform_user_id
        try:
            return await sender.edit_message_text(
                chat_id, action.message_id, action.text, buttons=action.buttons
            )
        except MessagingError as exc:
            if "not modified" in exc.message.lower():
                return None
            self._log.warning(
                "edit failed recipient_id=%s; sending a new message (%s)",
                action.recipient_id,
                type(exc).__name__,
            )
            return await sender.send_message(chat_id, action.text, buttons=action.buttons)

    async def _answer_callback(self, callback: UnifiedMessage) -> None:
        """Stop the client spinner after a button press; never raise."""
        if callback.callback_id is None:
            return
        sender = self._senders.get(callback.source)
        if sender is None:
            return
        try:
            await sender.answer_callback_query(callback.callback_id)
        except MessagingError as exc:
            self._log.warning(
                "answer_callback_query failed platform=%s (%s)",
                callback.source.value,
                type(exc).__name__,
            )

    async def _handle_delivery_failure(
        self, action: SendMessageAction, error: MessagingError
    ) -> None:
        """React to a failed delivery.

        A permanently unreachable recipient aborts the related pairing request
        (reusing the ``cancelled`` transition) and notifies the requester and the
        owner. Temporary failures are only logged — retrying is the delivery
        layer's job, and an explicit outbox worker is not part of this phase.
        """
        self._log.warning(
            "delivery failed recipient_id=%s permanent=%s error=%s",
            action.recipient_id,
            error.permanent,
            error.message,
        )
        request_id = action.related_request_id
        if request_id is None or not error.permanent:
            return

        try:
            request = self._pairing.abort(request_id, reason=error.message)
        except DomainError as exc:
            self._log.info(
                "request=%s not aborted (code=%s)", request_id, exc.code
            )
            return

        self._log.info(
            "request=%s cancelled because its notification to user=%s failed",
            request.id,
            request.target_id,
        )
        await self._send_system(
            request.requester_id,
            (
                f"درخواست جفت‌سازی شما (شماره {request.id}) لغو شد، چون پیام تأیید "
                "به کاربر مقابل تحویل داده نشد (کاربر ربات را استارت نکرده یا آن را "
                "بلاک کرده است)."
            ),
        )
        await self._notify_owners(
            action.recipient_id,
            (
                f"هشدار: درخواست جفت‌سازی {request.id} به کاربر "
                f"{request.target_id} تحویل نشد و لغو شد."
            ),
        )

    # ------------------------------------------------------------------ relay

    async def _handle_content(
        self, user: User, messages: Sequence[UnifiedMessage]
    ) -> None:
        """Relay content, or guide users who have no active pair.

        Banned users are told their access is blocked. Users with an active
        pair get their message(s) relayed to their partner; everyone else gets
        the short pairing guide.
        """
        if user.is_banned:
            self._log.info(
                "relay refused reason=banned platform=%s user_id=%s",
                user.platform.value,
                user.id,
            )
            await self._send_system(user.id, self.BANNED_TEXT)
            return

        pair = self._pairing.active_pair(user.id)
        if pair is None:
            await self._send_system(user.id, self.NO_ACTIVE_PAIR_TEXT)
            return

        await self._relay(user, pair, messages)

    async def _relay(
        self, sender: User, pair: Pair, messages: Sequence[UnifiedMessage]
    ) -> None:
        """Record and queue one relay (an album when several) for the partner.

        Routing decision (see the phase report): the sender must be ``active``,
        the pair must be the sender's own active pair, the partner is looked up
        inside that pair only, and nothing is ever addressed back to the sender.
        Every record and its outbox entry are written together, in one
        transaction, so the dedup guard can never suppress a relay that was
        recorded but not queued.
        """
        partner_id = pair.other(sender.id)
        partner = self._users.get(partner_id)
        if partner is None:
            self._log.warning(
                "relay refused reason=partner_missing pair_id=%s user_id=%s",
                pair.id,
                sender.id,
            )
            return
        if partner.is_banned:
            self._log.info(
                "relay refused reason=partner_banned pair_id=%s user_id=%s",
                pair.id,
                sender.id,
            )
            await self._send_system(
                sender.id,
                "ارسال پیام ممکن نیست: کاربر مقابل مسدود شده است.",
            )
            return

        # Loop/idempotency guard, first pass: an inbound update this pair already
        # relayed (platform redelivery, offset replay) is dropped here, before
        # any rendering work. The read is an optimisation: the insert below,
        # backed by the messages unique index, stays the race-safe authority —
        # and it is what reports a key that belongs to *another* pair.
        fresh = tuple(
            message for message in messages if not self._already_relayed(pair, message)
        )
        if not fresh:
            return

        album_id = self._album_id(fresh)
        now = self._clock()
        queued = 0

        # Records and outbox entries are written in ONE transaction. If the
        # queue write fails, the records roll back with it, so a message is
        # never both "already relayed" and never queued — a redelivery can still
        # relay it.
        try:
            with self._tx.transaction():
                pending: list[tuple[UnifiedMessage, int]] = []
                for message in fresh:
                    record, created = self._messages.add(
                        pair_id=pair.id,
                        source_platform=sender.platform,
                        source_chat_id=message.chat_id,
                        source_message_id=message.message_id,
                        created_at=message.timestamp,
                    )
                    if created:
                        pending.append((message, record.id))
                for index, (message, record_id) in enumerate(pending):
                    self._outbox.enqueue(
                        pair_id=pair.id,
                        payload=self._payload(
                            sender=sender,
                            partner=partner,
                            pair=pair,
                            message=message,
                            record_id=record_id,
                            album_id=album_id,
                            album_index=index,
                            album_size=len(pending),
                        ),
                        next_attempt_at=now,
                        created_at=now,
                    )
                queued = len(pending)
        except DuplicateMessageError:
            self._log.error(
                "relay refused reason=message_belongs_to_other_pair pair_id=%s "
                "user_id=%s source_message_id=%s",
                pair.id,
                sender.id,
                messages[-1].message_id,
            )
            return
        except DomainError as exc:
            self._log.error(
                "relay failed reason=%s pair_id=%s user_id=%s",
                exc.code,
                pair.id,
                sender.id,
            )
            return

        if not queued:
            self._log.info(
                "relay skipped reason=duplicate pair_id=%s user_id=%s",
                pair.id,
                sender.id,
            )
            return
        self._log.info(
            "relay queued pair_id=%s from_user=%s to_user=%s platform=%s "
            "messages=%s album=%s",
            pair.id,
            sender.id,
            partner.id,
            partner.platform.value,
            queued,
            album_id is not None,
        )

    def _album_id(self, messages: Sequence[UnifiedMessage]) -> str | None:
        """Stable album key shared by the members of one media group, or ``None``."""
        if len(messages) < 2:
            return None
        group_ids = {message.media_group_id for message in messages}
        if len(group_ids) != 1 or None in group_ids:
            return None
        leader = messages[0]
        return f"{leader.source.value}:{leader.chat_id}:{leader.media_group_id}"

    def _payload(
        self,
        *,
        sender: User,
        partner: User,
        pair: Pair,
        message: UnifiedMessage,
        record_id: int,
        album_id: str | None,
        album_index: int,
        album_size: int,
    ) -> dict[str, object]:
        """Outbox payload for one relayed message (text, media or placeholder).

        Only ids and metadata travel here — never file bytes and never content
        beyond the caption that must be shown on the other side.
        """
        render = lambda body, entities: render_relay_text(  # noqa: E731 - tiny local
            body, entities, partner.platform
        )
        payload: dict[str, object] = {
            "message_id": record_id,
            "target_user_id": partner.id,
            "target_chat_id": partner.last_chat_id or partner.platform_user_id,
            "reply_to_message_id": self._reply_reference(
                pair, sender.id, message.reply_to
            ),
            "source_platform": sender.platform.value,
            "source_user_id": sender.id,
            "source_message_id": message.message_id,
        }
        if message.media:
            payload.update(
                kind=RELAY_MEDIA_KIND,
                text=render(message.text or "", message.entities) or None,
                items=[media_item_payload(item) for item in message.media],
            )
        elif message.unsupported is not None:
            self._log.info(
                "relay placeholder unsupported_type=%s pair_id=%s user_id=%s",
                message.unsupported,
                pair.id,
                sender.id,
            )
            payload.update(
                kind=RELAY_PAYLOAD_KIND,
                text=unsupported_placeholder(message.unsupported),
            )
        else:
            payload.update(
                kind=RELAY_PAYLOAD_KIND,
                text=render(message.text or "", message.entities),
            )
        if album_id is not None:
            payload["album_id"] = album_id
            payload["album_index"] = album_index
            payload["album_size"] = album_size
        return payload

    def _already_relayed(self, pair: Pair, message: UnifiedMessage) -> bool:
        """Whether ``pair`` already relayed this platform message.

        The lookup itself is global (the natural key is unique across all
        pairs); a hit that belongs to a *different* pair is left to the insert
        below, which refuses it loudly as a corruption signal.
        """
        existing = self._messages.find_by_source(
            message.source, message.chat_id, message.message_id
        )
        if existing is None or existing.pair_id != pair.id:
            return False
        self._log.info(
            "relay skipped reason=duplicate pair_id=%s source_message_id=%s",
            pair.id,
            message.message_id,
        )
        return True

    def _reply_reference(
        self,
        pair: Pair,
        sender_user_id: int,
        reply_to: MessageRef | None,
    ) -> int | None:
        """Map an inbound reply onto the anchor id in the destination chat.

        ``reply_to`` points at a bot-delivered message in the *sender's* chat —
        its platform id is the ``target_message_id`` of the partner's original
        relay record. The lookup therefore matches
        ``(pair, partner platform, partner chat id, target_message_id)``; the
        returned anchor is that record's ``source_message_id``, i.e. the id the
        message carries in the *partner's* chat, where the reply will be shown.
        """
        if reply_to is None:
            return None
        partner = self._partner(pair, sender_user_id)
        if partner is None:
            return None
        record = self._messages.find_by_target_in_pair(
            pair.id,
            source_platform=partner.platform,
            source_chat_id=partner.last_chat_id or partner.platform_user_id,
            target_message_id=reply_to.message_id,
        )
        if record is None:
            self._log.debug(
                "reply target not mapped pair_id=%s user_id=%s", pair.id, sender_user_id
            )
            return None
        return record.source_message_id

    def _partner(self, pair: Pair, sender_user_id: int) -> User | None:
        """The pair member opposite ``sender_user_id``, or ``None``."""
        try:
            partner_id = pair.other(sender_user_id)
        except DomainError:
            return None
        return self._users.get(partner_id)

    async def _send_system(self, user_id: int, text: str) -> None:
        """Best-effort system message; failures are logged, never raised."""
        try:
            await self._deliver(SendMessageAction(recipient_id=user_id, text=text))
        except MessagingError as exc:
            self._log.warning(
                "system message to user_id=%s failed (%s): %s",
                user_id,
                type(exc).__name__,
                exc.message,
            )

    async def _send_direct(self, platform: Platform, chat_id: int, text: str) -> None:
        """Best-effort reply to a chat we may not have a user row for yet."""
        sender = self._senders.get(platform)
        if sender is None:
            self._log.warning("no sender configured for platform=%s", platform.value)
            return
        try:
            await sender.send_message(chat_id, text)
        except MessagingError as exc:
            self._log.warning(
                "reply to chat_id=%s failed (%s): %s", chat_id, type(exc).__name__, exc.message
            )

    async def _notify_owners(self, recipient_id: int, text: str) -> None:
        """Notify the owner on the platform where the failure happened."""
        recipient = self._users.get(recipient_id)
        platform = recipient.platform if recipient is not None else None
        if platform is None:
            return
        owner_platform_id = self._owner_platform_ids.get(platform)
        if owner_platform_id is None:
            return
        owner = self._users.get_by_platform(platform, owner_platform_id)
        if owner is None:
            self._log.info(
                "owner is not registered on platform=%s; warning skipped", platform.value
            )
            return
        await self._send_system(owner.id, text)


__all__ = ["Bridge"]
