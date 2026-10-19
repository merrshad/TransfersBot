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
from datetime import UTC, datetime

from core.commands import (
    Action,
    CommandRouter,
    EditMessageAction,
    SendMessageAction,
    parse_command,
)
from core.delivery import RELAY_PAYLOAD_KIND
from core.errors import DomainError, DuplicateMessageError, MessagingError, UserBannedError
from core.formatting import render_relay_text
from core.media import RELAY_MEDIA_KIND, media_item_payload, unsupported_placeholder
from core.models import MessageRef, Platform, SentMessage, UnifiedMessage
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
