"""Tests for :mod:`core.commands`: parsing, routing, access control, actions."""

from __future__ import annotations

import pytest

from core.commands import Command, CommandRouter, SendMessageAction, parse_command
from core.errors import InvalidArgumentError
from core.models import Platform
from core.pairing import PairingService, PairingStatus
from core.users import UserRole, UserService, UserStatus
from storage.db import Repositories

# -------------------------------------------------------------------- parsing


@pytest.mark.parametrize(
    "text", [None, "", "   ", "hello", "hello world", "/", "@bot hi", "5 /start"]
)
def test_non_commands_parse_to_none(text: str | None) -> None:
    assert parse_command(text) is None


def test_plain_command_parses() -> None:
    command = parse_command("/start")

    assert command == Command(name="start", args=(), raw="/start")


def test_command_is_case_insensitive() -> None:
    assert parse_command("/PAIR 12").name == "pair"


def test_bot_suffix_is_stripped() -> None:
    command = parse_command("/pair@BridgeBot 12")

    assert command is not None
    assert command.name == "pair"
    assert command.args == ("12",)


def test_multiple_arguments_and_whitespace_are_handled() -> None:
    command = parse_command("  /pair   12   13  ")

    assert command is not None
    assert command.args == ("12", "13")
    assert command.raw == "/pair   12   13"


def test_int_arg_parses_numbers() -> None:
    command = parse_command("/pair -5")

    assert command is not None
    assert command.int_arg(0, usage="/pair <id>") == -5


def test_int_arg_reports_missing_argument() -> None:
    command = parse_command("/pair")

    assert command is not None
    with pytest.raises(InvalidArgumentError) as excinfo:
        command.int_arg(0, usage="/pair <id>")

    assert "/pair <id>" in excinfo.value.message


def test_int_arg_reports_non_numeric_argument() -> None:
    command = parse_command("/pair abc")

    assert command is not None
    with pytest.raises(InvalidArgumentError) as excinfo:
        command.int_arg(0, usage="/pair <id>")

    assert "abc" in excinfo.value.message
    assert excinfo.value.code == "invalid_argument"


# -------------------------------------------------------------------- routing


def test_plain_text_produces_no_actions(router: CommandRouter, register) -> None:
    actor = register()

    assert router.handle(actor.id, "سلام، این یک پیام معمولی است") == ()
    assert router.handle(actor.id, "") == ()
    assert router.handle(actor.id, None) == ()


def test_unregistered_actor_is_prompted_to_start(router: CommandRouter) -> None:
    (action,) = router.handle(999_999, "/start")

    assert action == SendMessageAction(999_999, router.UNREGISTERED_TEXT)


def test_unknown_command_returns_error_action(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/wat")

    assert action.recipient_id == actor.id
    assert "دستور نامعتبر" in action.text


def test_actions_are_never_sent_directly(router: CommandRouter, register) -> None:
    actor = register()

    actions = router.handle(actor.id, "/help")

    assert actions
    assert all(isinstance(action, SendMessageAction) for action in actions)
    assert all(action.reply_to_message_id is None for action in actions)


def test_start_replies_with_id_and_help(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/start")

    assert str(actor.public_id) in action.text
    assert "/pair" in action.text
    assert action.recipient_id == actor.id


def test_help_hides_owner_commands_from_regular_users(
    router: CommandRouter, register
) -> None:
    plain = register()
    owner = register(role=UserRole.OWNER)

    (plain_help,) = router.handle(plain.id, "/help")
    (owner_help,) = router.handle(owner.id, "/help")

    assert "/ban" not in plain_help.text
    assert "/ban" in owner_help.text
    assert "/unban" in owner_help.text


def test_whoami_reports_identity(router: CommandRouter, register) -> None:
    actor = register(platform=Platform.BALE)

    (action,) = router.handle(actor.id, "/whoami")

    assert f"شناسه: {actor.public_id}" in action.text
    assert "bale" in action.text
    assert "user" in action.text
    assert "active" in action.text


def test_status_without_pairs(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/status")

    assert "جفت فعالی ندارید" in action.text


# ----------------------------------------------------------------------- pair


def test_pair_accepts_a_lowercase_public_id(
    router: CommandRouter, register, repos: Repositories
) -> None:
    alice = register()
    bob = register()

    router.handle(alice.id, f"/pair {bob.public_id.lower()}")

    request = repos.requests.list_pending_outgoing(alice.id)[0]
    assert request.target_id == bob.id


def test_pair_rejects_a_legacy_numeric_internal_id(
    router: CommandRouter, register, repos: Repositories
) -> None:
    alice = register()
    bob = register()

    (action,) = router.handle(alice.id, f"/pair {bob.id}")

    assert "معتبر نیست" in action.text
    assert repos.requests.list_pending_outgoing(alice.id) == ()


def test_ban_and_unban_use_the_public_id(
    router: CommandRouter, register, users: UserService
) -> None:
    owner = register(role=UserRole.OWNER)
    target = register()

    (banned,) = router.handle(owner.id, f"/ban {target.public_id}")
    assert "مسدود شد" in banned.text
    assert users.get(target.id).is_banned

    (unbanned,) = router.handle(owner.id, f"/unban {target.public_id}")
    assert "برداشته شد" in unbanned.text
    assert users.get(target.id).is_active


def test_pair_button_builds_a_deep_link_and_start_consumes_it(
    router: CommandRouter, register, repos: Repositories
) -> None:
    telegram = register(platform=Platform.TELEGRAM)

    (offer,) = router.handle_callback(telegram.id, "menu:pair")
    url_button = offer.buttons[0][0]
    assert url_button.url is not None
    assert url_button.url.startswith("https://ble.ir/TestBaleBot?start=")
    token = url_button.url.rsplit("start=", 1)[1]

    bale = register(platform=Platform.BALE)
    actions = router.handle(bale.id, f"/start {token}")

    assert "اتصال برقرار شد" in actions[0].text
    assert repos.pairs.count_active() == 1
    assert repos.pairs.find_active_between(telegram.id, bale.id) is not None


def test_pair_deep_link_works_in_the_reverse_direction(
    router: CommandRouter, register, repos: Repositories
) -> None:
    bale = register(platform=Platform.BALE)

    (offer,) = router.handle(bale.id, "/pair")
    url = offer.buttons[0][0].url
    assert url is not None and url.startswith("https://t.me/TestTelegramBot?start=")
    token = url.rsplit("start=", 1)[1]

    telegram = register(platform=Platform.TELEGRAM)
    actions = router.handle(telegram.id, f"/start {token}")

    assert "اتصال برقرار شد" in actions[0].text
    assert repos.pairs.find_active_between(telegram.id, bale.id) is not None


def test_invalid_pairing_token_shows_a_clear_error(
    router: CommandRouter, register
) -> None:
    actor = register(platform=Platform.TELEGRAM)

    (action,) = router.handle(actor.id, "/start not-a-valid-token")

    assert "معتبر نیست" in action.text
    assert "جفت‌سازی" in action.text


def test_owner_ban_callback_uses_the_public_id(
    router: CommandRouter, register, users: UserService
) -> None:
    owner = register(role=UserRole.OWNER)
    target = register()

    (action,) = router.handle_callback(owner.id, f"ban:{target.public_id}")

    assert "مسدود شد" in action.text
    assert users.get(target.id).is_banned


def test_pair_flow_notifies_both_users(router: CommandRouter, register, repos) -> None:
    alice = register()
    bob = register()

    actions = router.handle(alice.id, f"/pair {bob.public_id}")

    assert [action.recipient_id for action in actions] == [alice.id, bob.id]
    request = repos.requests.list_pending_outgoing(alice.id)[0]
    assert str(request.id) in actions[0].text
    assert f"/accept {request.id}" in actions[1].text
    assert f"از کاربر {alice.public_id}" in actions[1].text


def test_pair_with_self_is_refused(router: CommandRouter, register, repos) -> None:
    actor = register()

    (action,) = router.handle(actor.id, f"/pair {actor.public_id}")

    assert action.recipient_id == actor.id
    assert "خودتان" in action.text
    assert repos.requests.list_pending_outgoing(actor.id) == ()


def test_pair_with_unknown_user_is_refused(router: CommandRouter, register, repos) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/pair 999999")

    assert action.recipient_id == actor.id
    assert "پیدا نشد" in action.text
    assert repos.requests.list_pending_outgoing(actor.id) == ()


def test_pair_argument_errors(router: CommandRouter, register, repos) -> None:
    actor = register()

    (missing,) = router.handle(actor.id, "/pair")
    (not_a_number,) = router.handle(actor.id, "/pair abc")

    assert "لینک" in missing.text
    assert missing.buttons[0][0].url is not None
    assert "abc" in not_a_number.text
    assert repos.requests.list_pending_outgoing(actor.id) == ()


def test_pair_is_refused_when_target_is_banned(
    router: CommandRouter, register, repos, users: UserService
) -> None:
    owner = register(role=UserRole.OWNER)
    alice = register()
    bob = register()
    users.ban(owner, bob.id)

    (action,) = router.handle(alice.id, f"/pair {bob.public_id}")

    assert action.recipient_id == alice.id
    assert "مسدود" in action.text
    assert repos.requests.list_pending_outgoing(alice.id) == ()


def test_pair_is_refused_for_banned_actor(
    router: CommandRouter, register, repos, users: UserService
) -> None:
    owner = register(role=UserRole.OWNER)
    alice = register()
    bob = register()
    users.ban(owner, alice.id)

    (action,) = router.handle(alice.id, f"/pair {bob.public_id}")

    assert "مسدود" in action.text
    assert repos.requests.list_pending_outgoing(alice.id) == ()


def test_pairing_commands_reject_malformed_ids(router: CommandRouter, register) -> None:
    actor = register()

    for command in ("/accept x", "/reject x", "/cancel x", "/unpair x", "/ban x", "/unban x"):
        (action,) = router.handle(actor.id, command)
        assert action.recipient_id == actor.id
        assert "استفاده:" in action.text


# ------------------------------------------------------------------- requests


def test_pending_lists_incoming_requests(router: CommandRouter, register) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")

    (action,) = router.handle(bob.id, "/pending")

    assert "درخواست‌های دریافتی" in action.text
    assert f"از کاربر {alice.public_id}" in action.text


def test_pending_without_requests(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/pending")

    assert "درخواست دریافتی فعالی ندارید" in action.text


def test_accept_by_target_activates_pair(router: CommandRouter, register, repos, pairing) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]

    actions = router.handle(bob.id, f"/accept {request.id}")

    assert [action.recipient_id for action in actions] == [bob.id, alice.id]
    assert "فعال شد" in actions[0].text
    pair = pairing.active_pair(alice.id)
    assert pair is not None and pair.status is PairingStatus.ACTIVE
    assert str(pair.id) in actions[0].text


def test_accept_by_requester_is_refused(router: CommandRouter, register, repos) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_outgoing(alice.id)[0]

    (action,) = router.handle(alice.id, f"/accept {request.id}")

    assert action.recipient_id == alice.id
    assert "دریافت‌کننده" in action.text
    assert repos.pairs.count_active_for_user(alice.id) == 0


def test_accept_unknown_request_is_refused(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/accept 4242")

    assert "پیدا نشد" in action.text


def test_reject_notifies_requester(router: CommandRouter, register, repos) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]

    actions = router.handle(bob.id, f"/reject {request.id}")

    assert [action.recipient_id for action in actions] == [bob.id, alice.id]
    assert repos.requests.get(request.id).status is PairingStatus.REJECTED


def test_cancel_notifies_target(router: CommandRouter, register, repos) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_outgoing(alice.id)[0]

    actions = router.handle(alice.id, f"/cancel {request.id}")

    assert [action.recipient_id for action in actions] == [alice.id, bob.id]
    assert repos.requests.get(request.id).status is PairingStatus.CANCELLED


def test_cancel_by_target_is_refused(router: CommandRouter, register, repos) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]

    (action,) = router.handle(bob.id, f"/cancel {request.id}")

    assert "فقط فرستنده" in action.text
    assert repos.requests.get(request.id).status is PairingStatus.PENDING


def test_accept_expired_request_is_refused(
    router: CommandRouter, register, repos, clock
) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]

    clock.advance(hours=25)
    (action,) = router.handle(bob.id, f"/accept {request.id}")

    assert "مهلت" in action.text
    assert repos.requests.get(request.id).status is PairingStatus.EXPIRED


# ---------------------------------------------------------------------- unpair


def test_unpair_without_arguments_ends_active_pair(
    router: CommandRouter, register, repos, pairing
) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]
    router.handle(bob.id, f"/accept {request.id}")

    actions = router.handle(alice.id, "/unpair")

    assert [action.recipient_id for action in actions] == [alice.id, bob.id]
    assert pairing.active_pair(alice.id) is None
    assert "پایان یافت" in actions[0].text


def test_unpair_without_active_pair_is_refused(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/unpair")

    assert "جفت فعالی" in action.text


def test_unpair_with_explicit_pair_id(router: CommandRouter, register, repos, pairing) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]
    router.handle(bob.id, f"/accept {request.id}")
    pair = pairing.active_pair(alice.id)

    actions = router.handle(bob.id, f"/unpair {pair.id}")

    assert [action.recipient_id for action in actions] == [bob.id, alice.id]
    assert pairing.active_pair(alice.id) is None


def test_unpair_other_users_pair_is_refused(router: CommandRouter, register, pairing) -> None:
    alice = register()
    bob = register()
    carol = register()
    pair = pairing.accept(pairing.request(alice.id, bob.id).id, bob.id)

    (action,) = router.handle(carol.id, f"/unpair {pair.id}")

    assert "پیدا نشد" in action.text
    assert pairing.active_pair(alice.id).id == pair.id


# ------------------------------------------------------------------ owner ops


def test_ban_requires_owner(router: CommandRouter, register, users: UserService) -> None:
    actor = register()
    target = register()

    (action,) = router.handle(actor.id, f"/ban {target.public_id}")

    assert "اجازه" in action.text
    assert users.get(target.id).status is UserStatus.ACTIVE


def test_owner_can_ban_and_unban(router: CommandRouter, register, users: UserService) -> None:
    owner = register(role=UserRole.OWNER)
    target = register()

    (ban_action,) = router.handle(owner.id, f"/ban {target.public_id}")
    assert "مسدود شد" in ban_action.text
    assert users.get(target.id).status is UserStatus.BANNED

    (unban_action,) = router.handle(owner.id, f"/unban {target.public_id}")
    assert "برداشته شد" in unban_action.text
    assert users.get(target.id).status is UserStatus.ACTIVE


def test_owner_cannot_be_banned(router: CommandRouter, register) -> None:
    owner = register(role=UserRole.OWNER)

    (action,) = router.handle(owner.id, f"/ban {owner.public_id}")

    assert "مالک" in action.text


def test_banning_unknown_user_is_reported(router: CommandRouter, register) -> None:
    owner = register(role=UserRole.OWNER)

    (action,) = router.handle(owner.id, "/ban 4242")

    assert "پیدا نشد" in action.text


def test_banned_user_can_still_read_help(router: CommandRouter, register, users: UserService) -> None:
    owner = register(role=UserRole.OWNER)
    target = register()
    users.ban(owner, target.id)

    (action,) = router.handle(target.id, "/help")

    assert "/pair" in action.text


def test_status_shows_active_pair_and_requests(repos: Repositories, clock) -> None:
    # Two pairs per user, so an active pair and a pending request can coexist.
    users = UserService(repos.users, repos.db, max_users=0)
    pairing = PairingService(
        repos.users,
        repos.pairs,
        repos.requests,
        repos.db,
        max_pairs_per_user=2,
        clock=clock,
    )
    router = CommandRouter(users, pairing)
    alice = users.get_or_register(Platform.TELEGRAM, 1)
    bob = users.get_or_register(Platform.TELEGRAM, 2)
    carol = users.get_or_register(Platform.TELEGRAM, 3)
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]
    router.handle(bob.id, f"/accept {request.id}")
    router.handle(carol.id, f"/pair {alice.public_id}")

    (action,) = router.handle(alice.id, "/status")

    assert f"با کاربر {bob.public_id}" in action.text
    assert "درخواست‌های دریافتی" in action.text
    assert str(carol.public_id) in action.text


def test_help_covers_every_documented_command(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/help")

    for name in (
        "/start",
        "/whoami",
        "/status",
        "/pair",
        "/pending",
        "/accept",
        "/reject",
        "/cancel",
        "/unpair",
        "/help",
    ):
        assert name in action.text


def test_router_is_stateless_between_calls(router: CommandRouter, register) -> None:
    actor = register()

    first = router.handle(actor.id, "/whoami")
    second = router.handle(actor.id, "/whoami")

    assert first == second


def test_pairing_service_missing_argument_usage_text(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/accept")

    assert "/accept <شناسه درخواست>" in action.text


# ------------------------------------------------------- phase 2 owner commands


def test_users_command_requires_owner(router: CommandRouter, register) -> None:
    actor = register()
    register()

    (action,) = router.handle(actor.id, "/users")

    assert "اجازه" in action.text


def test_owner_lists_users(router: CommandRouter, register) -> None:
    owner = register(role=UserRole.OWNER)
    first = register(platform=Platform.TELEGRAM, platform_user_id=7001)
    second = register(platform=Platform.BALE, platform_user_id=7002)

    (action,) = router.handle(owner.id, "/users")

    assert f"{owner.public_id} | telegram | owner | active" in action.text
    assert f"{second.public_id} | bale | user | active" in action.text
    assert f"{first.public_id} | telegram | user | active" in action.text
    assert "از 3" in action.text


def test_owner_users_command_honours_an_explicit_limit(
    router: CommandRouter, register
) -> None:
    owner = register(role=UserRole.OWNER)
    for index in range(4):
        register(platform_user_id=8000 + index)

    (action,) = router.handle(owner.id, "/users 2")

    listed = [line for line in action.text.splitlines() if line.startswith("  ")]
    assert len(listed) == 2


def test_users_command_rejects_bad_limits(router: CommandRouter, register) -> None:
    owner = register(role=UserRole.OWNER)

    for command in ("/users 0", "/users 999", "/users abc"):
        (action,) = router.handle(owner.id, command)
        assert action.recipient_id == owner.id
        assert "تعداد" in action.text or "عدد معتبر" in action.text


def test_owner_status_shows_bridge_wide_counters(router: CommandRouter, register, repos) -> None:
    owner = register(role=UserRole.OWNER)
    alice = register(platform=Platform.TELEGRAM, platform_user_id=9001)
    bob = register(platform=Platform.BALE, platform_user_id=9002)
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]
    router.handle(bob.id, f"/accept {request.id}")

    (action,) = router.handle(owner.id, "/status")

    assert "وضعیت کلی پل" in action.text
    assert "کاربران: 3 (فعال 3، مسدود 0، مالک 1)" in action.text
    assert "جفت‌های فعال: 1" in action.text
    assert "درخواست‌های در انتظار: 0" in action.text


def test_regular_user_status_has_no_bridge_counters(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/status")

    assert "وضعیت کلی پل" not in action.text
    assert "جفت فعالی ندارید" in action.text


def test_owner_help_mentions_the_users_command(router: CommandRouter, register) -> None:
    owner = register(role=UserRole.OWNER)
    plain = register()

    (owner_help,) = router.handle(owner.id, "/help")
    (plain_help,) = router.handle(plain.id, "/help")

    assert "/users" in owner_help.text
    assert "/users" not in plain_help.text


def test_help_describes_the_current_relay_and_buttons(
    router: CommandRouter, register
) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/help")

    assert "دکمه‌های شیشه‌ای" in action.text
    assert "/pair" in action.text


def test_pair_notification_carries_the_request_context(
    router: CommandRouter, register
) -> None:
    alice = register()
    bob = register()

    actions = router.handle(alice.id, f"/pair {bob.public_id}")

    assert actions[0].related_request_id is None, "the requester's confirmation is unrelated"
    target_notice = actions[1]
    assert target_notice.related_request_id is not None
    assert f"/accept {target_notice.related_request_id}" in target_notice.text


# --------------------------------------------------------- inline keyboard UI


def flat_callbacks(action: SendMessageAction) -> list[str]:
    """Every callback_data in an action's keyboard, in row order."""
    return [button.callback_data for row in action.buttons for button in row]


def test_start_shows_the_main_inline_menu(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle(actor.id, "/start")

    assert {"menu:status", "menu:pair", "menu:pending", "menu:help"} <= set(
        flat_callbacks(action)
    )


def test_pair_notification_has_accept_and_reject_buttons(
    router: CommandRouter, register
) -> None:
    alice = register()
    bob = register()

    actions = router.handle(alice.id, f"/pair {bob.public_id}")

    request = actions[1]
    assert f"accept:{request.related_request_id}" in flat_callbacks(request)
    assert f"reject:{request.related_request_id}" in flat_callbacks(request)


def test_pending_and_status_offer_contextual_buttons(
    router: CommandRouter, register, repos: Repositories
) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]

    (pending,) = router.handle(bob.id, "/pending")
    assert f"accept:{request.id}" in flat_callbacks(pending)
    assert f"reject:{request.id}" in flat_callbacks(pending)

    router.handle(bob.id, f"/accept {request.id}")
    (status,) = router.handle(alice.id, "/status")
    pair = repos.pairs.list_active_for_user(alice.id)[0]
    assert f"unpair:{pair.id}" in flat_callbacks(status)


def test_callback_menu_navigation_and_accept(
    router: CommandRouter, register, repos: Repositories
) -> None:
    alice = register()
    bob = register()
    router.handle(alice.id, f"/pair {bob.public_id}")
    request = repos.requests.list_pending_incoming(bob.id)[0]

    (menu,) = router.handle_callback(alice.id, "menu:main")
    assert "منوی اصلی" in menu.text
    assert "menu:pair" in flat_callbacks(menu)

    accepted = router.handle_callback(bob.id, f"accept:{request.id}")
    assert "فعال شد" in accepted[0].text
    assert repos.pairs.count_active() == 1


def test_non_owner_cannot_open_the_owner_panel(router: CommandRouter, register) -> None:
    actor = register()

    (action,) = router.handle_callback(actor.id, "menu:owner")

    assert "اجازه" in action.text
    assert "menu:owner" not in flat_callbacks(action)


def test_owner_panel_and_user_actions_have_buttons(
    router: CommandRouter, register
) -> None:
    owner = register(role=UserRole.OWNER)
    other = register()

    (panel,) = router.handle_callback(owner.id, "menu:owner")
    assert {"menu:users", "menu:ban", "menu:unban"} <= set(flat_callbacks(panel))

    (users,) = router.handle(owner.id, "/users 10")
    assert f"ban:{other.public_id}" in flat_callbacks(users)
