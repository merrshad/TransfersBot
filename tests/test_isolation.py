"""Isolation tests: a pair's data must never surface through another pair."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.commands import CommandRouter, SendMessageAction
from core.errors import (
    DuplicateMessageError,
    PairNotFoundError,
    PairRequestNotFoundError,
)
from core.models import OutboxStatus, Platform
from core.pairing import Pair, PairingService, PairingStatus
from core.users import User, UserRole, UserService
from storage.db import Repositories, utc_now


def build_pair(
    users: UserService,
    pairing: PairingService,
    *,
    platform: Platform = Platform.TELEGRAM,
    platform_user_ids: tuple[int, int] | None = None,
) -> tuple[User, User, Pair]:
    """Register two users and activate a pair between them."""
    if platform_user_ids is None:
        first = users.get_or_register(platform, 7001)
        second = users.get_or_register(platform, 7002)
    else:
        first = users.get_or_register(platform, platform_user_ids[0])
        second = users.get_or_register(platform, platform_user_ids[1])
    request = pairing.request(first.id, second.id)
    pair = pairing.accept(request.id, second.id)
    return first, second, pair


def add_message(
    repos: Repositories,
    pair_id: int,
    *,
    chat_id: int,
    message_id: int,
    platform: Platform = Platform.TELEGRAM,
    offset_minutes: int = 0,
):
    """Store one message for a pair."""
    record, created = repos.messages.add(
        pair_id=pair_id,
        source_platform=platform,
        source_chat_id=chat_id,
        source_message_id=message_id,
        created_at=utc_now() + timedelta(minutes=offset_minutes),
    )
    return record, created


@pytest.fixture
def two_pairs(
    users: UserService, pairing: PairingService
) -> tuple[tuple[User, User, Pair], tuple[User, User, Pair]]:
    """Four users forming two independent active pairs."""
    first = build_pair(users, pairing, platform_user_ids=(7001, 7002))
    second = build_pair(
        users, pairing, platform=Platform.BALE, platform_user_ids=(8001, 8002)
    )
    return first, second


def test_messages_of_one_pair_never_surface_in_another(
    repos: Repositories, two_pairs
) -> None:
    (alice, bob, pair_a), (carol, dave, pair_b) = two_pairs
    first_a, _ = add_message(repos, pair_a.id, chat_id=10, message_id=1)
    second_a, _ = add_message(repos, pair_a.id, chat_id=10, message_id=2)
    first_b, _ = add_message(repos, pair_b.id, chat_id=20, message_id=1, offset_minutes=1)

    assert [record.id for record in repos.messages.list_for_pair(pair_a.id)] == [
        first_a.id,
        second_a.id,
    ]
    assert [record.id for record in repos.messages.list_for_pair(pair_b.id)] == [first_b.id]

    # Cross-pair lookups are always empty, even with the other pair's real id.
    assert repos.messages.get_for_pair(pair_b.id, first_a.id) is None
    assert repos.messages.get_for_pair(pair_a.id, first_b.id) is None
    assert (
        repos.messages.get_by_source_for_pair(pair_b.id, Platform.TELEGRAM, 10, 1) is None
    )
    assert repos.messages.get_by_source_for_pair(pair_a.id, Platform.TELEGRAM, 20, 1) is None
    # ...and pagination cannot walk into another pair's rows.
    assert repos.messages.list_for_pair(pair_a.id, after_id=first_a.id) == (second_a,)
    assert repos.messages.list_for_pair(pair_b.id, after_id=first_b.id) == ()
    assert {record.pair_id for record in repos.messages.list_for_pair(pair_b.id)} == {pair_b.id}


def test_same_source_ids_on_different_platforms_are_distinct(
    repos: Repositories, two_pairs
) -> None:
    (_, _, pair_a), (_, _, pair_b) = two_pairs

    record_a, created_a = add_message(repos, pair_a.id, chat_id=5, message_id=9)
    record_b, created_b = add_message(
        repos, pair_b.id, chat_id=5, message_id=9, platform=Platform.BALE
    )

    assert created_a and created_b
    assert record_a.id != record_b.id
    assert record_a.pair_id == pair_a.id
    assert record_b.pair_id == pair_b.id


def test_duplicate_source_message_cannot_be_reassigned_to_another_pair(
    repos: Repositories, two_pairs
) -> None:
    (_, _, pair_a), (_, _, pair_b) = two_pairs
    original, _ = add_message(repos, pair_a.id, chat_id=3, message_id=4)

    with pytest.raises(DuplicateMessageError):
        add_message(repos, pair_b.id, chat_id=3, message_id=4, platform=Platform.TELEGRAM)

    assert repos.messages.list_for_pair(pair_b.id) == ()
    assert repos.messages.get_for_pair(pair_a.id, original.id) == original


def test_attach_target_message_is_pair_scoped(repos: Repositories, two_pairs) -> None:
    (_, _, pair_a), (_, _, pair_b) = two_pairs
    record, _ = add_message(repos, pair_a.id, chat_id=1, message_id=1)

    assert repos.messages.attach_target_message(pair_b.id, record.id, 999) is None
    assert repos.messages.get_for_pair(pair_a.id, record.id).target_message_id is None

    assert repos.messages.attach_target_message(pair_a.id, record.id, 999).target_message_id == 999


def test_outbox_entries_are_pair_scoped(repos: Repositories, two_pairs) -> None:
    (_, _, pair_a), (_, _, pair_b) = two_pairs
    entry_a = repos.outbox.enqueue(
        pair_id=pair_a.id,
        payload={"text": "for A"},
        next_attempt_at=utc_now(),
        created_at=utc_now(),
    )
    entry_b = repos.outbox.enqueue(
        pair_id=pair_b.id,
        payload={"text": "for B"},
        next_attempt_at=utc_now(),
        created_at=utc_now(),
    )

    assert repos.outbox.list_for_pair(pair_a.id) == (entry_a,)
    assert repos.outbox.list_for_pair(pair_b.id) == (entry_b,)
    assert repos.outbox.get_for_pair(pair_b.id, entry_a.id) is None

    assert repos.outbox.mark_sent(pair_b.id, entry_a.id) is None
    assert repos.outbox.mark_failed(
        pair_b.id, entry_a.id, error="nope", next_attempt_at=utc_now()
    ) is None

    untouched = repos.outbox.get_for_pair(pair_a.id, entry_a.id)
    assert untouched.status is OutboxStatus.PENDING
    assert untouched.attempts == 0
    assert untouched.last_error is None


def test_pair_requests_are_scoped_to_participants(
    users: UserService, pairing: PairingService
) -> None:
    outsider = users.get_or_register(Platform.TELEGRAM, 1)
    other = users.get_or_register(Platform.TELEGRAM, 2)
    request = pairing.request(outsider.id, other.id)

    assert pairing.request_for_member(request.id, outsider.id) == request
    assert pairing.request_for_member(request.id, other.id) == request
    with pytest.raises(PairRequestNotFoundError):
        pairing.request_for_member(request.id, 999_999)

    # A pending request between two users is invisible to the pair querying it.
    third = users.get_or_register(Platform.BALE, 3)
    fourth = users.get_or_register(Platform.BALE, 4)
    pairing.request(third.id, fourth.id)

    assert [r.id for r in pairing.incoming_requests(other.id)] == [request.id]
    assert pairing.outgoing_requests(third.id)[0].target_id == fourth.id
    assert pairing.incoming_requests(third.id) == ()


def test_pair_lookups_reject_non_members(two_pairs, pairing: PairingService) -> None:
    (alice, _, pair_a), (carol, _, _) = two_pairs

    assert pairing.active_pair_for_member(pair_a.id, alice.id).id == pair_a.id
    with pytest.raises(PairNotFoundError):
        pairing.active_pair_for_member(pair_a.id, carol.id)
    with pytest.raises(PairNotFoundError):
        pairing.unpair(pair_a.id, carol.id)

    assert pairing.active_pair(alice.id).id == pair_a.id


def test_unpairing_one_pair_leaves_the_other_intact(
    two_pairs, pairing: PairingService
) -> None:
    (alice, bob, pair_a), (carol, dave, pair_b) = two_pairs

    pairing.unpair(pair_a.id, alice.id)

    assert pairing.active_pair(alice.id) is None
    assert pairing.active_pair(bob.id) is None
    assert pairing.active_pair(carol.id).id == pair_b.id
    assert pairing.active_pair(dave.id).id == pair_b.id


def test_bulk_expiry_does_not_touch_other_users_requests(
    users: UserService, pairing: PairingService, repos: Repositories, clock
) -> None:
    alice = users.get_or_register(Platform.TELEGRAM, 1)
    bob = users.get_or_register(Platform.TELEGRAM, 2)
    carol = users.get_or_register(Platform.BALE, 3)
    dave = users.get_or_register(Platform.BALE, 4)

    stale = pairing.request(alice.id, bob.id)
    clock.advance(minutes=30)
    fresh = pairing.request(carol.id, dave.id)
    clock.advance(hours=23, minutes=30)

    assert pairing.expire_stale_requests() == 1
    assert repos.requests.get(stale.id).status is PairingStatus.EXPIRED
    assert repos.requests.get(fresh.id).status is PairingStatus.PENDING
    assert [r.id for r in pairing.incoming_requests(dave.id)] == [fresh.id]


def test_command_actions_never_reach_a_third_party(
    users: UserService, pairing: PairingService, repos: Repositories
) -> None:
    router = CommandRouter(users, pairing)
    alice = users.get_or_register(Platform.TELEGRAM, 11)
    bob = users.get_or_register(Platform.BALE, 22)
    outsider = users.get_or_register(Platform.TELEGRAM, 33, role=UserRole.OWNER)

    collected: list[SendMessageAction] = []
    collected.extend(router.handle(alice.id, f"/pair {bob.public_id}"))
    request_id = repos.requests.list_pending_outgoing(alice.id)[0].id
    collected.extend(router.handle(bob.id, f"/accept {request_id}"))
    pair = pairing.active_pair(alice.id)
    collected.extend(router.handle(alice.id, f"/unpair {pair.id}"))
    collected.extend(router.handle(alice.id, "/status"))
    collected.extend(router.handle(bob.id, "/pending"))

    assert collected
    allowed = {alice.id, bob.id}
    assert {action.recipient_id for action in collected} <= allowed
    assert outsider.id not in {action.recipient_id for action in collected}
    assert all(isinstance(action, SendMessageAction) for action in collected)


def test_owner_cannot_act_on_another_users_pairing(
    users: UserService, pairing: PairingService, repos: Repositories
) -> None:
    router = CommandRouter(users, pairing)
    alice = users.get_or_register(Platform.BALE, 2)
    bob = users.get_or_register(Platform.BALE, 3)
    owner = users.get_or_register(Platform.TELEGRAM, 1, role=UserRole.OWNER)
    request = pairing.request(alice.id, bob.id)
    pair = pairing.accept(request.id, bob.id)

    for command in (f"/accept {request.id}", f"/cancel {request.id}", f"/unpair {pair.id}"):
        (action,) = router.handle(owner.id, command)
        assert action.recipient_id == owner.id, "errors must go only to the actor"

    # Nothing about the other users was revealed or changed.
    assert repos.requests.get(request.id).status is PairingStatus.ACTIVE
    assert pairing.active_pair(alice.id).id == pair.id

    (status_action,) = router.handle(owner.id, "/status")
    assert status_action.recipient_id == owner.id
    assert f"جفت شماره {pair.id}" not in status_action.text
    assert "جفت فعالی ندارید" in status_action.text


# ------------------------------------------------------- relay lookups stay scoped


def test_target_id_lookup_is_scoped_to_one_pair(repos: Repositories, two_pairs) -> None:
    (alice, bob, pair_a), (carol, dave, pair_b) = two_pairs
    record, _ = add_message(repos, pair_a.id, chat_id=10, message_id=1)
    attached = repos.messages.attach_target_message(pair_a.id, record.id, 555)
    assert attached is not None and attached.target_message_id == 555

    found = repos.messages.find_by_target_in_pair(
        pair_a.id,
        source_platform=Platform.TELEGRAM,
        source_chat_id=10,
        target_message_id=555,
    )
    assert found is not None and found.id == record.id

    # The same target id means nothing inside another pair...
    assert (
        repos.messages.find_by_target_in_pair(
            pair_b.id,
            source_platform=Platform.TELEGRAM,
            source_chat_id=10,
            target_message_id=555,
        )
        is None
    )
    # ...nor with a different source chat (a reply in another chat must not map).
    assert (
        repos.messages.find_by_target_in_pair(
            pair_a.id,
            source_platform=Platform.TELEGRAM,
            source_chat_id=999,
            target_message_id=555,
        )
        is None
    )


def test_source_key_lookup_is_global_by_contract(repos: Repositories, two_pairs) -> None:
    """``find_by_source`` exists only for the relay dedup guard, never for display."""
    (alice, bob, pair_a), _ = two_pairs
    record, _ = add_message(repos, pair_a.id, chat_id=10, message_id=1)

    found = repos.messages.find_by_source(Platform.TELEGRAM, 10, 1)
    assert found is not None and found.pair_id == pair_a.id
    assert repos.messages.find_by_source(Platform.TELEGRAM, 10, 2) is None
