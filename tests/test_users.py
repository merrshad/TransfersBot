"""Tests for :mod:`core.users`: open registration, roles, bans, user cap."""

from __future__ import annotations

import pytest

from core.errors import (
    CannotBanOwnerError,
    PermissionDeniedError,
    UserBannedError,
    UserLimitReachedError,
    UserNotFoundError,
)
from core.models import Platform
from core.users import User, UserRole, UserService, UserStatus
from storage.db import Repositories


def test_enum_values_match_database_constraints() -> None:
    assert {role.value for role in UserRole} == {"owner", "user"}
    assert {status.value for status in UserStatus} == {"active", "banned"}


def test_registration_is_open_and_immediately_active(register) -> None:
    user = register(platform=Platform.TELEGRAM, platform_user_id=555)

    assert isinstance(user, User)
    assert user.role is UserRole.USER
    assert user.status is UserStatus.ACTIVE
    assert user.is_active and not user.is_banned and not user.is_owner
    assert user.platform is Platform.TELEGRAM
    assert user.platform_user_id == 555


def test_each_user_gets_a_short_random_public_id(users: UserService) -> None:
    first = users.get_or_register(Platform.TELEGRAM, 101)
    second = users.get_or_register(Platform.BALE, 102)

    assert len(first.public_id) == 8
    assert len(second.public_id) == 8
    assert first.public_id != second.public_id
    assert users.get_by_public_id(first.public_id) == first
    assert users.get_by_public_id(first.public_id.lower()) is None


def test_registration_is_idempotent(users: UserService) -> None:
    first = users.get_or_register(Platform.TELEGRAM, 42)
    second = users.get_or_register(Platform.TELEGRAM, 42)

    assert first.id == second.id
    assert users.count() == 1


def test_same_numeric_id_on_both_platforms_is_two_users(users: UserService) -> None:
    telegram_user = users.get_or_register(Platform.TELEGRAM, 99)
    bale_user = users.get_or_register(Platform.BALE, 99)

    assert telegram_user.id != bale_user.id
    assert users.count() == 2


def test_owner_registration_sets_role(users: UserService) -> None:
    owner = users.get_or_register(Platform.TELEGRAM, 1, role=UserRole.OWNER)

    assert owner.is_owner


def test_existing_user_is_promoted_to_owner_without_status_change(users: UserService) -> None:
    user = users.get_or_register(Platform.BALE, 7)
    owner = users.get_or_register(Platform.TELEGRAM, 1, role=UserRole.OWNER)
    users.ban(owner, user.id)

    promoted = users.get_or_register(Platform.BALE, 7, role=UserRole.OWNER)

    assert promoted.id == user.id
    assert promoted.is_owner
    assert promoted.is_banned, "promotion must not silently unban the account"


def test_plain_login_does_not_demote_owner(users: UserService) -> None:
    users.get_or_register(Platform.TELEGRAM, 1, role=UserRole.OWNER)

    again = users.get_or_register(Platform.TELEGRAM, 1)

    assert again.is_owner


def test_max_users_blocks_extra_registrations(repos: Repositories) -> None:
    service = UserService(repos.users, repos.db, max_users=2)

    service.get_or_register(Platform.TELEGRAM, 1)
    service.get_or_register(Platform.TELEGRAM, 2)

    with pytest.raises(UserLimitReachedError) as excinfo:
        service.get_or_register(Platform.TELEGRAM, 3)
    assert excinfo.value.code == "user_limit_reached"
    assert service.count() == 2


def test_owner_registration_bypasses_max_users(repos: Repositories) -> None:
    service = UserService(repos.users, repos.db, max_users=1)
    service.get_or_register(Platform.TELEGRAM, 1)

    owner = service.get_or_register(Platform.BALE, 1, role=UserRole.OWNER)

    assert owner.is_owner
    assert service.count() == 2


def test_max_users_zero_is_unlimited(repos: Repositories) -> None:
    service = UserService(repos.users, repos.db, max_users=0)

    for platform_user_id in range(10):
        service.get_or_register(Platform.TELEGRAM, platform_user_id)

    assert service.count() == 10


def test_negative_max_users_is_a_programming_error(repos: Repositories) -> None:
    with pytest.raises(ValueError):
        UserService(repos.users, repos.db, max_users=-1)


def test_existing_registration_still_works_at_capacity(repos: Repositories) -> None:
    service = UserService(repos.users, repos.db, max_users=1)
    first = service.get_or_register(Platform.TELEGRAM, 1)

    again = service.get_or_register(Platform.TELEGRAM, 1)

    assert again.id == first.id


def test_ban_and_unban_by_owner(users: UserService, register) -> None:
    owner = register(role=UserRole.OWNER)
    target = register()

    banned = users.ban(owner, target.id)
    assert banned.status is UserStatus.BANNED
    assert users.get(target.id).is_banned

    unbanned = users.unban(owner, target.id)
    assert unbanned.status is UserStatus.ACTIVE


def test_ban_and_unban_are_idempotent(users: UserService, register) -> None:
    owner = register(role=UserRole.OWNER)
    target = register()

    first = users.ban(owner, target.id)
    second = users.ban(owner, target.id)

    assert first == second
    assert users.unban(owner, target.id) == users.unban(owner, target.id)


def test_non_owner_cannot_ban_or_unban(users: UserService, register) -> None:
    actor = register()
    target = register()

    with pytest.raises(PermissionDeniedError):
        users.ban(actor, target.id)
    with pytest.raises(PermissionDeniedError):
        users.unban(actor, target.id)

    assert users.get(target.id).is_active


def test_owner_cannot_be_banned(users: UserService, register) -> None:
    owner = register(role=UserRole.OWNER)
    other_owner = register(role=UserRole.OWNER)

    with pytest.raises(CannotBanOwnerError):
        users.ban(owner, other_owner.id)
    with pytest.raises(CannotBanOwnerError):
        users.ban(owner, owner.id)

    assert users.get(other_owner.id).is_active


def test_banning_unknown_user_fails(users: UserService, register) -> None:
    owner = register(role=UserRole.OWNER)

    with pytest.raises(UserNotFoundError):
        users.ban(owner, 999_999)


def test_require_and_require_active(users: UserService, register) -> None:
    owner = register(role=UserRole.OWNER)
    target = register()

    assert users.require(target.id).id == target.id
    assert users.require_active(target.id).is_active

    with pytest.raises(UserNotFoundError):
        users.require(999_999)

    users.ban(owner, target.id)
    with pytest.raises(UserBannedError) as excinfo:
        users.require_active(target.id)
    assert excinfo.value.code == "user_banned"


def test_require_owner_guard(users: UserService, register) -> None:
    owner = register(role=UserRole.OWNER)
    plain = register()

    assert users.require_owner(owner) == owner
    with pytest.raises(PermissionDeniedError):
        users.require_owner(plain)


def test_lookup_helpers(users: UserService, register) -> None:
    user = register(platform=Platform.BALE, platform_user_id=321)

    assert users.get(user.id) == user
    assert users.get_by_platform(Platform.BALE, 321) == user
    assert users.get_by_platform(Platform.TELEGRAM, 321) is None
    assert users.get(999_999) is None


def test_banned_status_survives_reregistration(users: UserService, register) -> None:
    owner = register(role=UserRole.OWNER)
    target = register(platform=Platform.TELEGRAM, platform_user_id=88)
    users.ban(owner, target.id)

    again = users.get_or_register(Platform.TELEGRAM, 88)

    assert again.is_banned
