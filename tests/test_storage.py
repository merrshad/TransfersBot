"""Tests for :mod:`storage.db`: migrations, constraints, repositories, tx."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from core.errors import DuplicateMessageError
from core.models import OutboxStatus, Platform
from core.pairing import PairingStatus
from core.users import UserRole, UserStatus
from storage.db import (
    MIGRATIONS,
    Database,
    Repositories,
    from_db_time,
    to_db_time,
    utc_now,
)

UTC = UTC
NOW = datetime(2026, 5, 4, 10, 30, 0, 123456, tzinfo=UTC)

USER_COLUMNS = "(platform, platform_user_id, status, role, created_at)"
USER_PLACEHOLDERS = "(?, ?, ?, ?, ?)"
PAIR_COLUMNS = "(user_a_id, user_b_id, status, created_at, ended_at)"
PAIR_PLACEHOLDERS = "(?, ?, ?, ?, ?)"
REQUEST_COLUMNS = "(requester_id, target_id, status, created_at, expires_at, pair_id)"
REQUEST_PLACEHOLDERS = "(?, ?, ?, ?, ?, ?)"
OUTBOX_COLUMNS = "(pair_id, payload, attempts, next_attempt_at, status, created_at)"
OUTBOX_PLACEHOLDERS = "(?, ?, ?, ?, ?, ?)"


def sql(repos: Repositories, query: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
    """Run raw SQL (test-only helper for negative/constraint cases)."""
    return repos.db.connection.execute(query, params)


def insert_user_row(
    repos: Repositories,
    *,
    platform: str = "telegram",
    platform_user_id: int = 1,
    status: str = "active",
    role: str = "user",
    created_at: str | None = None,
) -> None:
    """Insert a user with raw SQL so constraint violations surface directly."""
    sql(
        repos,
        f"INSERT INTO users {USER_COLUMNS} VALUES {USER_PLACEHOLDERS}",
        (platform, platform_user_id, status, role, created_at or to_db_time(NOW)),
    )


@pytest.fixture
def with_users(repos: Repositories) -> Repositories:
    """Repositories whose ``users`` table already holds internal ids 1..10."""
    for index in range(1, 11):
        repos.users.create(
            platform=Platform.BALE,
            platform_user_id=1000 + index,
            role=UserRole.USER,
            status=UserStatus.ACTIVE,
            created_at=NOW,
        )
    return repos


@pytest.fixture
def two_pairs(repos: Repositories) -> tuple[int, int]:
    """Two unrelated active pairs built from four brand-new users."""
    user_ids = [
        repos.users.create(
            platform=Platform.TELEGRAM,
            platform_user_id=6000 + index,
            role=UserRole.USER,
            status=UserStatus.ACTIVE,
            created_at=NOW,
        ).id
        for index in range(4)
    ]
    first = repos.pairs.create(
        user_a_id=user_ids[0], user_b_id=user_ids[1], status=PairingStatus.ACTIVE, created_at=NOW
    )
    second = repos.pairs.create(
        user_a_id=user_ids[2], user_b_id=user_ids[3], status=PairingStatus.ACTIVE, created_at=NOW
    )
    return first.id, second.id


# ------------------------------------------------------------------ migrations


def test_fresh_database_is_migrated_to_latest_version(repos: Repositories) -> None:
    assert repos.db.schema_version() == 7
    assert repos.db.applied_versions() == (1, 2, 3, 4, 5, 6, 7)


def test_schema_contains_all_required_tables(repos: Repositories) -> None:
    rows = sql(
        repos,
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    ).fetchall()
    names = {row["name"] for row in rows}

    assert {
        "users",
        "pairs",
        "pair_requests",
        "messages",
        "outbox",
        "outbox_errors",
        "pairing_invites",
        "group_links",
        "group_link_invites",
        "group_messages",
        "group_outbox",
        "poll_offsets",
        "schema_version",
    } <= names


def test_schema_contains_required_indexes(repos: Repositories) -> None:
    rows = sql(repos, "SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
    names = {row["name"] for row in rows}

    assert {
        "ux_messages_source",
        "ux_pair_requests_pending_members",
        "ux_pairs_active_members",
    } <= names


def test_schema_versions_are_recorded_exactly_once(repos: Repositories) -> None:
    total = sql(repos, "SELECT COUNT(*) AS total FROM schema_version").fetchone()["total"]

    assert total == len(repos.db.applied_versions()) == 7


def test_migrate_is_idempotent(repos: Repositories) -> None:
    before = repos.db.applied_versions()

    assert repos.db.migrate() == 7
    assert repos.db.applied_versions() == before


def test_reopening_a_file_database_keeps_data_and_skips_migrations(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "bridge.db"
    first = Repositories.open(path)
    first.users.create(
        platform=Platform.TELEGRAM,
        platform_user_id=77,
        role=UserRole.USER,
        status=UserStatus.ACTIVE,
        created_at=NOW,
    )
    first.close()

    assert path.exists(), "missing parent directories must be created"
    second = Repositories.open(path)
    try:
        assert second.users.count() == 1
        assert second.db.applied_versions() == (1, 2, 3, 4, 5, 6, 7)
    finally:
        second.close()


def test_in_memory_database_has_no_file() -> None:
    database = Database(":memory:")
    try:
        assert database.path == ":memory:"
        assert database.schema_version() == 7
    finally:
        database.close()


def test_last_chat_id_column_is_added_by_migration_two(repos: Repositories) -> None:
    columns = {
        row["name"]
        for row in sql(repos, "PRAGMA table_info(users)").fetchall()
    }

    assert "last_chat_id" in columns


def test_migration_three_preserves_rows_and_backfills_timestamps(
    tmp_path: Path,
) -> None:
    """A v2 file upgrades in place: old outbox rows survive with timestamps."""
    path = tmp_path / "v2.db"
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        for migration in MIGRATIONS[:2]:
            for statement in migration.statements:
                connection.execute(statement)
            connection.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                (migration.version, to_db_time(NOW)),
            )
        connection.execute(
            "INSERT INTO users (id, platform, platform_user_id, status, role, created_at) "
            "VALUES (1, 'telegram', 1, 'active', 'user', ?)",
            (to_db_time(NOW),),
        )
        connection.execute(
            "INSERT INTO users (id, platform, platform_user_id, status, role, created_at) "
            "VALUES (2, 'bale', 2, 'active', 'user', ?)",
            (to_db_time(NOW),),
        )
        connection.execute(
            "INSERT INTO pairs (id, user_a_id, user_b_id, status, created_at) "
            "VALUES (1, 1, 2, 'active', ?)",
            (to_db_time(NOW),),
        )
        connection.execute(
            "INSERT INTO outbox "
            "(id, pair_id, payload, attempts, next_attempt_at, status, created_at, last_error) "
            "VALUES (1, 1, '{}', 2, ?, 'sent', ?, NULL)",
            (to_db_time(NOW), to_db_time(NOW)),
        )
        connection.commit()
    finally:
        connection.close()

    repos = Repositories.open(path)
    try:
        assert repos.db.schema_version() == 7
        entry = repos.outbox.get_for_pair(1, 1)
        assert entry is not None
        assert entry.status is OutboxStatus.SENT
        assert entry.attempts == 2
        assert entry.sent_at == NOW
        assert entry.last_attempt_at == NOW
        for user_id in (1, 2):
            migrated = repos.users.get_by_id(user_id)
            assert migrated is not None
            assert len(migrated.public_id) == 8
            assert repos.users.get_by_public_id(migrated.public_id) == migrated
    finally:
        repos.close()


# ---------------------------------------------------------------- time helpers


def test_time_round_trip_preserves_microseconds_and_utc() -> None:
    stored = to_db_time(NOW)

    assert from_db_time(stored) == NOW
    assert stored.endswith("+00:00")


def test_to_db_time_converts_other_timezones_to_utc() -> None:
    plus_two = timezone(timedelta(hours=2))
    moment = datetime(2026, 1, 1, 12, 0, tzinfo=plus_two)

    assert from_db_time(to_db_time(moment)) == moment.astimezone(UTC)
    assert from_db_time(to_db_time(moment)).hour == 10


def test_to_db_time_rejects_naive_datetimes() -> None:
    with pytest.raises(ValueError):
        to_db_time(datetime(2026, 1, 1, 12, 0))


def test_from_db_time_accepts_naive_text_as_utc() -> None:
    assert from_db_time("2026-01-01T12:00:00").tzinfo is UTC


def test_utc_now_is_aware() -> None:
    assert utc_now().tzinfo is not None


# ------------------------------------------------------------------ constraints


def test_user_platform_identity_is_unique(repos: Repositories) -> None:
    insert_user_row(repos, platform_user_id=5)

    with pytest.raises(sqlite3.IntegrityError):
        insert_user_row(repos, platform_user_id=5)


def test_same_numeric_id_on_another_platform_is_allowed(repos: Repositories) -> None:
    insert_user_row(repos, platform="telegram", platform_user_id=5)
    insert_user_row(repos, platform="bale", platform_user_id=5)

    assert repos.users.count() == 2


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("platform", "whatsapp"),
        ("status", "sleeping"),
        ("role", "admin"),
    ],
)
def test_user_columns_are_constrained(repos: Repositories, column: str, value: str) -> None:
    overrides: dict[str, str] = {column: value}

    with pytest.raises(sqlite3.IntegrityError):
        insert_user_row(repos, **overrides)  # type: ignore[arg-type]


def test_pairs_require_canonical_member_order(with_users: Repositories) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        sql(
            with_users,
            f"INSERT INTO pairs {PAIR_COLUMNS} VALUES {PAIR_PLACEHOLDERS}",
            (2, 1, "active", to_db_time(NOW), None),
        )


def test_only_one_active_pair_per_member_pair(with_users: Repositories) -> None:
    first = with_users.pairs.create(
        user_a_id=1, user_b_id=2, status=PairingStatus.ACTIVE, created_at=NOW
    )
    assert first.status is PairingStatus.ACTIVE

    with pytest.raises(sqlite3.IntegrityError):
        with_users.pairs.create(
            user_a_id=1, user_b_id=2, status=PairingStatus.ACTIVE, created_at=NOW
        )

    # An ended pair for the same members is still allowed (history is kept).
    with_users.pairs.set_status(first.id, PairingStatus.UNPAIRED, ended_at=NOW)
    second = with_users.pairs.create(
        user_a_id=1, user_b_id=2, status=PairingStatus.ACTIVE, created_at=NOW
    )
    assert second.id != first.id


def test_pair_ended_at_must_match_status(with_users: Repositories) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        sql(
            with_users,
            f"INSERT INTO pairs {PAIR_COLUMNS} VALUES {PAIR_PLACEHOLDERS}",
            (1, 2, "active", to_db_time(NOW), to_db_time(NOW)),
        )
    with pytest.raises(sqlite3.IntegrityError):
        sql(
            with_users,
            f"INSERT INTO pairs {PAIR_COLUMNS} VALUES {PAIR_PLACEHOLDERS}",
            (1, 2, "unpaired", to_db_time(NOW), None),
        )


def test_pairs_reject_self_membership(with_users: Repositories) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        with_users.pairs.create(
            user_a_id=1, user_b_id=1, status=PairingStatus.ACTIVE, created_at=NOW
        )


def test_pair_requests_cannot_target_self(with_users: Repositories) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        sql(
            with_users,
            f"INSERT INTO pair_requests {REQUEST_COLUMNS} VALUES {REQUEST_PLACEHOLDERS}",
            (1, 1, "pending", to_db_time(NOW), to_db_time(NOW), None),
        )


def test_duplicate_pending_request_is_blocked_in_both_directions(
    with_users: Repositories,
) -> None:
    with_users.requests.create(requester_id=1, target_id=2, created_at=NOW, expires_at=NOW)

    with pytest.raises(sqlite3.IntegrityError):
        with_users.requests.create(requester_id=1, target_id=2, created_at=NOW, expires_at=NOW)
    with pytest.raises(sqlite3.IntegrityError):
        with_users.requests.create(requester_id=2, target_id=1, created_at=NOW, expires_at=NOW)


def test_pending_request_uniqueness_is_released_after_expiry(with_users: Repositories) -> None:
    first = with_users.requests.create(
        requester_id=1, target_id=2, created_at=NOW, expires_at=NOW
    )
    with_users.requests.set_status(first.id, PairingStatus.EXPIRED)

    second = with_users.requests.create(
        requester_id=2, target_id=1, created_at=NOW, expires_at=NOW
    )

    assert second.id != first.id
    assert second.status is PairingStatus.PENDING


def test_active_request_requires_pair_id(with_users: Repositories) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        sql(
            with_users,
            f"INSERT INTO pair_requests {REQUEST_COLUMNS} VALUES {REQUEST_PLACEHOLDERS}",
            (1, 2, "active", to_db_time(NOW), to_db_time(NOW), None),
        )


def test_messages_source_key_is_unique(two_pairs: tuple[int, int], repos: Repositories) -> None:
    pair_id, _ = two_pairs
    repos.messages.add(
        pair_id=pair_id,
        source_platform=Platform.TELEGRAM,
        source_chat_id=10,
        source_message_id=20,
        created_at=NOW,
    )

    with pytest.raises(sqlite3.IntegrityError):
        sql(
            repos,
            """
            INSERT INTO messages
                (pair_id, source_platform, source_chat_id, source_message_id, created_at)
            VALUES (?, 'telegram', 10, 20, ?)
            """,
            (pair_id, to_db_time(NOW)),
        )


def test_foreign_keys_are_enforced(repos: Repositories) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        repos.messages.add(
            pair_id=999,
            source_platform=Platform.TELEGRAM,
            source_chat_id=1,
            source_message_id=1,
            created_at=NOW,
        )
    with pytest.raises(sqlite3.IntegrityError):
        repos.outbox.enqueue(
            pair_id=999,
            payload={"text": "hi"},
            next_attempt_at=NOW,
            created_at=NOW,
        )


def test_deleting_a_user_cascades_to_pairs(repos: Repositories) -> None:
    first = repos.users.create(
        platform=Platform.BALE,
        platform_user_id=1,
        role=UserRole.USER,
        status=UserStatus.ACTIVE,
        created_at=NOW,
    )
    second = repos.users.create(
        platform=Platform.BALE,
        platform_user_id=2,
        role=UserRole.USER,
        status=UserStatus.ACTIVE,
        created_at=NOW,
    )
    repos.pairs.create(
        user_a_id=first.id, user_b_id=second.id, status=PairingStatus.ACTIVE, created_at=NOW
    )

    sql(repos, "DELETE FROM users WHERE id = ?", (first.id,))

    assert sql(repos, "SELECT COUNT(*) AS total FROM pairs").fetchone()["total"] == 0


def test_outbox_status_is_constrained(two_pairs: tuple[int, int], repos: Repositories) -> None:
    pair_id, _ = two_pairs

    with pytest.raises(sqlite3.IntegrityError):
        sql(
            repos,
            f"INSERT INTO outbox {OUTBOX_COLUMNS} VALUES {OUTBOX_PLACEHOLDERS}",
            (pair_id, "{}", 0, to_db_time(NOW), "unknown", to_db_time(NOW)),
        )
    with pytest.raises(sqlite3.IntegrityError):
        sql(
            repos,
            f"INSERT INTO outbox {OUTBOX_COLUMNS} VALUES {OUTBOX_PLACEHOLDERS}",
            (pair_id, "{}", -1, to_db_time(NOW), "pending", to_db_time(NOW)),
        )


# -------------------------------------------------------------- repositories


def test_user_repository_remembers_the_chat_id(repos: Repositories) -> None:
    created = repos.users.create(
        platform=Platform.BALE,
        platform_user_id=51,
        role=UserRole.USER,
        status=UserStatus.ACTIVE,
        created_at=NOW,
    )
    assert created.last_chat_id is None

    updated = repos.users.set_last_chat_id(created.id, 4242)

    assert updated is not None and updated.last_chat_id == 4242
    assert repos.users.get_by_id(created.id).last_chat_id == 4242
    assert repos.users.set_last_chat_id(999, 1) is None


def test_user_repository_aggregates(repos: Repositories) -> None:
    for index, (status, role) in enumerate(
        [
            (UserStatus.ACTIVE, UserRole.USER),
            (UserStatus.ACTIVE, UserRole.OWNER),
            (UserStatus.BANNED, UserRole.USER),
        ],
        start=1,
    ):
        repos.users.create(
            platform=Platform.TELEGRAM,
            platform_user_id=index,
            role=role,
            status=status,
            created_at=NOW,
        )

    counts = repos.users.count_by_status()
    assert counts[UserStatus.ACTIVE] == 2
    assert counts[UserStatus.BANNED] == 1
    assert UserStatus.ACTIVE in counts and UserStatus.BANNED in counts
    assert repos.users.count_by_role(UserRole.OWNER) == 1
    assert repos.users.count_by_role(UserRole.USER) == 2

    recent = repos.users.list_recent(2)
    assert [user.platform_user_id for user in recent] == [3, 2]
    with pytest.raises(ValueError):
        repos.users.list_recent(0)


def test_pairing_repositories_expose_aggregates(with_users: Repositories) -> None:
    assert with_users.pairs.count_active() == 0
    assert with_users.requests.count_pending() == 0

    pair = with_users.pairs.create(
        user_a_id=1, user_b_id=2, status=PairingStatus.ACTIVE, created_at=NOW
    )
    with_users.requests.create(requester_id=3, target_id=4, created_at=NOW, expires_at=NOW)

    assert with_users.pairs.count_active() == 1
    assert with_users.requests.count_pending() == 1

    with_users.pairs.set_status(pair.id, PairingStatus.UNPAIRED, ended_at=NOW)
    assert with_users.pairs.count_active() == 0


# ---------------------------------------------------------- pairing invites


def test_pairing_invite_repository_lifecycle(with_users: Repositories) -> None:
    invite = with_users.invites.create(
        inviter_id=1,
        target_platform=Platform.BALE,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=15),
    )

    assert len(invite.token) >= 20
    assert invite.target_platform is Platform.BALE
    assert with_users.invites.get_by_token(invite.token) == invite

    assert with_users.invites.invalidate_for_inviter(1, NOW + timedelta(minutes=1)) == 1
    invalidated = with_users.invites.get_by_token(invite.token)
    assert invalidated is not None
    assert invalidated.expires_at == NOW + timedelta(minutes=1)

    used = with_users.invites.mark_used(
        invite.id, used_by_id=2, used_at=NOW + timedelta(minutes=2)
    )
    assert used is not None and used.used_by_id == 2
    assert with_users.invites.mark_used(
        invite.id, used_by_id=3, used_at=NOW + timedelta(minutes=3)
    ) is None


def test_pairing_invite_tokens_are_unique(with_users: Repositories) -> None:
    tokens = {
        with_users.invites.create(
            inviter_id=1,
            target_platform=Platform.BALE,
            created_at=NOW,
            expires_at=NOW + timedelta(minutes=15),
        ).token
        for _ in range(20)
    }

    assert len(tokens) == 20


# ------------------------------------------------------------- poll offsets


def test_poll_offset_repository_upsert_and_read(repos: Repositories) -> None:
    assert repos.poll_offsets.get(Platform.TELEGRAM) is None

    assert repos.poll_offsets.set(Platform.TELEGRAM, 10) == 10
    assert repos.poll_offsets.get(Platform.TELEGRAM) == 10
    assert repos.poll_offsets.set(Platform.TELEGRAM, 42) == 42
    assert repos.poll_offsets.get(Platform.TELEGRAM) == 42

    # Platforms are independent.
    assert repos.poll_offsets.get(Platform.BALE) is None
    repos.poll_offsets.set(Platform.BALE, 7)
    assert repos.poll_offsets.get(Platform.TELEGRAM) == 42
    assert repos.poll_offsets.get(Platform.BALE) == 7

    rows = sql(repos, "SELECT platform, offset FROM poll_offsets ORDER BY platform").fetchall()
    assert [(row["platform"], row["offset"]) for row in rows] == [("bale", 7), ("telegram", 42)]


def test_poll_offset_rejects_negative_values(repos: Repositories) -> None:
    with pytest.raises(ValueError):
        repos.poll_offsets.set(Platform.TELEGRAM, -1)


def test_poll_offset_survives_a_reopen(tmp_path: Path) -> None:
    path = tmp_path / "offsets.db"
    first = Repositories.open(path)
    first.poll_offsets.set(Platform.BALE, 123)
    first.close()

    second = Repositories.open(path)
    try:
        assert second.poll_offsets.get(Platform.BALE) == 123
    finally:
        second.close()


def test_poll_offsets_table_rejects_unknown_platform(repos: Repositories) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        sql(
            repos,
            "INSERT INTO poll_offsets (platform, offset, updated_at) VALUES ('whatsapp', 1, ?)",
            (to_db_time(NOW),),
        )


def test_user_repository_crud(repos: Repositories) -> None:
    created = repos.users.create(
        platform=Platform.BALE,
        platform_user_id=42,
        role=UserRole.USER,
        status=UserStatus.ACTIVE,
        created_at=NOW,
    )

    assert repos.users.get_by_id(created.id) == created
    assert repos.users.get_by_platform(Platform.BALE, 42) == created
    assert repos.users.get_by_id(999) is None
    assert repos.users.set_role(created.id, UserRole.OWNER).is_owner
    assert repos.users.set_status(created.id, UserStatus.BANNED).is_banned
    assert repos.users.count() == 1
    assert repos.users.set_role(999, UserRole.OWNER) is None
    assert repos.users.set_status(999, UserStatus.BANNED) is None


def test_pair_repository_scoped_queries(with_users: Repositories) -> None:
    pair = with_users.pairs.create(
        user_a_id=5, user_b_id=3, status=PairingStatus.ACTIVE, created_at=NOW
    )

    assert pair.user_a_id == 3 and pair.user_b_id == 5
    assert with_users.pairs.get(pair.id) == pair
    assert with_users.pairs.get_for_user(pair.id, 3) == pair
    assert with_users.pairs.get_for_user(pair.id, 5) == pair
    assert with_users.pairs.get_for_user(pair.id, 99) is None
    assert with_users.pairs.find_active_between(5, 3) == pair
    assert with_users.pairs.find_active_between(3, 3) is None
    assert with_users.pairs.list_active_for_user(5) == (pair,)
    assert with_users.pairs.count_active_for_user(5) == 1
    assert with_users.pairs.count_active_for_user(99) == 0
    assert with_users.pairs.set_status(999, PairingStatus.UNPAIRED) is None


def test_pair_repository_ended_pairs_are_not_active(with_users: Repositories) -> None:
    pair = with_users.pairs.create(
        user_a_id=1, user_b_id=2, status=PairingStatus.ACTIVE, created_at=NOW
    )
    ended = with_users.pairs.set_status(pair.id, PairingStatus.UNPAIRED, ended_at=NOW)

    assert ended.status is PairingStatus.UNPAIRED
    assert ended.ended_at == NOW
    assert with_users.pairs.count_active_for_user(1) == 0
    assert with_users.pairs.find_active_between(1, 2) is None


def test_pair_request_repository_scoped_queries(with_users: Repositories) -> None:
    request = with_users.requests.create(
        requester_id=7, target_id=8, created_at=NOW, expires_at=NOW + timedelta(hours=1)
    )

    assert with_users.requests.get_for_user(request.id, 7) == request
    assert with_users.requests.get_for_user(request.id, 8) == request
    assert with_users.requests.get_for_user(request.id, 9) is None
    assert with_users.requests.find_pending_between(8, 7) == request
    assert with_users.requests.find_pending_between(7, 7) is None
    assert with_users.requests.list_pending_incoming(8) == (request,)
    assert with_users.requests.list_pending_outgoing(7) == (request,)
    assert with_users.requests.list_pending_incoming(7) == ()
    assert with_users.requests.get(999) is None


def test_request_status_change_keeps_pair_id(with_users: Repositories) -> None:
    pair = with_users.pairs.create(
        user_a_id=1, user_b_id=2, status=PairingStatus.ACTIVE, created_at=NOW
    )
    request = with_users.requests.create(
        requester_id=1, target_id=2, created_at=NOW, expires_at=NOW
    )

    linked = with_users.requests.set_status(request.id, PairingStatus.ACTIVE, pair_id=pair.id)
    assert linked.pair_id == pair.id

    unchanged = with_users.requests.set_status(request.id, PairingStatus.UNPAIRED)
    assert unchanged.pair_id == pair.id
    assert unchanged.status is PairingStatus.UNPAIRED
    assert with_users.requests.set_status(999, PairingStatus.CANCELLED) is None


def test_expire_overdue_only_touches_pending_rows(with_users: Repositories) -> None:
    overdue = with_users.requests.create(
        requester_id=1, target_id=2, created_at=NOW, expires_at=NOW - timedelta(hours=1)
    )
    fresh = with_users.requests.create(
        requester_id=3, target_id=4, created_at=NOW, expires_at=NOW + timedelta(hours=1)
    )
    answered = with_users.requests.create(
        requester_id=5, target_id=6, created_at=NOW, expires_at=NOW - timedelta(hours=1)
    )
    with_users.requests.set_status(answered.id, PairingStatus.CANCELLED)

    assert with_users.requests.expire_overdue(NOW) == 1
    assert with_users.requests.get(overdue.id).status is PairingStatus.EXPIRED
    assert with_users.requests.get(fresh.id).status is PairingStatus.PENDING
    assert with_users.requests.get(answered.id).status is PairingStatus.CANCELLED
    assert with_users.requests.expire_overdue(NOW) == 0


def test_message_repository_add_is_idempotent_for_same_pair(
    two_pairs: tuple[int, int], repos: Repositories
) -> None:
    pair_id, _ = two_pairs
    first, created_first = repos.messages.add(
        pair_id=pair_id,
        source_platform=Platform.TELEGRAM,
        source_chat_id=10,
        source_message_id=11,
        created_at=NOW,
    )
    again, created_again = repos.messages.add(
        pair_id=pair_id,
        source_platform=Platform.TELEGRAM,
        source_chat_id=10,
        source_message_id=11,
        created_at=NOW + timedelta(minutes=5),
    )

    assert created_first is True
    assert created_again is False
    assert again == first
    assert again.target_message_id is None


def test_message_repository_conflict_across_pairs_is_rejected(
    two_pairs: tuple[int, int], repos: Repositories
) -> None:
    first_pair, second_pair = two_pairs
    repos.messages.add(
        pair_id=first_pair,
        source_platform=Platform.BALE,
        source_chat_id=5,
        source_message_id=6,
        created_at=NOW,
    )

    with pytest.raises(DuplicateMessageError) as excinfo:
        repos.messages.add(
            pair_id=second_pair,
            source_platform=Platform.BALE,
            source_chat_id=5,
            source_message_id=6,
            created_at=NOW,
        )

    assert excinfo.value.code == "duplicate_message"
    assert repos.messages.list_for_pair(second_pair) == ()


def test_message_repository_attach_and_list(
    two_pairs: tuple[int, int], repos: Repositories
) -> None:
    pair_id, _ = two_pairs
    records = [
        repos.messages.add(
            pair_id=pair_id,
            source_platform=Platform.TELEGRAM,
            source_chat_id=1,
            source_message_id=index,
            created_at=NOW + timedelta(seconds=index),
        )[0]
        for index in range(1, 6)
    ]

    attached = repos.messages.attach_target_message(pair_id, records[0].id, 555)
    assert attached.target_message_id == 555
    assert repos.messages.attach_target_message(pair_id, 4242, 1) is None

    assert [record.id for record in repos.messages.list_for_pair(pair_id)] == [
        record.id for record in records
    ]
    assert [record.id for record in repos.messages.list_for_pair(pair_id, limit=2)] == [
        records[0].id,
        records[1].id,
    ]
    assert [
        record.id for record in repos.messages.list_for_pair(pair_id, after_id=records[2].id)
    ] == [records[3].id, records[4].id]


def test_message_repository_validates_limit(repos: Repositories) -> None:
    with pytest.raises(ValueError):
        repos.messages.list_for_pair(1, limit=0)


def test_outbox_repository_lifecycle(two_pairs: tuple[int, int], repos: Repositories) -> None:
    pair_id, _ = two_pairs
    soon = repos.outbox.enqueue(
        pair_id=pair_id, payload={"text": "first"}, next_attempt_at=NOW, created_at=NOW
    )
    later = repos.outbox.enqueue(
        pair_id=pair_id,
        payload={"text": "second"},
        next_attempt_at=NOW + timedelta(hours=1),
        created_at=NOW,
    )

    assert soon.status is OutboxStatus.PENDING
    assert soon.attempts == 0
    assert soon.payload == {"text": "first"}
    assert [entry.id for entry in repos.outbox.list_due(NOW)] == [soon.id]
    assert [entry.id for entry in repos.outbox.list_due(NOW + timedelta(hours=2))] == [
        soon.id,
        later.id,
    ]

    retried = repos.outbox.mark_failed(
        pair_id=pair_id,
        entry_id=soon.id,
        error="boom",
        next_attempt_at=NOW + timedelta(minutes=1),
    )
    assert retried.attempts == 1
    assert retried.status is OutboxStatus.PENDING
    assert retried.last_error == "boom"

    dead = repos.outbox.mark_failed(
        pair_id=pair_id,
        entry_id=soon.id,
        error="boom again",
        next_attempt_at=NOW + timedelta(minutes=5),
        terminal=True,
    )
    assert dead.attempts == 2
    assert dead.status is OutboxStatus.FAILED
    assert [entry.id for entry in repos.outbox.list_due(NOW + timedelta(hours=2))] == [later.id]

    sent = repos.outbox.mark_sent(pair_id, later.id)
    assert sent.status is OutboxStatus.SENT
    assert sent.last_error is None
    assert repos.outbox.list_for_pair(pair_id, status=OutboxStatus.SENT) == (sent,)
    assert repos.outbox.list_for_pair(pair_id, status=OutboxStatus.FAILED) == (dead,)
    assert repos.outbox.list_for_pair(pair_id, status=OutboxStatus.PENDING) == ()


def test_mark_sent_scrubs_the_delivered_payload(
    two_pairs: tuple[int, int], repos: Repositories
) -> None:
    pair_id, _ = two_pairs
    entry = repos.outbox.enqueue(
        pair_id=pair_id,
        payload={"kind": "relay_text", "text": "محتوای حساس"},
        next_attempt_at=NOW,
        created_at=NOW,
    )

    sent = repos.outbox.mark_sent(pair_id, entry.id, sent_at=NOW)

    assert sent is not None
    assert sent.payload == {}, "content must not stay in the database"
    raw = sql(repos, "SELECT payload FROM outbox WHERE id = ?", (entry.id,)).fetchone()
    assert "محتوای حساس" not in raw["payload"]


def test_purge_sent_removes_only_old_tombstones(
    two_pairs: tuple[int, int], repos: Repositories
) -> None:
    pair_id, _ = two_pairs
    old = repos.outbox.enqueue(
        pair_id=pair_id, payload={"text": "old"}, next_attempt_at=NOW, created_at=NOW
    )
    recent = repos.outbox.enqueue(
        pair_id=pair_id, payload={"text": "recent"}, next_attempt_at=NOW, created_at=NOW
    )
    pending = repos.outbox.enqueue(
        pair_id=pair_id, payload={"text": "pending"}, next_attempt_at=NOW, created_at=NOW
    )
    repos.outbox.mark_sent(pair_id, old.id, sent_at=NOW - timedelta(days=10))
    repos.outbox.mark_sent(pair_id, recent.id, sent_at=NOW)

    assert repos.outbox.purge_sent(NOW - timedelta(days=7)) == 1
    assert repos.outbox.get_for_pair(pair_id, old.id) is None
    assert repos.outbox.get_for_pair(pair_id, recent.id) is not None
    assert repos.outbox.get_for_pair(pair_id, pending.id) is not None


def test_last_relay_survives_the_sent_purge(
    two_pairs: tuple[int, int], repos: Repositories
) -> None:
    pair_id, _ = two_pairs
    record, _ = repos.messages.add(
        pair_id=pair_id,
        source_platform=Platform.TELEGRAM,
        source_chat_id=1,
        source_message_id=1,
        created_at=NOW,
        target_message_id=99,
    )
    entry = repos.outbox.enqueue(
        pair_id=pair_id, payload={"text": "x"}, next_attempt_at=NOW, created_at=NOW
    )
    repos.outbox.mark_sent(pair_id, entry.id, sent_at=NOW)
    assert repos.outbox.purge_sent(NOW + timedelta(days=1)) == 1

    stats = repos.outbox.queue_stats(NOW)

    assert stats.last_relay_at == record.created_at


def test_outbox_repository_validates_limit(repos: Repositories) -> None:
    with pytest.raises(ValueError):
        repos.outbox.list_due(NOW, limit=0)


# --------------------------------------------------------------- transaction


def test_transaction_commits_on_success(repos: Repositories) -> None:
    with repos.db.transaction():
        insert_user_row(repos, platform_user_id=1)

    assert repos.users.count() == 1


def test_transaction_rolls_back_on_exception(repos: Repositories) -> None:
    with pytest.raises(RuntimeError):
        with repos.db.transaction():
            insert_user_row(repos, platform_user_id=1)
            assert repos.db.connection.in_transaction
            raise RuntimeError("rollback please")

    assert repos.users.count() == 0


def test_nested_transactions_are_rejected(repos: Repositories) -> None:
    with repos.db.transaction():
        with pytest.raises(RuntimeError):
            with repos.db.transaction():  # pragma: no cover - must not be reached
                pass


def test_autocommit_outside_transactions(repos: Repositories) -> None:
    insert_user_row(repos, platform_user_id=9)

    assert repos.db.connection.in_transaction is False
    assert repos.users.count() == 1
