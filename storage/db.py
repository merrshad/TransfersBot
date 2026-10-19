"""SQLite persistence: a single connection, versioned migrations, repositories.

Design notes
------------
* One :class:`Database` owns one ``sqlite3`` connection. All repositories built
  from it share that connection, which is what makes
  ``with db.transaction():`` cover every write of a service method atomically.
* Every read query is constrained by ``pair_id`` or ``user_id`` (see the
  repository signatures). The only deliberate exceptions are
  :meth:`OutboxRepository.list_due` and :meth:`OutboxRepository.claim_due`, the
  worker's queue poll/lease operations.
* Timestamps are stored as UTC ISO-8601 text, which sorts chronologically and
  round-trips exactly.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, Literal

from core.errors import DuplicateMessageError
from core.ids import new_public_id
from core.models import MessageRecord, OutboxEntry, OutboxStats, OutboxStatus, Platform
from core.pairing import Pair, PairingStatus, PairRequest
from core.users import User, UserRole, UserStatus

# ------------------------------------------------------------------ time utils


def utc_now() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def to_db_time(moment: datetime) -> str:
    """Serialise a timezone-aware datetime for storage (UTC ISO-8601)."""
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return moment.astimezone(UTC).isoformat(timespec="microseconds")


def from_db_time(value: str) -> datetime:
    """Parse a stored timestamp back into a timezone-aware UTC datetime."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


# ----------------------------------------------------------------- migrations


@dataclass(frozen=True, slots=True)
class Migration:
    """One forward-only schema migration."""

    version: int
    description: str
    statements: tuple[str, ...]


MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version=1,
        description="initial schema: users, pairs, pair_requests, messages, outbox",
        statements=(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                platform_user_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL,
                CONSTRAINT uq_users_platform_identity UNIQUE (platform, platform_user_id),
                CONSTRAINT ck_users_platform CHECK (platform IN ('telegram', 'bale')),
                CONSTRAINT ck_users_status CHECK (status IN ('active', 'banned')),
                CONSTRAINT ck_users_role CHECK (role IN ('owner', 'user'))
            )
            """,
            """
            CREATE TABLE pairs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_a_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
                user_b_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                ended_at TEXT,
                CONSTRAINT ck_pairs_order CHECK (user_a_id < user_b_id),
                CONSTRAINT ck_pairs_status CHECK (status IN ('active', 'unpaired')),
                CONSTRAINT ck_pairs_ended CHECK ((status = 'unpaired') = (ended_at IS NOT NULL))
            )
            """,
            """
            CREATE UNIQUE INDEX ux_pairs_active_members
                ON pairs (user_a_id, user_b_id)
                WHERE status = 'active'
            """,
            "CREATE INDEX ix_pairs_user_a ON pairs (user_a_id, status)",
            "CREATE INDEX ix_pairs_user_b ON pairs (user_b_id, status)",
            """
            CREATE TABLE pair_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                requester_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
                target_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                pair_id INTEGER REFERENCES pairs (id) ON DELETE SET NULL,
                CONSTRAINT ck_pair_requests_distinct CHECK (requester_id <> target_id),
                CONSTRAINT ck_pair_requests_status CHECK (
                    status IN ('pending', 'active', 'rejected', 'expired', 'cancelled', 'unpaired')
                ),
                CONSTRAINT ck_pair_requests_pair_active CHECK (
                    (status = 'active') <= (pair_id IS NOT NULL)
                )
            )
            """,
            """
            CREATE UNIQUE INDEX ux_pair_requests_pending_members
                ON pair_requests (min(requester_id, target_id), max(requester_id, target_id))
                WHERE status = 'pending'
            """,
            "CREATE INDEX ix_pair_requests_target ON pair_requests (target_id, status)",
            "CREATE INDEX ix_pair_requests_requester ON pair_requests (requester_id, status)",
            "CREATE INDEX ix_pair_requests_expiry ON pair_requests (status, expires_at)",
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_id INTEGER NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
                source_platform TEXT NOT NULL,
                source_chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                target_message_id INTEGER,
                created_at TEXT NOT NULL,
                CONSTRAINT ck_messages_platform CHECK (source_platform IN ('telegram', 'bale'))
            )
            """,
            """
            CREATE UNIQUE INDEX ux_messages_source
                ON messages (source_platform, source_chat_id, source_message_id)
            """,
            "CREATE INDEX ix_messages_pair ON messages (pair_id, id)",
            """
            CREATE TABLE outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_id INTEGER NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
                payload TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                last_error TEXT,
                CONSTRAINT ck_outbox_status CHECK (status IN ('pending', 'sent', 'failed')),
                CONSTRAINT ck_outbox_attempts CHECK (attempts >= 0)
            )
            """,
            "CREATE INDEX ix_outbox_pair ON outbox (pair_id, status)",
            "CREATE INDEX ix_outbox_due ON outbox (status, next_attempt_at)",
        ),
    ),
    Migration(
        version=2,
        description="poll_offsets table and users.last_chat_id",
        statements=(
            "ALTER TABLE users ADD COLUMN last_chat_id INTEGER",
            """
            CREATE TABLE poll_offsets (
                platform TEXT PRIMARY KEY,
                offset INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                CONSTRAINT ck_poll_offsets_platform CHECK (platform IN ('telegram', 'bale')),
                CONSTRAINT ck_poll_offsets_offset CHECK (offset >= 0)
            )
            """,
        ),
    ),
    Migration(
        version=3,
        description="outbox sending lease, delivery timestamps and error log",
        statements=(
            # SQLite cannot alter a CHECK constraint, so the table is rebuilt.
            # No other table references ``outbox`` at this point, which keeps the
            # drop safe even with foreign_keys=ON.
            """
            CREATE TABLE outbox_v3 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_id INTEGER NOT NULL REFERENCES pairs (id) ON DELETE CASCADE,
                payload TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                last_error TEXT,
                locked_at TEXT,
                sent_at TEXT,
                last_attempt_at TEXT,
                CONSTRAINT ck_outbox_status CHECK (
                    status IN ('pending', 'sending', 'sent', 'failed')
                ),
                CONSTRAINT ck_outbox_attempts CHECK (attempts >= 0)
            )
            """,
            """
            INSERT INTO outbox_v3
                (id, pair_id, payload, attempts, next_attempt_at, status,
                 created_at, last_error, locked_at, sent_at, last_attempt_at)
            SELECT
                id, pair_id, payload, attempts, next_attempt_at, status,
                created_at, last_error, NULL,
                CASE WHEN status = 'sent' THEN created_at ELSE NULL END,
                CASE WHEN attempts > 0 THEN created_at ELSE NULL END
            FROM outbox
            """,
            "DROP TABLE outbox",
            "ALTER TABLE outbox_v3 RENAME TO outbox",
            "CREATE INDEX ix_outbox_pair ON outbox (pair_id, status)",
            "CREATE INDEX ix_outbox_due ON outbox (status, next_attempt_at)",
            "CREATE INDEX ix_outbox_locked ON outbox (status, locked_at)",
            """
            CREATE TABLE outbox_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                outbox_id INTEGER NOT NULL REFERENCES outbox (id) ON DELETE CASCADE,
                occurred_at TEXT NOT NULL,
                error TEXT NOT NULL
            )
            """,
            "CREATE INDEX ix_outbox_errors_time ON outbox_errors (occurred_at)",
        ),
    ),
    Migration(
        version=4,
        description="users.public_id: short random user-facing identifiers",
        statements=(
            # Nullable during the migration; Database.migrate() backfills every
            # existing row after the index exists. SQLite allows multiple NULLs
            # in a UNIQUE index, so the index can be created first.
            "ALTER TABLE users ADD COLUMN public_id TEXT",
            "CREATE UNIQUE INDEX ux_users_public_id ON users (public_id)",
        ),
    ),
)

SCHEMA_VERSION_TABLE = "schema_version"


def _last_row_id(cursor: sqlite3.Cursor) -> int:
    """Return the rowid of a successful INSERT, failing loudly otherwise."""
    row_id = cursor.lastrowid
    if row_id is None:  # pragma: no cover - defensive
        raise RuntimeError("insert did not produce a row id")
    return int(row_id)


# ------------------------------------------------------------------- database


class _Transaction(AbstractContextManager[None]):
    """Explicit ``BEGIN``/``COMMIT``/``ROLLBACK`` for autocommit connections.

    Nesting is rejected loudly: services must not call each other inside an open
    transaction, and a silent no-op commit here would hide data loss.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def __enter__(self) -> None:
        if self._connection.in_transaction:
            raise RuntimeError("nested transactions are not supported")
        self._connection.execute("BEGIN IMMEDIATE")
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        if exc_type is None:
            self._connection.execute("COMMIT")
        else:
            self._connection.execute("ROLLBACK")
        return False


class Database:
    """SQLite connection wrapper owning migrations and transactions.

    Args:
        path: Database file path, or ``":memory:"`` for ephemeral databases.
            Missing parent directories are created automatically.
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._path = str(path)
        if self._path != ":memory:":
            Path(self._path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self._path, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        if self._path != ":memory:":
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = NORMAL")
        self.migrate()

    @property
    def path(self) -> str:
        """Filesystem path of the database (or ``":memory:"``)."""
        return self._path

    @property
    def connection(self) -> sqlite3.Connection:
        """Underlying connection, exposed for repository implementations."""
        return self._connection

    def close(self) -> None:
        """Close the connection."""
        self._connection.close()

    def transaction(self) -> AbstractContextManager[None]:
        """Return a transaction context manager (see :class:`_Transaction`)."""
        return _Transaction(self._connection)

    def schema_version(self) -> int:
        """Highest applied migration version (``0`` for an empty database)."""
        row = self._connection.execute(
            f"SELECT MAX(version) AS version FROM {SCHEMA_VERSION_TABLE}"
        ).fetchone()
        if row is None or row["version"] is None:
            return 0
        return int(row["version"])

    def applied_versions(self) -> tuple[int, ...]:
        """All applied migration versions, ascending."""
        rows = self._connection.execute(
            f"SELECT version FROM {SCHEMA_VERSION_TABLE} ORDER BY version"
        ).fetchall()
        return tuple(int(row["version"]) for row in rows)

    def migrate(self) -> int:
        """Apply every pending migration and return the resulting version."""
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_VERSION_TABLE} (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """
        )
        applied = set(self.applied_versions())
        for migration in MIGRATIONS:
            if migration.version in applied:
                continue
            with self.transaction():
                for statement in migration.statements:
                    self._connection.execute(statement)
                self._connection.execute(
                    f"INSERT INTO {SCHEMA_VERSION_TABLE} (version, applied_at) VALUES (?, ?)",
                    (migration.version, to_db_time(utc_now())),
                )
        self._backfill_public_ids()
        return self.schema_version()

    def _backfill_public_ids(self) -> None:
        """Give every legacy user a random public id.

        Runs on every open (cheap when the table is already backfilled). The
        UNIQUE index makes a collision observable as ``IntegrityError``; we
        retry with a fresh id instead of failing the whole startup.
        """
        try:
            rows = self._connection.execute(
                "SELECT id FROM users WHERE public_id IS NULL OR public_id = ''"
            ).fetchall()
        except sqlite3.OperationalError:  # pragma: no cover - pre-v4 database
            return
        for row in rows:
            user_id = int(row["id"])
            for _ in range(10):
                try:
                    self._connection.execute(
                        "UPDATE users SET public_id = ? WHERE id = ?",
                        (new_public_id(), user_id),
                    )
                except sqlite3.IntegrityError:
                    continue
                break
            else:  # pragma: no cover - 40 bits, only pathological collisions
                raise RuntimeError("could not allocate a unique public_id")


# --------------------------------------------------------------- row mappers


def _to_user(row: sqlite3.Row) -> User:
    last_chat_id = row["last_chat_id"]
    return User(
        id=int(row["id"]),
        public_id=str(row["public_id"] or ""),
        platform=Platform(row["platform"]),
        platform_user_id=int(row["platform_user_id"]),
        role=UserRole(row["role"]),
        status=UserStatus(row["status"]),
        created_at=from_db_time(row["created_at"]),
        last_chat_id=int(last_chat_id) if last_chat_id is not None else None,
    )


def _to_pair(row: sqlite3.Row) -> Pair:
    ended_at = row["ended_at"]
    return Pair(
        id=int(row["id"]),
        user_a_id=int(row["user_a_id"]),
        user_b_id=int(row["user_b_id"]),
        status=PairingStatus(row["status"]),
        created_at=from_db_time(row["created_at"]),
        ended_at=from_db_time(ended_at) if ended_at is not None else None,
    )


def _to_pair_request(row: sqlite3.Row) -> PairRequest:
    pair_id = row["pair_id"]
    return PairRequest(
        id=int(row["id"]),
        requester_id=int(row["requester_id"]),
        target_id=int(row["target_id"]),
        status=PairingStatus(row["status"]),
        created_at=from_db_time(row["created_at"]),
        expires_at=from_db_time(row["expires_at"]),
        pair_id=int(pair_id) if pair_id is not None else None,
    )


def _to_message(row: sqlite3.Row) -> MessageRecord:
    target_message_id = row["target_message_id"]
    return MessageRecord(
        id=int(row["id"]),
        pair_id=int(row["pair_id"]),
        source_platform=Platform(row["source_platform"]),
        source_chat_id=int(row["source_chat_id"]),
        source_message_id=int(row["source_message_id"]),
        target_message_id=int(target_message_id) if target_message_id is not None else None,
        created_at=from_db_time(row["created_at"]),
    )


def _to_outbox_entry(row: sqlite3.Row) -> OutboxEntry:
    last_error = row["last_error"]
    locked_at = row["locked_at"]
    sent_at = row["sent_at"]
    last_attempt_at = row["last_attempt_at"]
    return OutboxEntry(
        id=int(row["id"]),
        pair_id=int(row["pair_id"]),
        payload=json.loads(row["payload"]),
        attempts=int(row["attempts"]),
        next_attempt_at=from_db_time(row["next_attempt_at"]),
        status=OutboxStatus(row["status"]),
        created_at=from_db_time(row["created_at"]),
        last_error=str(last_error) if last_error is not None else None,
        locked_at=from_db_time(locked_at) if locked_at is not None else None,
        sent_at=from_db_time(sent_at) if sent_at is not None else None,
        last_attempt_at=(
            from_db_time(last_attempt_at) if last_attempt_at is not None else None
        ),
    )


# --------------------------------------------------------------------- users


class UserRepository:
    """SQLite implementation of :class:`core.users.UserRepositoryPort`."""

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def _connection(self) -> sqlite3.Connection:
        return self._db.connection

    def get_by_id(self, user_id: int) -> User | None:
        """Fetch a user by internal id."""
        row = self._connection.execute(
            "SELECT * FROM users WHERE id = ?", (user_id,)
        ).fetchone()
        return _to_user(row) if row is not None else None

    def get_by_public_id(self, public_id: str) -> User | None:
        """Fetch a user by the short random id shown to users."""
        row = self._connection.execute(
            "SELECT * FROM users WHERE public_id = ?", (public_id,)
        ).fetchone()
        return _to_user(row) if row is not None else None

    def get_by_platform(self, platform: Platform, platform_user_id: int) -> User | None:
        """Fetch a user by their platform identity."""
        row = self._connection.execute(
            "SELECT * FROM users WHERE platform = ? AND platform_user_id = ?",
            (platform.value, platform_user_id),
        ).fetchone()
        return _to_user(row) if row is not None else None

    def create(
        self,
        *,
        platform: Platform,
        platform_user_id: int,
        role: UserRole,
        status: UserStatus,
        created_at: datetime,
        public_id: str | None = None,
    ) -> User:
        """Insert a new user, generating a short random public id if needed."""
        for _ in range(10):
            candidate = public_id or new_public_id()
            try:
                cursor = self._connection.execute(
                    """
                    INSERT INTO users
                        (public_id, platform, platform_user_id, status, role, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate,
                        platform.value,
                        platform_user_id,
                        status.value,
                        role.value,
                        to_db_time(created_at),
                    ),
                )
            except sqlite3.IntegrityError:
                if self.get_by_platform(platform, platform_user_id) is not None:
                    raise
                if public_id is not None:
                    raise
                if self.get_by_public_id(candidate) is None:
                    raise
                continue
            created = self.get_by_id(_last_row_id(cursor))
            if created is None:  # pragma: no cover - insert just succeeded
                raise RuntimeError("user row disappeared right after insert")
            return created
        raise RuntimeError("could not allocate a unique public_id")  # pragma: no cover

    def set_role(self, user_id: int, role: UserRole) -> User | None:
        """Change a user's role; ``None`` when the user does not exist."""
        cursor = self._connection.execute(
            "UPDATE users SET role = ? WHERE id = ?", (role.value, user_id)
        )
        if cursor.rowcount == 0:
            return None
        return self.get_by_id(user_id)

    def set_status(self, user_id: int, status: UserStatus) -> User | None:
        """Change a user's status; ``None`` when the user does not exist."""
        cursor = self._connection.execute(
            "UPDATE users SET status = ? WHERE id = ?", (status.value, user_id)
        )
        if cursor.rowcount == 0:
            return None
        return self.get_by_id(user_id)

    def count(self) -> int:
        """Total number of registered users."""
        row = self._connection.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return int(row["total"]) if row is not None else 0

    def set_last_chat_id(self, user_id: int, chat_id: int) -> User | None:
        """Remember the private chat id observed for a user."""
        cursor = self._connection.execute(
            "UPDATE users SET last_chat_id = ? WHERE id = ?", (chat_id, user_id)
        )
        if cursor.rowcount == 0:
            return None
        return self.get_by_id(user_id)

    def count_by_status(self) -> Mapping[UserStatus, int]:
        """Registered users grouped by status (aggregate only, no rows)."""
        rows = self._connection.execute(
            "SELECT status, COUNT(*) AS total FROM users GROUP BY status"
        ).fetchall()
        return {UserStatus(row["status"]): int(row["total"]) for row in rows}

    def count_by_role(self, role: UserRole) -> int:
        """Number of users with ``role``."""
        row = self._connection.execute(
            "SELECT COUNT(*) AS total FROM users WHERE role = ?", (role.value,)
        ).fetchone()
        return int(row["total"]) if row is not None else 0

    def list_recent(self, limit: int) -> tuple[User, ...]:
        """Newest users first, at most ``limit`` rows."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        rows = self._connection.execute(
            "SELECT * FROM users ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return tuple(_to_user(row) for row in rows)


# --------------------------------------------------------------------- pairs


def _ordered(user_a_id: int, user_b_id: int) -> tuple[int, int]:
    """Canonical (smaller, larger) ordering used for pair rows."""
    return (user_a_id, user_b_id) if user_a_id < user_b_id else (user_b_id, user_a_id)


class PairRepository:
    """SQLite implementation of :class:`core.pairing.PairRepositoryPort`."""

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def _connection(self) -> sqlite3.Connection:
        return self._db.connection

    def get(self, pair_id: int) -> Pair | None:
        """Fetch a pair by id (internal use; prefer ``get_for_user``)."""
        row = self._connection.execute(
            "SELECT * FROM pairs WHERE id = ?", (pair_id,)
        ).fetchone()
        return _to_pair(row) if row is not None else None

    def get_for_user(self, pair_id: int, user_id: int) -> Pair | None:
        """Fetch a pair only if ``user_id`` is one of its two members."""
        row = self._connection.execute(
            """
            SELECT * FROM pairs
            WHERE id = ? AND (user_a_id = ? OR user_b_id = ?)
            """,
            (pair_id, user_id, user_id),
        ).fetchone()
        return _to_pair(row) if row is not None else None

    def create(
        self,
        *,
        user_a_id: int,
        user_b_id: int,
        status: PairingStatus,
        created_at: datetime,
    ) -> Pair:
        """Insert a pair row with canonical member ordering."""
        ordered_a, ordered_b = _ordered(user_a_id, user_b_id)
        cursor = self._connection.execute(
            """
            INSERT INTO pairs (user_a_id, user_b_id, status, created_at, ended_at)
            VALUES (?, ?, ?, ?, NULL)
            """,
            (ordered_a, ordered_b, status.value, to_db_time(created_at)),
        )
        created = self.get(_last_row_id(cursor))
        if created is None:  # pragma: no cover - insert just succeeded
            raise RuntimeError("pair row disappeared right after insert")
        return created

    def find_active_between(self, user_a_id: int, user_b_id: int) -> Pair | None:
        """The active pair between two users, if any."""
        if user_a_id == user_b_id:
            return None
        ordered_a, ordered_b = _ordered(user_a_id, user_b_id)
        row = self._connection.execute(
            """
            SELECT * FROM pairs
            WHERE status = 'active' AND user_a_id = ? AND user_b_id = ?
            """,
            (ordered_a, ordered_b),
        ).fetchone()
        return _to_pair(row) if row is not None else None

    def list_active_for_user(self, user_id: int) -> tuple[Pair, ...]:
        """All active pairs a user is a member of."""
        rows = self._connection.execute(
            """
            SELECT * FROM pairs
            WHERE status = 'active' AND (user_a_id = ? OR user_b_id = ?)
            ORDER BY id
            """,
            (user_id, user_id),
        ).fetchall()
        return tuple(_to_pair(row) for row in rows)

    def count_active_for_user(self, user_id: int) -> int:
        """Number of active pairs a user is a member of."""
        row = self._connection.execute(
            """
            SELECT COUNT(*) AS total FROM pairs
            WHERE status = 'active' AND (user_a_id = ? OR user_b_id = ?)
            """,
            (user_id, user_id),
        ).fetchone()
        return int(row["total"]) if row is not None else 0

    def count_active(self) -> int:
        """Total number of active pairs (aggregate only, no rows)."""
        row = self._connection.execute(
            "SELECT COUNT(*) AS total FROM pairs WHERE status = 'active'"
        ).fetchone()
        return int(row["total"]) if row is not None else 0

    def set_status(
        self, pair_id: int, status: PairingStatus, *, ended_at: datetime | None = None
    ) -> Pair | None:
        """Update a pair's status (and ``ended_at``); ``None`` if unknown."""
        cursor = self._connection.execute(
            "UPDATE pairs SET status = ?, ended_at = ? WHERE id = ?",
            (status.value, to_db_time(ended_at) if ended_at is not None else None, pair_id),
        )
        if cursor.rowcount == 0:
            return None
        return self.get(pair_id)


# ------------------------------------------------------------- pair requests


class PairRequestRepository:
    """SQLite implementation of :class:`core.pairing.PairRequestRepositoryPort`."""

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def _connection(self) -> sqlite3.Connection:
        return self._db.connection

    def get(self, request_id: int) -> PairRequest | None:
        """Fetch a request by id (internal use; prefer ``get_for_user``)."""
        row = self._connection.execute(
            "SELECT * FROM pair_requests WHERE id = ?", (request_id,)
        ).fetchone()
        return _to_pair_request(row) if row is not None else None

    def get_for_user(self, request_id: int, user_id: int) -> PairRequest | None:
        """Fetch a request only if the user sent or received it."""
        row = self._connection.execute(
            """
            SELECT * FROM pair_requests
            WHERE id = ? AND (requester_id = ? OR target_id = ?)
            """,
            (request_id, user_id, user_id),
        ).fetchone()
        return _to_pair_request(row) if row is not None else None

    def create(
        self,
        *,
        requester_id: int,
        target_id: int,
        created_at: datetime,
        expires_at: datetime,
    ) -> PairRequest:
        """Insert a pending request (status is always ``pending``)."""
        cursor = self._connection.execute(
            """
            INSERT INTO pair_requests
                (requester_id, target_id, status, created_at, expires_at, pair_id)
            VALUES (?, ?, 'pending', ?, ?, NULL)
            """,
            (
                requester_id,
                target_id,
                to_db_time(created_at),
                to_db_time(expires_at),
            ),
        )
        created = self.get(_last_row_id(cursor))
        if created is None:  # pragma: no cover - insert just succeeded
            raise RuntimeError("pair request row disappeared right after insert")
        return created

    def find_pending_between(self, user_a_id: int, user_b_id: int) -> PairRequest | None:
        """A pending request between two users, in either direction."""
        if user_a_id == user_b_id:
            return None
        row = self._connection.execute(
            """
            SELECT * FROM pair_requests
            WHERE status = 'pending'
              AND ((requester_id = ? AND target_id = ?)
                   OR (requester_id = ? AND target_id = ?))
            ORDER BY id
            LIMIT 1
            """,
            (user_a_id, user_b_id, user_b_id, user_a_id),
        ).fetchone()
        return _to_pair_request(row) if row is not None else None

    def list_pending_incoming(self, user_id: int) -> tuple[PairRequest, ...]:
        """Requests awaiting this user's answer, soonest expiry first."""
        rows = self._connection.execute(
            """
            SELECT * FROM pair_requests
            WHERE target_id = ? AND status = 'pending'
            ORDER BY expires_at, id
            """,
            (user_id,),
        ).fetchall()
        return tuple(_to_pair_request(row) for row in rows)

    def list_pending_outgoing(self, user_id: int) -> tuple[PairRequest, ...]:
        """Requests this user sent that are still pending."""
        rows = self._connection.execute(
            """
            SELECT * FROM pair_requests
            WHERE requester_id = ? AND status = 'pending'
            ORDER BY expires_at, id
            """,
            (user_id,),
        ).fetchall()
        return tuple(_to_pair_request(row) for row in rows)

    def set_status(
        self, request_id: int, status: PairingStatus, *, pair_id: int | None = None
    ) -> PairRequest | None:
        """Update a request's status; ``pair_id`` is kept when not provided."""
        cursor = self._connection.execute(
            """
            UPDATE pair_requests
            SET status = ?, pair_id = COALESCE(?, pair_id)
            WHERE id = ?
            """,
            (status.value, pair_id, request_id),
        )
        if cursor.rowcount == 0:
            return None
        return self.get(request_id)

    def count_pending(self) -> int:
        """Total number of pending requests (aggregate only, no rows)."""
        row = self._connection.execute(
            "SELECT COUNT(*) AS total FROM pair_requests WHERE status = 'pending'"
        ).fetchone()
        return int(row["total"]) if row is not None else 0

    def expire_overdue(self, now: datetime) -> int:
        """Bulk-expire pending requests whose TTL passed; returns rows changed."""
        cursor = self._connection.execute(
            """
            UPDATE pair_requests
            SET status = 'expired'
            WHERE status = 'pending' AND expires_at <= ?
            """,
            (to_db_time(now),),
        )
        return int(cursor.rowcount)


# ------------------------------------------------------------------ messages


class MessageRepository:
    """SQLite implementation of :class:`core.ports.MessageRepositoryPort`."""

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def _connection(self) -> sqlite3.Connection:
        return self._db.connection

    def add(
        self,
        *,
        pair_id: int,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
        created_at: datetime,
        target_message_id: int | None = None,
    ) -> tuple[MessageRecord, bool]:
        """Insert a message record.

        Idempotent for the same pair: re-inserting the same source message
        returns the existing row with ``created=False``.

        Raises:
            DuplicateMessageError: If the source message is already stored for a
                *different* pair, which would break pair isolation.
        """
        try:
            cursor = self._connection.execute(
                """
                INSERT INTO messages
                    (pair_id, source_platform, source_chat_id, source_message_id,
                     target_message_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    pair_id,
                    source_platform.value,
                    source_chat_id,
                    source_message_id,
                    target_message_id,
                    to_db_time(created_at),
                ),
            )
        except sqlite3.IntegrityError:
            existing = self._find_by_source(
                source_platform, source_chat_id, source_message_id
            )
            if existing is None:
                # Not a duplicate at all (e.g. unknown pair_id): surface the
                # original constraint violation instead of masking it.
                raise
            if existing.pair_id != pair_id:
                raise DuplicateMessageError(
                    "این پیام قبلاً برای جفت دیگری ثبت شده است."
                ) from None
            return existing, False
        created = self.get_for_pair(pair_id, _last_row_id(cursor))
        if created is None:  # pragma: no cover - insert just succeeded
            raise RuntimeError("message row disappeared right after insert")
        return created, True

    def get_for_pair(self, pair_id: int, message_id: int) -> MessageRecord | None:
        """Fetch a stored message by id, restricted to its pair."""
        row = self._connection.execute(
            "SELECT * FROM messages WHERE id = ? AND pair_id = ?",
            (message_id, pair_id),
        ).fetchone()
        return _to_message(row) if row is not None else None

    def _find_by_source(
        self,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
    ) -> MessageRecord | None:
        """Look up a message by its global natural key (constraint diagnosis)."""
        row = self._connection.execute(
            """
            SELECT * FROM messages
            WHERE source_platform = ? AND source_chat_id = ? AND source_message_id = ?
            """,
            (source_platform.value, source_chat_id, source_message_id),
        ).fetchone()
        return _to_message(row) if row is not None else None

    def find_by_source(
        self,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
    ) -> MessageRecord | None:
        """Look up a message by its global natural key (relay dedup only)."""
        return self._find_by_source(source_platform, source_chat_id, source_message_id)

    def find_by_target_in_pair(
        self,
        pair_id: int,
        *,
        source_platform: Platform,
        source_chat_id: int,
        target_message_id: int,
    ) -> MessageRecord | None:
        """Find the record whose bot-delivered id is ``target_message_id``.

        Scoped to the partner's source chat (see the port protocol): the record
        delivered into one member's chat is always the one sourced from the
        other member's chat within the same pair.
        """
        row = self._connection.execute(
            """
            SELECT * FROM messages
            WHERE pair_id = ? AND source_platform = ? AND source_chat_id = ?
              AND target_message_id = ?
            """,
            (pair_id, source_platform.value, source_chat_id, target_message_id),
        ).fetchone()
        return _to_message(row) if row is not None else None

    def get_by_source_for_pair(
        self,
        pair_id: int,
        source_platform: Platform,
        source_chat_id: int,
        source_message_id: int,
    ) -> MessageRecord | None:
        """Fetch a stored message by source key, restricted to its pair."""
        row = self._connection.execute(
            """
            SELECT * FROM messages
            WHERE pair_id = ? AND source_platform = ?
              AND source_chat_id = ? AND source_message_id = ?
            """,
            (pair_id, source_platform.value, source_chat_id, source_message_id),
        ).fetchone()
        return _to_message(row) if row is not None else None

    def attach_target_message(
        self, pair_id: int, message_id: int, target_message_id: int
    ) -> MessageRecord | None:
        """Store the id this message was delivered as on the target platform."""
        cursor = self._connection.execute(
            """
            UPDATE messages SET target_message_id = ?
            WHERE id = ? AND pair_id = ?
            """,
            (target_message_id, message_id, pair_id),
        )
        if cursor.rowcount == 0:
            return None
        return self.get_for_pair(pair_id, message_id)

    def list_for_pair(
        self, pair_id: int, *, limit: int = 50, after_id: int | None = None
    ) -> tuple[MessageRecord, ...]:
        """List a pair's messages oldest-first, optionally after ``after_id``."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if after_id is None:
            rows = self._connection.execute(
                "SELECT * FROM messages WHERE pair_id = ? ORDER BY id LIMIT ?",
                (pair_id, limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT * FROM messages
                WHERE pair_id = ? AND id > ?
                ORDER BY id
                LIMIT ?
                """,
                (pair_id, after_id, limit),
            ).fetchall()
        return tuple(_to_message(row) for row in rows)


# -------------------------------------------------------------------- outbox


class OutboxRepository:
    """SQLite implementation of :class:`core.ports.OutboxRepositoryPort`."""

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def _connection(self) -> sqlite3.Connection:
        return self._db.connection

    def enqueue(
        self,
        *,
        pair_id: int,
        payload: Mapping[str, Any],
        next_attempt_at: datetime,
        created_at: datetime,
    ) -> OutboxEntry:
        """Append a pending delivery for a pair."""
        cursor = self._connection.execute(
            """
            INSERT INTO outbox
                (pair_id, payload, attempts, next_attempt_at, status, created_at, last_error)
            VALUES (?, ?, 0, ?, 'pending', ?, NULL)
            """,
            (
                pair_id,
                json.dumps(dict(payload), ensure_ascii=False, sort_keys=True),
                to_db_time(next_attempt_at),
                to_db_time(created_at),
            ),
        )
        created = self.get_for_pair(pair_id, _last_row_id(cursor))
        if created is None:  # pragma: no cover - insert just succeeded
            raise RuntimeError("outbox row disappeared right after insert")
        return created

    def get_for_pair(self, pair_id: int, entry_id: int) -> OutboxEntry | None:
        """Fetch one outbox entry, restricted to its pair."""
        row = self._connection.execute(
            "SELECT * FROM outbox WHERE id = ? AND pair_id = ?",
            (entry_id, pair_id),
        ).fetchone()
        return _to_outbox_entry(row) if row is not None else None

    def list_for_pair(
        self, pair_id: int, *, status: OutboxStatus | None = None
    ) -> tuple[OutboxEntry, ...]:
        """List a pair's outbound entries oldest-first."""
        if status is None:
            rows = self._connection.execute(
                "SELECT * FROM outbox WHERE pair_id = ? ORDER BY id", (pair_id,)
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT * FROM outbox
                WHERE pair_id = ? AND status = ?
                ORDER BY id
                """,
                (pair_id, status.value),
            ).fetchall()
        return tuple(_to_outbox_entry(row) for row in rows)

    def list_due(self, now: datetime, *, limit: int = 100) -> tuple[OutboxEntry, ...]:
        """Pending entries ready for a delivery attempt (worker queue poll).

        This is the only query without a ``pair_id``/``user_id`` filter, because
        the delivery worker has no pair context. Callers must use it purely as a
        queue: never to display another pair's content.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        rows = self._connection.execute(
            """
            SELECT * FROM outbox
            WHERE status = 'pending' AND next_attempt_at <= ?
            ORDER BY next_attempt_at, id
            LIMIT ?
            """,
            (to_db_time(now), limit),
        ).fetchall()
        return tuple(_to_outbox_entry(row) for row in rows)

    def claim_due(self, now: datetime, *, limit: int = 100) -> tuple[OutboxEntry, ...]:
        """Atomically lease due entries by moving them ``pending → sending``.

        The lease is committed before any network call. A crash therefore leaves
        a ``sending`` row rather than a silently dropped message; the next worker
        start recovers it (see :meth:`recover_orphaned`). As with
        :meth:`list_due`, this is the worker's queue operation and its results
        must never be displayed.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        moment = to_db_time(now)
        with self._db.transaction():
            rows = self._connection.execute(
                """
                SELECT id FROM outbox
                WHERE status = 'pending' AND next_attempt_at <= ?
                ORDER BY next_attempt_at, id
                LIMIT ?
                """,
                (moment, limit),
            ).fetchall()
            if not rows:
                return ()
            ids = [int(row["id"]) for row in rows]
            placeholders = ",".join("?" for _ in ids)
            self._connection.execute(
                f"""
                UPDATE outbox
                SET status = 'sending', locked_at = ?
                WHERE id IN ({placeholders}) AND status = 'pending'
                """,
                (moment, *ids),
            )
            claimed = self._connection.execute(
                f"""
                SELECT * FROM outbox
                WHERE id IN ({placeholders}) AND status = 'sending'
                ORDER BY next_attempt_at, id
                """,
                tuple(ids),
            ).fetchall()
        return tuple(_to_outbox_entry(row) for row in claimed)

    def recover_orphaned(self, now: datetime | None = None) -> int:
        """Return every ``sending`` row to ``pending`` immediately.

        Called once when a worker starts: a single worker owns the queue, so any
        lease that exists at startup belongs to a process that died before it
        could finish the send. This is what makes a crash-and-restart replay the
        message exactly once *instead of losing it*; the platform itself offers
        no idempotency key, so a crash after the platform accepted a send but
        before ``mark_sent`` can still lead to a duplicate (documented
        at-least-once boundary).
        """
        moment = now or utc_now()
        cursor = self._connection.execute(
            """
            UPDATE outbox
            SET status = 'pending', locked_at = NULL, next_attempt_at = ?
            WHERE status = 'sending'
            """,
            (to_db_time(moment),),
        )
        return int(cursor.rowcount)

    def recover_stale(self, now: datetime, *, max_age: timedelta) -> int:
        """Return ``sending`` leases older than ``max_age`` to ``pending``.

        A safety net for a crashed delivery pass inside a long-running worker;
        normal restarts use :meth:`recover_orphaned` and need no grace period.
        """
        if max_age < timedelta(0):
            raise ValueError("max_age must be >= 0")
        cutoff = to_db_time(now - max_age)
        cursor = self._connection.execute(
            """
            UPDATE outbox
            SET status = 'pending', locked_at = NULL, next_attempt_at = ?
            WHERE status = 'sending' AND (locked_at IS NULL OR locked_at <= ?)
            """,
            (to_db_time(now), cutoff),
        )
        return int(cursor.rowcount)

    def mark_sent(
        self,
        pair_id: int,
        entry_id: int,
        *,
        sent_at: datetime | None = None,
        scrub_payload: bool = True,
    ) -> OutboxEntry | None:
        """Mark an entry as delivered and remember when it happened.

        With ``scrub_payload`` (the default) the JSON body is replaced by an
        empty object, so the delivered text/caption/file handle stops occupying
        space as soon as the platform accepts it. The row itself is kept for
        the owner's ``/status`` counters and is removed later by
        :meth:`purge_sent`.
        """
        moment = sent_at or utc_now()
        payload = "{}" if scrub_payload else None
        cursor = self._connection.execute(
            """
            UPDATE outbox
            SET status = 'sent',
                payload = COALESCE(?, payload),
                last_error = NULL,
                locked_at = NULL,
                sent_at = ?,
                last_attempt_at = ?
            WHERE id = ? AND pair_id = ?
            """,
            (payload, to_db_time(moment), to_db_time(moment), entry_id, pair_id),
        )
        if cursor.rowcount == 0:
            return None
        return self.get_for_pair(pair_id, entry_id)

    def purge_sent(self, before: datetime) -> int:
        """Delete ``sent`` rows older than ``before`` and return the count.

        Their ``outbox_errors`` rows are removed by the foreign key's cascade.
        Only content-free tombstones are deleted; pending/sending/failed rows
        are never touched.
        """
        cursor = self._connection.execute(
            """
            DELETE FROM outbox
            WHERE status = 'sent' AND sent_at IS NOT NULL AND sent_at < ?
            """,
            (to_db_time(before),),
        )
        return int(cursor.rowcount)

    def mark_failed(
        self,
        pair_id: int,
        entry_id: int,
        *,
        error: str,
        next_attempt_at: datetime,
        terminal: bool = False,
        now: datetime | None = None,
    ) -> OutboxEntry | None:
        """Record a failed attempt, incrementing ``attempts``.

        With ``terminal=True`` the entry moves to ``failed`` and stops retrying.
        Every failure is also appended to ``outbox_errors`` so the owner's
        ``/status`` can report a rolling 24-hour error count even after a later
        retry succeeds. The call is a no-op when the entry is unknown or belongs
        to another pair.
        """
        status = OutboxStatus.FAILED.value if terminal else OutboxStatus.PENDING.value
        moment = now or utc_now()
        cursor = self._connection.execute(
            """
            UPDATE outbox
            SET attempts = attempts + 1,
                last_error = ?,
                next_attempt_at = ?,
                status = ?,
                locked_at = NULL,
                last_attempt_at = ?
            WHERE id = ? AND pair_id = ?
            """,
            (
                error,
                to_db_time(next_attempt_at),
                status,
                to_db_time(moment),
                entry_id,
                pair_id,
            ),
        )
        if cursor.rowcount == 0:
            return None
        self._connection.execute(
            """
            INSERT INTO outbox_errors (outbox_id, occurred_at, error)
            VALUES (?, ?, ?)
            """,
            (entry_id, to_db_time(moment), error),
        )
        return self.get_for_pair(pair_id, entry_id)

    def defer(
        self,
        pair_id: int,
        entry_id: int,
        *,
        next_attempt_at: datetime,
        reason: str | None = None,
        now: datetime | None = None,
    ) -> OutboxEntry | None:
        """Release a lease without counting an attempt (rate limiting).

        The entry returns to ``pending`` and is due at ``next_attempt_at``. No
        ``outbox_errors`` row is written: being throttled is not a delivery
        failure, and it must not consume the retry budget.
        """
        cursor = self._connection.execute(
            """
            UPDATE outbox
            SET status = 'pending', locked_at = NULL, next_attempt_at = ?,
                last_error = COALESCE(?, last_error)
            WHERE id = ? AND pair_id = ?
            """,
            (
                to_db_time(next_attempt_at),
                reason,
                entry_id,
                pair_id,
            ),
        )
        if cursor.rowcount == 0:
            return None
        return self.get_for_pair(pair_id, entry_id)

    def queue_stats(self, now: datetime | None = None) -> OutboxStats:
        """Aggregate queue size, 24-hour errors and last successful relay."""
        moment = now or utc_now()
        cutoff = to_db_time(moment - timedelta(hours=24))
        rows = self._connection.execute(
            "SELECT status, COUNT(*) AS total FROM outbox GROUP BY status"
        ).fetchall()
        counts = {str(row["status"]): int(row["total"]) for row in rows}
        errors_row = self._connection.execute(
            "SELECT COUNT(*) AS total FROM outbox_errors WHERE occurred_at >= ?",
            (cutoff,),
        ).fetchone()
        # ``messages`` keeps the relay record (and its target id) even after the
        # outbox row is scrubbed/purged, so it is the durable source for the
        # "last relay" line. The outbox timestamp is only a fallback for
        # pre-existing rows that have no message record.
        last_message = self._connection.execute(
            """
            SELECT MAX(created_at) AS last_relay FROM messages
            WHERE target_message_id IS NOT NULL
            """
        ).fetchone()
        last_value = last_message["last_relay"] if last_message is not None else None
        if last_value is None:
            last_row = self._connection.execute(
                "SELECT MAX(sent_at) AS last_sent FROM outbox WHERE status = 'sent'"
            ).fetchone()
            last_value = last_row["last_sent"] if last_row is not None else None
        return OutboxStats(
            pending=counts.get(OutboxStatus.PENDING.value, 0),
            sending=counts.get(OutboxStatus.SENDING.value, 0),
            sent=counts.get(OutboxStatus.SENT.value, 0),
            failed=counts.get(OutboxStatus.FAILED.value, 0),
            errors_24h=int(errors_row["total"]) if errors_row is not None else 0,
            last_relay_at=from_db_time(last_value) if last_value else None,
        )


# -------------------------------------------------------------- poll offsets


class PollOffsetRepository:
    """Remembers the last confirmed ``getUpdates`` offset per platform.

    Long polling is stateless across restarts, so the offset is persisted after
    every processed batch; otherwise a restart would replay old updates and
    duplicate registration/pairing work.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def _connection(self) -> sqlite3.Connection:
        return self._db.connection

    def get(self, platform: Platform) -> int | None:
        """Return the stored offset, or ``None`` when nothing was processed yet."""
        row = self._connection.execute(
            "SELECT offset FROM poll_offsets WHERE platform = ?", (platform.value,)
        ).fetchone()
        return int(row["offset"]) if row is not None else None

    def set(
        self, platform: Platform, offset: int, *, updated_at: datetime | None = None
    ) -> int:
        """Upsert the confirmed offset and return it.

        Raises:
            ValueError: If ``offset`` is negative.
        """
        if offset < 0:
            raise ValueError("offset must be >= 0")
        moment = updated_at or utc_now()
        self._connection.execute(
            """
            INSERT INTO poll_offsets (platform, offset, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (platform) DO UPDATE
                SET offset = excluded.offset, updated_at = excluded.updated_at
            """,
            (platform.value, offset, to_db_time(moment)),
        )
        return offset


# --------------------------------------------------------------- container


@dataclass(frozen=True, slots=True)
class Repositories:
    """Bundle of repositories sharing one connection (and its transactions)."""

    db: Database
    users: UserRepository
    pairs: PairRepository
    requests: PairRequestRepository
    messages: MessageRepository
    outbox: OutboxRepository
    poll_offsets: PollOffsetRepository

    @classmethod
    def open(cls, path: str | Path = ":memory:") -> Repositories:
        """Open (and migrate) a database, returning all repositories."""
        database = Database(path)
        return cls(
            db=database,
            users=UserRepository(database),
            pairs=PairRepository(database),
            requests=PairRequestRepository(database),
            messages=MessageRepository(database),
            outbox=OutboxRepository(database),
            poll_offsets=PollOffsetRepository(database),
        )

    def close(self) -> None:
        """Close the underlying connection."""
        self.db.close()


def open_repositories(path: str | Path = ":memory:") -> Repositories:
    """Convenience wrapper around :meth:`Repositories.open`."""
    return Repositories.open(path)


__all__ = [
    "MIGRATIONS",
    "SCHEMA_VERSION_TABLE",
    "Database",
    "MessageRepository",
    "Migration",
    "OutboxRepository",
    "PairRepository",
    "PairRequestRepository",
    "PollOffsetRepository",
    "Repositories",
    "UserRepository",
    "from_db_time",
    "open_repositories",
    "to_db_time",
    "utc_now",
]
