"""Short, random, human-friendly public identifiers for users.

The database keeps a numeric primary key for foreign keys and joins. The id a
user sees (and types in ``/pair``, ``/ban``, …) is a separate random string.
Eight characters from a 32-symbol alphabet give ~40 bits of entropy, which
makes accidental guessing impractical without turning the id into a password:
knowing an id still does **not** authorise a pairing; the other side must
accept the request.

The alphabet omits ``0/O`` and ``1/I`` so ids are easy to read aloud and type
on a phone. The value is not a secret; it is only an anti-enumeration id.
"""

from __future__ import annotations

import secrets

from core.models import Platform

PUBLIC_ID_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
PUBLIC_ID_LENGTH = 8
PUBLIC_ID_MAX_LENGTH = 32
_PUBLIC_ID_CHARS = frozenset(PUBLIC_ID_ALPHABET)


def new_public_id(length: int = PUBLIC_ID_LENGTH) -> str:
    """Return a fresh random public id (uppercase, unambiguous alphabet)."""
    if length < 4:
        raise ValueError("public id length must be >= 4")
    return "".join(secrets.choice(PUBLIC_ID_ALPHABET) for _ in range(length))


def new_pair_token() -> str:
    """Return a URL-safe one-time token for a pairing deep link.

    Telegram's ``start`` payload allows ``A-Z a-z 0-9 _ -`` and at most 64
    characters; ``token_urlsafe(18)`` is 24 characters and fits comfortably.
    """
    return secrets.token_urlsafe(18)


def pair_deep_link(
    platform: Platform, bot_username: str, token: str
) -> str:
    """Build the official bot deep link for ``platform``.

    Telegram uses ``t.me/<bot>?start=...``; Bale documents the equivalent
    ``ble.ir/<bot>?start=...`` link.
    """
    username = bot_username.strip().lstrip("@")
    if not username:
        raise ValueError("bot username must not be blank")
    if platform is Platform.TELEGRAM:
        return f"https://t.me/{username}?start={token}"
    return f"https://ble.ir/{username}?start={token}"


def normalise_public_id(value: str) -> str | None:
    """Normalise user input, returning ``None`` for an invalid id.

    Input is case-insensitive and surrounding whitespace is ignored. Only the
    unambiguous uppercase alphabet is accepted, which keeps lookups strict and
    avoids confusing ``O`` with ``0``.
    """
    candidate = value.strip().upper()
    if not 4 <= len(candidate) <= PUBLIC_ID_MAX_LENGTH:
        return None
    if any(char not in _PUBLIC_ID_CHARS for char in candidate):
        return None
    return candidate


__all__ = [
    "PUBLIC_ID_ALPHABET",
    "PUBLIC_ID_LENGTH",
    "PUBLIC_ID_MAX_LENGTH",
    "new_pair_token",
    "new_public_id",
    "normalise_public_id",
    "pair_deep_link",
]
