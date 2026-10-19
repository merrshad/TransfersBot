"""Tests for short random public user ids."""

from __future__ import annotations

import pytest

from core.ids import (
    PUBLIC_ID_ALPHABET,
    PUBLIC_ID_LENGTH,
    new_public_id,
    normalise_public_id,
)


def test_new_public_id_uses_the_safe_alphabet_and_expected_length() -> None:
    for _ in range(50):
        value = new_public_id()

        assert len(value) == PUBLIC_ID_LENGTH
        assert all(char in PUBLIC_ID_ALPHABET for char in value)
        assert not set(value) & set("01OI")


def test_new_public_ids_are_practically_unique() -> None:
    values = {new_public_id() for _ in range(500)}

    assert len(values) == 500


def test_normalise_public_id_is_case_insensitive_and_trims_whitespace() -> None:
    value = new_public_id()

    assert normalise_public_id(f"  {value.lower()}  ") == value


@pytest.mark.parametrize(
    "value",
    ["", "ABC", "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890", "ABCD-EFGH", "0O1I"],
)
def test_invalid_public_ids_are_rejected(value: str) -> None:
    assert normalise_public_id(value) is None


def test_public_id_length_must_be_positive() -> None:
    with pytest.raises(ValueError):
        new_public_id(3)
