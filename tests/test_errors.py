"""Tests for :mod:`core.errors`: stable codes and usable messages."""

from __future__ import annotations

import re

import pytest

from core import errors

ERROR_CLASSES = [
    obj
    for _, obj in vars(errors).items()
    if isinstance(obj, type) and issubclass(obj, errors.DomainError) and obj is not errors.DomainError
]

CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def test_base_error_carries_message_and_code() -> None:
    error = errors.DomainError("boom")

    assert str(error) == "boom"
    assert error.message == "boom"
    assert error.code == "domain_error"


def test_error_catalogue_is_not_empty() -> None:
    assert len(ERROR_CLASSES) >= 15


@pytest.mark.parametrize("error_class", ERROR_CLASSES, ids=lambda cls: cls.__name__)
def test_every_error_has_a_snake_case_code(error_class: type[errors.DomainError]) -> None:
    assert CODE_PATTERN.match(error_class.code), error_class.code
    assert error_class.code != "domain_error"


def test_error_codes_are_unique() -> None:
    codes = [error_class.code for error_class in ERROR_CLASSES]

    assert len(codes) == len(set(codes))


@pytest.mark.parametrize("error_class", ERROR_CLASSES, ids=lambda cls: cls.__name__)
def test_default_messages_are_non_empty_and_overridable(
    error_class: type[errors.DomainError],
) -> None:
    default = error_class()

    assert isinstance(default.message, str) and default.message.strip()

    custom = error_class("پیام سفارشی")
    assert str(custom) == "پیام سفارشی"


def test_all_errors_are_domain_errors_and_catchable_together() -> None:
    for error_class in ERROR_CLASSES:
        assert issubclass(error_class, errors.DomainError)

    with pytest.raises(errors.DomainError):
        raise errors.PairLimitReachedError()


def test_programming_errors_are_not_domain_errors() -> None:
    assert not issubclass(ValueError, errors.DomainError)
    assert not issubclass(RuntimeError, errors.DomainError)
