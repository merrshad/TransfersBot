"""Tests for the pairing state machine itself (pure functions, no storage)."""

from __future__ import annotations

import pytest

from core.errors import InvalidTransitionError
from core.pairing import (
    PAIR_TRANSITIONS,
    REQUEST_TRANSITIONS,
    TERMINAL_STATUSES,
    PairingStatus,
    can_transition,
    ensure_transition,
)

ALL_STATUSES = set(PairingStatus)
TERMINALS = {
    PairingStatus.REJECTED,
    PairingStatus.EXPIRED,
    PairingStatus.CANCELLED,
    PairingStatus.UNPAIRED,
}


def test_status_vocabulary_matches_specification() -> None:
    assert {status.value for status in PairingStatus} == {
        "pending",
        "active",
        "rejected",
        "expired",
        "cancelled",
        "unpaired",
    }


def test_request_transition_table_is_complete() -> None:
    assert set(REQUEST_TRANSITIONS) == ALL_STATUSES, "every state must be described"
    assert set(PAIR_TRANSITIONS) == ALL_STATUSES


def test_pending_request_can_only_be_answered_or_expire() -> None:
    assert REQUEST_TRANSITIONS[PairingStatus.PENDING] == frozenset(
        {
            PairingStatus.ACTIVE,
            PairingStatus.REJECTED,
            PairingStatus.EXPIRED,
            PairingStatus.CANCELLED,
        }
    )


def test_active_pair_can_only_be_unpaired() -> None:
    assert PAIR_TRANSITIONS[PairingStatus.ACTIVE] == frozenset({PairingStatus.UNPAIRED})


def test_terminal_states_have_no_outgoing_transitions() -> None:
    for status in TERMINALS:
        assert REQUEST_TRANSITIONS[status] == frozenset()
        assert PAIR_TRANSITIONS[status] == frozenset()
        assert status in TERMINAL_STATUSES

    assert PairingStatus.PENDING not in TERMINAL_STATUSES
    assert PairingStatus.ACTIVE not in TERMINAL_STATUSES


@pytest.mark.parametrize(
    "new",
    [
        PairingStatus.ACTIVE,
        PairingStatus.REJECTED,
        PairingStatus.EXPIRED,
        PairingStatus.CANCELLED,
    ],
)
def test_can_transition_allows_pending_targets(new: PairingStatus) -> None:
    assert can_transition(PairingStatus.PENDING, new)


@pytest.mark.parametrize(
    ("current", "new"),
    [
        (PairingStatus.PENDING, PairingStatus.UNPAIRED),
        (PairingStatus.PENDING, PairingStatus.PENDING),
        (PairingStatus.ACTIVE, PairingStatus.PENDING),
        (PairingStatus.ACTIVE, PairingStatus.REJECTED),
        (PairingStatus.REJECTED, PairingStatus.ACTIVE),
        (PairingStatus.EXPIRED, PairingStatus.ACTIVE),
        (PairingStatus.CANCELLED, PairingStatus.PENDING),
        (PairingStatus.UNPAIRED, PairingStatus.ACTIVE),
    ],
)
def test_can_transition_rejects_invalid_moves(current: PairingStatus, new: PairingStatus) -> None:
    assert not can_transition(current, new)


def test_ensure_transition_raises_with_code() -> None:
    with pytest.raises(InvalidTransitionError) as excinfo:
        ensure_transition(PairingStatus.REJECTED, PairingStatus.ACTIVE)

    error = excinfo.value
    assert error.code == "invalid_transition"
    assert "rejected" in error.message and "active" in error.message


def test_ensure_transition_accepts_valid_move() -> None:
    ensure_transition(PairingStatus.PENDING, PairingStatus.ACTIVE)  # does not raise
    ensure_transition(
        PairingStatus.ACTIVE,
        PairingStatus.UNPAIRED,
        transitions=PAIR_TRANSITIONS,
    )


def test_pair_transitions_reject_request_only_targets() -> None:
    with pytest.raises(InvalidTransitionError):
        ensure_transition(
            PairingStatus.ACTIVE,
            PairingStatus.CANCELLED,
            transitions=PAIR_TRANSITIONS,
        )
