"""Tests for the container health probe (no Docker, no clock waits)."""

from __future__ import annotations

import json
import os
from pathlib import Path

from healthcheck import check_health, main


def write_marker(path: Path, *, status: str = "ok", age_seconds: float = 0.0) -> None:
    """Write a heartbeat and backdate its mtime by ``age_seconds``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"status": status, "time": "2026-01-01T12:00:00+00:00", "pid": 1}),
        encoding="utf-8",
    )
    if age_seconds:
        stamp = path.stat().st_mtime - age_seconds
        os.utime(path, (stamp, stamp))


def test_fresh_ok_marker_is_healthy(tmp_path: Path) -> None:
    path = tmp_path / "health.json"
    write_marker(path)

    healthy, reason = check_health(path, max_age_seconds=90)

    assert healthy is True
    assert "healthy" in reason


def test_missing_marker_is_unhealthy(tmp_path: Path) -> None:
    healthy, reason = check_health(tmp_path / "missing.json", max_age_seconds=90)

    assert healthy is False
    assert "missing" in reason


def test_stale_marker_is_unhealthy(tmp_path: Path) -> None:
    path = tmp_path / "health.json"
    write_marker(path, age_seconds=100)

    healthy, reason = check_health(path, max_age_seconds=90)

    assert healthy is False
    assert "stale" in reason


def test_degraded_marker_is_unhealthy(tmp_path: Path) -> None:
    path = tmp_path / "health.json"
    write_marker(path, status="degraded")

    healthy, reason = check_health(path, max_age_seconds=90)

    assert healthy is False
    assert "degraded" in reason


def test_invalid_json_is_unhealthy(tmp_path: Path) -> None:
    path = tmp_path / "health.json"
    path.write_text("not-json", encoding="utf-8")

    healthy, reason = check_health(path, max_age_seconds=90)

    assert healthy is False
    assert "unreadable" in reason


def test_cli_returns_zero_for_a_healthy_marker(
    tmp_path: Path, capsys
) -> None:
    path = tmp_path / "health.json"
    write_marker(path)

    code = main(["--path", str(path), "--max-age", "90"])

    assert code == 0
    assert "OK:" in capsys.readouterr().out
