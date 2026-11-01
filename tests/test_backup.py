"""Tests for the daily Bale backup script (no network)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from backup_to_bale import backup_caption, create_backup, send_document


def test_create_backup_copies_committed_data(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO users (name) VALUES ('test')")
        connection.commit()

    destination = tmp_path / "backup.db"
    create_backup(source, destination)

    with sqlite3.connect(destination) as connection:
        row = connection.execute("SELECT name FROM users").fetchone()
    assert row == ("test",)


def test_backup_caption_contains_date_time_and_size() -> None:
    now = datetime(
        2026, 11, 1, 3, 5, tzinfo=timezone(timedelta(minutes=210))
    )

    caption = backup_caption(now, 2 * 1024 * 1024)

    assert "2026-11-01" in caption
    assert "03:05" in caption
    assert "2.00" in caption
    assert "به وقت ایران" in caption


def test_send_document_reports_bale_error(tmp_path: Path) -> None:
    path = tmp_path / "backup.db"
    path.write_bytes(b"backup")

    def handler(request: httpx.Request) -> httpx.Response:
        assert "sendDocument" in request.url.path
        return httpx.Response(
            200,
            json={"ok": False, "error_code": 400, "description": "bad request"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RuntimeError, match="sendDocument failed"):
            send_document(
                token="test-token",
                chat_id=123,
                api_base_url="https://example.test",
                path=path,
                caption="backup",
                client=client,
            )
