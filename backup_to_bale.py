"""Create a consistent SQLite backup and send it to a Bale group.

Designed to run inside the bridge container (same DB volume):
    python backup_to_bale.py

Required environment:
    BACKUP_BALE_BOT_TOKEN   dedicated bot token (never commit it)
    BACKUP_BALE_CHAT_ID     Bale group id

Optional:
    DB_PATH                 default /app/data/bridge.db
    BALE_API_BASE_URL       default https://tapi.bale.ai
    BACKUP_TIMEZONE_OFFSET_MINUTES  default 210 (Iran, UTC+03:30)
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

DEFAULT_DB_PATH = Path("/app/data/bridge.db")
DEFAULT_API_BASE_URL = "https://tapi.bale.ai"
DEFAULT_TIMEZONE_OFFSET_MINUTES = 210


def create_backup(source: Path, destination: Path) -> None:
    """Copy a live SQLite database consistently, including committed WAL data."""
    if not source.is_file():
        raise FileNotFoundError(f"database not found: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(source)
    dst = sqlite3.connect(destination)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def backup_caption(now: datetime, size_bytes: int) -> str:
    """Human-readable Persian caption with date, time and size."""
    return (
        "🗄 بکاپ روزانه دیتابیس\n"
        f"📅 تاریخ: {now:%Y-%m-%d}\n"
        f"🕐 ساعت: {now:%H:%M} (به وقت ایران)\n"
        f"📦 حجم: {size_bytes / (1024 * 1024):.2f} مگابایت"
    )


def send_document(
    *,
    token: str,
    chat_id: int,
    api_base_url: str,
    path: Path,
    caption: str,
    client: httpx.Client | None = None,
) -> None:
    """Upload the backup with Bale's ``sendDocument`` endpoint."""
    owns_client = client is None
    http = client or httpx.Client(timeout=120.0)
    try:
        with path.open("rb") as handle:
            response = http.post(
                f"{api_base_url.rstrip('/')}/bot{token}/sendDocument",
                data={"chat_id": str(chat_id), "caption": caption},
                files={
                    "document": (
                        path.name,
                        handle,
                        "application/octet-stream",
                    )
                },
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Bale returned non-JSON response (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict) or not payload.get("ok"):
            code = payload.get("error_code") if isinstance(payload, dict) else None
            description = (
                payload.get("description") if isinstance(payload, dict) else None
            )
            raise RuntimeError(
                f"Bale sendDocument failed (code={code}): {description or 'unknown error'}"
            )
    finally:
        if owns_client:
            http.close()


def main() -> int:
    token = os.environ.get("BACKUP_BALE_BOT_TOKEN", "").strip()
    raw_chat_id = os.environ.get("BACKUP_BALE_CHAT_ID", "").strip()
    if not token:
        print("BACKUP_BALE_BOT_TOKEN is missing", file=sys.stderr)
        return 2
    if not raw_chat_id:
        print("BACKUP_BALE_CHAT_ID is missing", file=sys.stderr)
        return 2
    try:
        chat_id = int(raw_chat_id)
    except ValueError:
        print("BACKUP_BALE_CHAT_ID must be numeric", file=sys.stderr)
        return 2

    db_path = Path(os.environ.get("DB_PATH", str(DEFAULT_DB_PATH)))
    api_base_url = os.environ.get("BALE_API_BASE_URL", DEFAULT_API_BASE_URL)
    offset = int(
        os.environ.get(
            "BACKUP_TIMEZONE_OFFSET_MINUTES",
            str(DEFAULT_TIMEZONE_OFFSET_MINUTES),
        )
    )
    now = datetime.now(timezone(timedelta(minutes=offset)))
    stamp = now.strftime("%Y%m%d-%H%M%S")
    temp_path = Path(tempfile.gettempdir()) / f"bridge-backup-{stamp}.db"
    try:
        create_backup(db_path, temp_path)
        size = temp_path.stat().st_size
        caption = backup_caption(now, size)
        send_document(
            token=token,
            chat_id=chat_id,
            api_base_url=api_base_url,
            path=temp_path,
            caption=caption,
        )
        print(f"backup sent: {temp_path.name} ({size} bytes)")
        return 0
    except Exception as exc:  # noqa: BLE001 - cron needs a non-zero exit
        print(f"backup failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
