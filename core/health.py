"""A tiny file-based heartbeat for Docker health checks.

The outbox worker rewrites the marker after every pass, so its age is a cheap
liveness signal: a fresh file means the delivery loop is running, a stale file
means the process is wedged or dead. The write is atomic (temp file + replace)
to avoid a health check ever reading a half-written JSON document.

This module intentionally has no dependency on Docker: the operator can point a
container ``HEALTHCHECK`` at the file, or read it from any external monitor.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path


class Heartbeat:
    """Writes ``{"status", "time", "pid"}`` to a file atomically.

    Args:
        path: Marker file path; parent directories are created on first write.
        clock: Injectable UTC clock (tests pass a fake).
    """

    def __init__(
        self, path: str | Path, *, clock: Callable[[], datetime] | None = None
    ) -> None:
        self._path = Path(path)
        self._clock = clock or (lambda: datetime.now(UTC))

    @property
    def path(self) -> Path:
        """Marker file path."""
        return self._path

    def beat(self, *, status: str = "ok") -> None:
        """Write a fresh heartbeat document."""
        moment = self._clock()
        payload = {
            "status": status,
            "time": moment.isoformat(),
            "pid": os.getpid(),
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_name(f".{self._path.name}.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
        temporary.replace(self._path)

    def clear(self) -> None:
        """Remove the marker (clean shutdown); missing files are ignored."""
        try:
            self._path.unlink(missing_ok=True)
        except OSError:  # pragma: no cover - best effort
            pass


__all__ = ["Heartbeat"]
