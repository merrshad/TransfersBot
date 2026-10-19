"""Container health probe for the bridge's file-based heartbeat.

The outbox worker rewrites ``HEALTHCHECK_FILE`` after every pass. This script
turns that marker into a Docker health status:

* missing file -> unhealthy (the worker has not started or cannot write);
* marker older than ``HEALTHCHECK_MAX_AGE_SECONDS`` -> unhealthy (wedged);
* status other than ``ok`` -> unhealthy (the last pass raised);
* otherwise -> healthy (exit 0).

It prints only the path, age and status — never message content or secrets.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

DEFAULT_PATH = "data/health"
DEFAULT_MAX_AGE_SECONDS = 90.0


def check_health(path: Path, *, max_age_seconds: float, now: float | None = None) -> tuple[bool, str]:
    """Return ``(healthy, reason)`` for one heartbeat file."""
    if max_age_seconds <= 0:
        return False, "HEALTHCHECK_MAX_AGE_SECONDS must be positive"
    try:
        stat = path.stat()
    except OSError:
        return False, f"health file is missing: {path}"

    age = max((time.time() if now is None else now) - stat.st_mtime, 0.0)
    if age > max_age_seconds:
        return False, f"heartbeat is stale: {age:.0f}s > {max_age_seconds:.0f}s"

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False, "health file is unreadable or not valid JSON"
    status = payload.get("status") if isinstance(payload, dict) else None
    if status != "ok":
        return False, f"worker status is {status!r}"
    return True, f"healthy (age {age:.0f}s)"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; returns a process exit code."""
    parser = argparse.ArgumentParser(description="Check the bridge heartbeat file.")
    parser.add_argument(
        "--path",
        default=os.environ.get("HEALTHCHECK_FILE", DEFAULT_PATH),
        help="heartbeat file (default: $HEALTHCHECK_FILE or data/health)",
    )
    parser.add_argument(
        "--max-age",
        type=float,
        default=float(
            os.environ.get("HEALTHCHECK_MAX_AGE_SECONDS", DEFAULT_MAX_AGE_SECONDS)
        ),
        help="maximum accepted heartbeat age in seconds",
    )
    args = parser.parse_args(argv)
    healthy, reason = check_health(Path(args.path), max_age_seconds=args.max_age)
    print(("OK: " if healthy else "UNHEALTHY: ") + reason)
    return 0 if healthy else 1


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    raise SystemExit(main())


__all__ = ["DEFAULT_MAX_AGE_SECONDS", "DEFAULT_PATH", "check_health", "main"]
