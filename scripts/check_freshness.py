#!/usr/bin/env python3
"""Fail visibly when the OONI current-data heartbeat is absent or stale."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = 1


def parse_utc(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("completed_at must be a UTC timestamp ending in Z")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("completed_at must be UTC")
    return parsed


def parse_day(value: object, field: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO date")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO date") from error


def check(
    path: Path,
    *,
    now: datetime,
    max_age_hours: float,
    collector_conclusion: str = "",
) -> dict:
    if collector_conclusion and collector_conclusion != "success":
        raise ValueError(f"collector conclusion is {collector_conclusion!r}")
    try:
        heartbeat = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError("last-success heartbeat is missing") from error
    except json.JSONDecodeError as error:
        raise ValueError("last-success heartbeat is malformed JSON") from error
    if heartbeat.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported heartbeat schema")
    completed = parse_utc(heartbeat.get("completed_at"))
    window = heartbeat.get("recent_window")
    if not isinstance(window, dict):
        raise ValueError("recent_window must be an object")
    parse_day(window.get("since"), "recent_window.since")
    parse_day(window.get("until"), "recent_window.until")
    if window["since"] >= window["until"]:
        raise ValueError("recent_window must have positive duration")
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(timezone.utc)
    age_hours = (now - completed).total_seconds() / 3600
    if age_hours < -0.25:
        raise ValueError("heartbeat completion time is in the future")
    if age_hours > max_age_hours:
        raise ValueError(
            f"last successful current-data collection is {age_hours:.2f} hours old"
        )
    return {
        "fresh": True,
        "completed_at": heartbeat["completed_at"],
        "age_hours": round(max(age_hours, 0.0), 2),
        "max_age_hours": max_age_hours,
        "recent_window": window,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--now", default="")
    parser.add_argument("--max-age-hours", type=float, default=30.0)
    parser.add_argument("--collector-conclusion", default="")
    args = parser.parse_args()
    now = parse_utc(args.now) if args.now else datetime.now(timezone.utc)
    result = check(
        args.root / "state" / "last-success.json",
        now=now,
        max_age_hours=args.max_age_hours,
        collector_conclusion=args.collector_conclusion,
    )
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"FRESHNESS CHECK FAILED: {error}", file=sys.stderr)
        raise
