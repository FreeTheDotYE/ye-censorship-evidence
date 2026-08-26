#!/usr/bin/env python3
"""Collect reproducible OONI evidence and aggregate context for Yemen."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ooni_common import (
    NETWORK_HEADER,
    TEST_HEADER,
    atomic_write,
    merge_aggregates,
    merge_events,
    normalize_event,
    write_derived_outputs,
)

API = "https://api.ooni.org/api/v1"
STATE_VERSION = 1
BACKFILL_START = date(2012, 1, 1)
WINDOW_DAYS = 180


class OONIClient:
    def __init__(self, delay: float = 1.0, timeout: float = 60.0):
        self.delay = delay
        self.timeout = timeout
        self.requests = 0

    def get_json(self, endpoint_or_url: str, params: dict | None = None) -> dict:
        if endpoint_or_url.startswith("https://"):
            url = endpoint_or_url
        else:
            query = urllib.parse.urlencode(params or {})
            url = f"{API}/{endpoint_or_url}"
            if query:
                url += "?" + query

        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname not in {
            "api.ooni.org",
            "api.ooni.io",
        }:
            raise ValueError(f"Refusing unexpected API URL: {url}")

        if self.requests and self.delay:
            time.sleep(self.delay)
        self.requests += 1
        headers = {
            "Accept": "application/json",
            "User-Agent": "FreeTheDotYE-OONI-Archive/1.0",
        }
        last_error = None
        for attempt in range(4):
            try:
                request = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    return json.load(response)
            except Exception as error:
                last_error = error
                if attempt == 3:
                    break
                time.sleep(2**attempt)
        raise RuntimeError(f"OONI request failed after retries: {url}") from last_error


def day_string(value: date) -> str:
    return value.isoformat()


def parse_day(value: str) -> date:
    return date.fromisoformat(value)


def load_state(path: Path, today: date) -> dict:
    if path.exists():
        state = json.loads(path.read_text(encoding="utf-8"))
    else:
        state = {
            "schema_version": STATE_VERSION,
            "backfill": {
                "complete": False,
                "since": day_string(BACKFILL_START),
                "until": day_string(min(BACKFILL_START + timedelta(days=WINDOW_DAYS), today + timedelta(days=1))),
                "offset": 0,
            },
        }
    if state.get("schema_version") != STATE_VERSION:
        raise ValueError("Unsupported state schema")
    return state


def write_state(path: Path, state: dict) -> None:
    payload = json.dumps(state, sort_keys=True, indent=2).encode("utf-8") + b"\n"
    atomic_write(path, payload)


def collect_event_window(
    client: OONIClient,
    since: date,
    until: date,
    *,
    offset: int = 0,
    max_pages: int,
    require_complete: bool,
) -> tuple[list[dict], int | None, int]:
    rows: list[dict] = []
    current_offset = offset
    for page_number in range(1, max_pages + 1):
        response = client.get_json(
            "measurements",
            {
                "probe_cc": "YE",
                "anomaly": "true",
                "since": day_string(since),
                "until": day_string(until),
                "limit": 100,
                "offset": current_offset,
                "order_by": "measurement_start_time",
                "order": "asc",
            },
        )
        results = response.get("results")
        metadata = response.get("metadata")
        if not isinstance(results, list) or not isinstance(metadata, dict):
            raise ValueError("Unexpected OONI measurements response")
        rows.extend(normalize_event(item) for item in results)
        next_url = metadata.get("next_url")
        if not next_url:
            return rows, None, page_number
        current_offset += len(results)
        if not results:
            raise ValueError("OONI pagination returned an empty page with next_url")

    if require_complete:
        raise RuntimeError(
            f"Recent OONI window exceeded {max_pages} pages; refusing a partial refresh"
        )
    return rows, current_offset, max_pages


def aggregate_rows(client: OONIClient, since: date, until: date, axis_y: str) -> list[dict]:
    response = client.get_json(
        "aggregation",
        {
            "probe_cc": "YE",
            "since": day_string(since),
            "until": day_string(until),
            "time_grain": "day",
            "axis_x": "measurement_start_day",
            "axis_y": axis_y,
        },
    )
    results = response.get("result")
    if not isinstance(results, list):
        raise ValueError("Unexpected OONI aggregation response")
    output = []
    second_key = "test_name" if axis_y == "test_name" else "probe_asn"
    for source in results:
        row = {
            "day": str(source["measurement_start_day"]),
            second_key: str(source[second_key]),
            "measurement_count": int(source["measurement_count"]),
            "ok_count": int(source["ok_count"]),
            "anomaly_count": int(source["anomaly_count"]),
            "confirmed_count": int(source["confirmed_count"]),
            "failure_count": int(source["failure_count"]),
        }
        output.append(row)
    return output


def merge_window_aggregates(root: Path, client: OONIClient, since: date, until: date) -> None:
    tests = aggregate_rows(client, since, until, "test_name")
    networks = aggregate_rows(client, since, until, "probe_asn")
    merge_aggregates(
        root / "data" / "aggregates" / "daily_by_test.csv",
        TEST_HEADER,
        ("day", "test_name"),
        tests,
    )
    merge_aggregates(
        root / "data" / "aggregates" / "daily_by_network.csv",
        NETWORK_HEADER,
        ("day", "probe_asn"),
        networks,
    )


def advance_backfill(state: dict, today: date) -> None:
    backfill = state["backfill"]
    next_since = parse_day(backfill["until"])
    final_until = today + timedelta(days=1)
    if next_since >= final_until:
        backfill.update(
            {
                "complete": True,
                "since": day_string(final_until),
                "until": day_string(final_until),
                "offset": 0,
            }
        )
        return
    backfill.update(
        {
            "complete": False,
            "since": day_string(next_since),
            "until": day_string(min(next_since + timedelta(days=WINDOW_DAYS), final_until)),
            "offset": 0,
        }
    )


def run(args: argparse.Namespace) -> dict:
    root = Path(args.root).resolve()
    state_path = root / "state" / "cursor.json"
    today = datetime.now(timezone.utc).date()
    client = OONIClient(args.request_delay, args.timeout)
    state = load_state(state_path, today)

    recent_since = today - timedelta(days=max(args.recent_days - 1, 0))
    recent_until = today + timedelta(days=1)
    recent, remaining, _ = collect_event_window(
        client,
        recent_since,
        recent_until,
        max_pages=args.recent_max_pages,
        require_complete=True,
    )
    if remaining is not None:
        raise AssertionError("Complete recent collection returned a cursor")
    merge_events(root, recent)
    merge_window_aggregates(root, client, recent_since, recent_until)

    history_count = 0
    history_pages_left = args.history_pages
    while (
        not args.no_backfill
        and not state["backfill"]["complete"]
        and history_pages_left > 0
    ):
        backfill = state["backfill"]
        since = parse_day(backfill["since"])
        until = parse_day(backfill["until"])
        historical, next_offset, pages_used = collect_event_window(
            client,
            since,
            until,
            offset=int(backfill["offset"]),
            max_pages=history_pages_left,
            require_complete=False,
        )
        history_pages_left -= pages_used
        merge_events(root, historical)
        history_count += len(historical)
        if next_offset is None:
            merge_window_aggregates(root, client, since, until)
            advance_backfill(state, today)
        else:
            backfill["offset"] = next_offset
            break

    write_state(state_path, state)
    write_derived_outputs(root)
    return {
        "requests": client.requests,
        "recent_events_received": len(recent),
        "history_events_received": history_count,
        "backfill": state["backfill"],
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--root", default=Path(__file__).resolve().parents[1])
    result.add_argument("--recent-days", type=int, default=4)
    result.add_argument("--recent-max-pages", type=int, default=100)
    result.add_argument("--history-pages", type=int, default=100)
    result.add_argument("--request-delay", type=float, default=1.0)
    result.add_argument("--timeout", type=float, default=60.0)
    result.add_argument("--no-backfill", action="store_true")
    return result


if __name__ == "__main__":
    try:
        report = run(parser().parse_args())
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise
    print(json.dumps(report, sort_keys=True, indent=2))
