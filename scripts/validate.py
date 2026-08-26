#!/usr/bin/env python3
"""Validate repository data, checksums, ordering, and aggregate arithmetic."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

from ooni_common import (
    DAY,
    EVENT_INDEX_HEADER,
    NETWORK_HEADER,
    TEST_HEADER,
    derived_outputs,
    iter_event_files,
    read_event_file,
)


def validate_aggregate(path: Path, header: list[str], second_key: str) -> int:
    if not path.exists():
        raise ValueError(f"Missing aggregate: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != header:
            raise ValueError(f"{path}: unexpected header {reader.fieldnames}")
        rows = list(reader)

    keys = [(row["day"], row[second_key]) for row in rows]
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise ValueError(f"{path}: rows must be sorted and unique")
    for number, row in enumerate(rows, 2):
        if not DAY.fullmatch(row["day"]):
            raise ValueError(f"{path}:{number}: invalid day")
        counts = {
            field: int(row[field])
            for field in (
                "measurement_count",
                "ok_count",
                "anomaly_count",
                "confirmed_count",
                "failure_count",
            )
        }
        if min(counts.values()) < 0:
            raise ValueError(f"{path}:{number}: negative count")
        if counts["measurement_count"] != (
            counts["ok_count"]
            + counts["anomaly_count"]
            + counts["confirmed_count"]
            + counts["failure_count"]
        ):
            raise ValueError(f"{path}:{number}: aggregate categories do not total")
        # OONI aggregation reports confirmed as a separate outcome bucket.
    return len(rows)


def validate(root: Path) -> dict:
    root = root.resolve()
    event_files = list(iter_event_files(root))
    for path in event_files:
        raw = path.read_bytes()
        if len(raw) < 10 or raw[:2] != b"\x1f\x8b":
            raise ValueError(f"{path}: not gzip")
        if raw[4:8] != b"\x00\x00\x00\x00":
            raise ValueError(f"{path}: gzip timestamp is not deterministic")
        day = path.name.removesuffix(".jsonl.gz")
        rows = read_event_file(path)
        if any(row["measurement_start_day"] != day for row in rows):
            raise ValueError(f"{path}: row stored under wrong day")

    expected_index, expected_summary = derived_outputs(root)
    index_path = root / "data" / "events" / "index.csv"
    if not index_path.exists():
        raise ValueError("Missing event index")
    with index_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EVENT_INDEX_HEADER:
            raise ValueError("Unexpected event index header")
        actual_index = list(reader)
    expected_index_text = [
        {key: str(row.get(key, "")) for key in EVENT_INDEX_HEADER}
        for row in expected_index
    ]
    if actual_index != expected_index_text:
        raise ValueError("Event index does not match event files")

    summary_path = root / "data" / "summary.json"
    if json.loads(summary_path.read_text(encoding="utf-8")) != expected_summary:
        raise ValueError("Summary does not match event files")

    state = json.loads((root / "state" / "cursor.json").read_text(encoding="utf-8"))
    if state.get("schema_version") != 1:
        raise ValueError("Unsupported cursor schema")
    backfill = state.get("backfill", {})
    if not isinstance(backfill.get("complete"), bool):
        raise ValueError("Invalid backfill completion flag")
    for key in ("since", "until"):
        if not DAY.fullmatch(str(backfill.get(key, ""))):
            raise ValueError(f"Invalid backfill {key}")
    if not isinstance(backfill.get("offset"), int) or backfill["offset"] < 0:
        raise ValueError("Invalid backfill offset")

    test_rows = validate_aggregate(
        root / "data" / "aggregates" / "daily_by_test.csv",
        TEST_HEADER,
        "test_name",
    )
    network_rows = validate_aggregate(
        root / "data" / "aggregates" / "daily_by_network.csv",
        NETWORK_HEADER,
        "probe_asn",
    )
    return {
        "event_files": len(event_files),
        "events": expected_summary["event_count"],
        "test_aggregate_rows": test_rows,
        "network_aggregate_rows": network_rows,
    }


if __name__ == "__main__":
    try:
        result = validate(Path(__file__).resolve().parents[1])
    except Exception as error:
        print(f"VALIDATION FAILED: {error}", file=sys.stderr)
        raise
    print(json.dumps(result, sort_keys=True, indent=2))
