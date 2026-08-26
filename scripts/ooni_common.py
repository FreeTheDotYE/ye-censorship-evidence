#!/usr/bin/env python3
"""Shared deterministic storage and validation helpers."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import re
import tempfile
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit

EVENT_REQUIRED = {
    "anomaly",
    "event_id",
    "confirmed",
    "failure",
    "input",
    "measurement_start_day",
    "measurement_start_time",
    "measurement_uid",
    "measurement_url",
    "probe_asn",
    "probe_cc",
    "report_id",
    "scores",
    "source",
    "source_summary",
    "source_summary_sha256",
    "test_name",
    "verification_status",
}
HEX64 = re.compile(r"^[0-9a-f]{64}$")
DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
UID = re.compile(r"^[A-Za-z0-9_.:-]+$")
EVENT_INDEX_HEADER = [
    "day",
    "path",
    "sha256",
    "event_count",
    "anomaly_count",
    "confirmed_count",
    "failure_count",
    "first_uid",
    "last_uid",
]
TEST_HEADER = [
    "day",
    "test_name",
    "measurement_count",
    "ok_count",
    "anomaly_count",
    "confirmed_count",
    "failure_count",
]
NETWORK_HEADER = [
    "day",
    "probe_asn",
    "measurement_count",
    "ok_count",
    "anomaly_count",
    "confirmed_count",
    "failure_count",
]


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def event_path(root: Path, day: str) -> Path:
    year, month, _ = day.split("-")
    return root / "data" / "events" / year / month / f"{day}.jsonl.gz"


def deterministic_gzip(payload: bytes) -> bytes:
    output = io.BytesIO()
    with gzip.GzipFile(
        filename="", mode="wb", fileobj=output, compresslevel=9, mtime=0
    ) as archive:
        archive.write(payload)
    return output.getvalue()


def atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def extract_input_domain(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    candidate = value if "://" in value else f"//{value}"
    try:
        return urlsplit(candidate).hostname
    except ValueError:
        return None


def normalize_event(source: dict) -> dict:
    required_source = {
        "anomaly",
        "confirmed",
        "failure",
        "measurement_start_time",
        "measurement_uid",
        "probe_asn",
        "probe_cc",
        "test_name",
    }
    missing = required_source - source.keys()
    if missing:
        raise ValueError(f"OONI result missing fields: {sorted(missing)}")

    start_time = str(source["measurement_start_time"])
    if len(start_time) < 10 or not DAY.fullmatch(start_time[:10]):
        raise ValueError(f"Invalid measurement_start_time: {start_time!r}")

    source_digest = sha256_bytes(canonical_bytes(source))
    row = {
        "anomaly": bool(source["anomaly"]),
        "confirmed": bool(source["confirmed"]),
        "failure": bool(source["failure"]),
        "event_id": str(source["measurement_uid"]) + ":" + source_digest,
        "input": source.get("input"),
        "input_domain": extract_input_domain(source.get("input")),
        "measurement_start_day": start_time[:10],
        "measurement_start_time": start_time,
        "measurement_uid": str(source["measurement_uid"]),
        "measurement_url": source.get("measurement_url"),
        "probe_asn": str(source["probe_asn"]),
        "probe_cc": str(source["probe_cc"]),
        "report_id": source.get("report_id"),
        "scores": source.get("scores") or {},
        "source": "OONI",
        "source_summary": source,
        "source_summary_sha256": source_digest,
        "test_name": str(source["test_name"]),
        "verification_status": source.get("verification_status"),
    }
    if not row["measurement_url"]:
        row["measurement_url"] = (
            "https://api.ooni.io/api/v1/raw_measurement?measurement_uid="
            + row["measurement_uid"]
        )
    validate_event(row)
    return row


def validate_event(row: dict) -> None:
    missing = EVENT_REQUIRED - row.keys()
    if missing:
        raise ValueError(f"Event missing fields: {sorted(missing)}")
    if row["source"] != "OONI" or row["probe_cc"] != "YE":
        raise ValueError("Event scope must be OONI measurements from YE")
    if not DAY.fullmatch(str(row["measurement_start_day"])):
        raise ValueError("Invalid event day")
    if str(row["measurement_start_time"])[:10] != row["measurement_start_day"]:
        raise ValueError("Event time and day disagree")
    if not UID.fullmatch(str(row["measurement_uid"])):
        raise ValueError("Invalid measurement UID")
    expected_event_id = row["measurement_uid"] + ":" + row["source_summary_sha256"]
    if row["event_id"] != expected_event_id:
        raise ValueError("Invalid event ID")
    parsed = urlsplit(str(row["measurement_url"]))
    if parsed.scheme != "https" or parsed.hostname not in {
        "api.ooni.io",
        "api.ooni.org",
    }:
        raise ValueError("Invalid OONI measurement URL")
    if not HEX64.fullmatch(str(row["source_summary_sha256"])):
        raise ValueError("Invalid source summary digest")
    for key in ("anomaly", "confirmed", "failure"):
        if not isinstance(row[key], bool):
            raise ValueError(f"{key} must be boolean")
    if not isinstance(row["scores"], dict):
        raise ValueError("scores must be an object")
    if not isinstance(row["source_summary"], dict):
        raise ValueError("source_summary must be an object")
    if sha256_bytes(canonical_bytes(row["source_summary"])) != row["source_summary_sha256"]:
        raise ValueError("source summary digest mismatch")


def read_event_file(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.endswith("\n"):
                raise ValueError(f"{path}:{line_number}: missing newline")
            row = json.loads(line)
            if "event_id" not in row and "source_summary_sha256" in row:
                row["event_id"] = row["measurement_uid"] + ":" + row["source_summary_sha256"]
            validate_event(row)
            rows.append(row)
    return rows


def merge_events(root: Path, incoming: list[dict]) -> set[str]:
    by_day: dict[str, list[dict]] = defaultdict(list)
    for row in incoming:
        validate_event(row)
        by_day[row["measurement_start_day"]].append(row)

    changed: set[str] = set()
    for day, additions in sorted(by_day.items()):
        path = event_path(root, day)
        existing = {row["event_id"]: row for row in read_event_file(path)}
        for row in additions:
            existing[row["event_id"]] = row
        ordered = [existing[key] for key in sorted(existing)]
        payload = b"".join(canonical_bytes(row) + b"\n" for row in ordered)
        compressed = deterministic_gzip(payload)
        if not path.exists() or path.read_bytes() != compressed:
            atomic_write(path, compressed)
            changed.add(day)
    return changed


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, header: list[str], rows: list[dict]) -> None:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=header, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in header})
    atomic_write(path, output.getvalue().encode("utf-8"))


def merge_aggregates(path: Path, header: list[str], key_fields: tuple[str, str], incoming: list[dict]) -> None:
    merged = {
        tuple(row[field] for field in key_fields): row
        for row in read_csv_rows(path)
    }
    for row in incoming:
        key = tuple(str(row[field]) for field in key_fields)
        merged[key] = {field: row[field] for field in header}
    ordered = [merged[key] for key in sorted(merged)]
    write_csv_rows(path, header, ordered)


def iter_event_files(root: Path):
    base = root / "data" / "events"
    if base.exists():
        yield from sorted(base.glob("*/*/*.jsonl.gz"))


def derived_outputs(root: Path) -> tuple[list[dict], dict]:
    index_rows: list[dict] = []
    all_events: list[dict] = []
    seen: set[str] = set()
    for path in iter_event_files(root):
        rows = read_event_file(path)
        event_ids = [row["event_id"] for row in rows]
        uids = [row["measurement_uid"] for row in rows]
        if event_ids != sorted(event_ids) or len(event_ids) != len(set(event_ids)):
            raise ValueError(f"{path}: event records are not unique and sorted")
        duplicates = seen.intersection(event_ids)
        if duplicates:
            raise ValueError(f"Duplicate event record across files: {sorted(duplicates)[:3]}")
        seen.update(event_ids)
        all_events.extend(rows)
        raw = path.read_bytes()
        day = path.name.removesuffix(".jsonl.gz")
        index_rows.append(
            {
                "day": day,
                "path": path.relative_to(root).as_posix(),
                "sha256": sha256_bytes(raw),
                "event_count": len(rows),
                "anomaly_count": sum(row["anomaly"] for row in rows),
                "confirmed_count": sum(row["confirmed"] for row in rows),
                "failure_count": sum(row["failure"] for row in rows),
                "first_uid": uids[0] if uids else "",
                "last_uid": uids[-1] if uids else "",
            }
        )

    days = sorted({row["measurement_start_day"] for row in all_events})
    summary = {
        "schema_version": 1,
        "scope": {"probe_cc": "YE", "source": "OONI"},
        "event_count": len(all_events),
        "anomaly_count": sum(row["anomaly"] for row in all_events),
        "confirmed_count": sum(row["confirmed"] for row in all_events),
        "failure_count": sum(row["failure"] for row in all_events),
        "earliest_event_day": days[0] if days else None,
        "latest_event_day": days[-1] if days else None,
        "unique_measurement_count": len({row["measurement_uid"] for row in all_events}),
        "source_summary_variant_count": len(all_events)
        - len({row["measurement_uid"] for row in all_events}),
        "unique_probe_asns": len({row["probe_asn"] for row in all_events}),
        "unique_test_names": len({row["test_name"] for row in all_events}),
        "unique_input_domains": len(
            {row["input_domain"] for row in all_events if row["input_domain"]}
        ),
    }
    return index_rows, summary


def write_derived_outputs(root: Path) -> None:
    index_rows, summary = derived_outputs(root)
    write_csv_rows(root / "data" / "events" / "index.csv", EVENT_INDEX_HEADER, index_rows)
    atomic_write(
        root / "data" / "summary.json",
        json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        + b"\n",
    )
