import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from ooni_common import (  # noqa: E402
    deterministic_gzip,
    event_path,
    merge_events,
    normalize_event,
    read_event_file,
    validate_event,
)


def source(uid="20260825000532.458680_YE_stunreachability_969b33cfee76b0cc"):
    return {
        "anomaly": True,
        "confirmed": False,
        "failure": False,
        "input": "stun://stun.voipgate.com:3478",
        "measurement_start_time": "2026-08-25T00:05:15.000000Z",
        "measurement_uid": uid,
        "measurement_url": "https://api.ooni.io/api/v1/raw_measurement?measurement_uid=" + uid,
        "probe_asn": "AS30873",
        "probe_cc": "YE",
        "report_id": "public-report-id",
        "scores": {
            "blocking_general": 1.0,
            "extra": {"failure": "generic_timeout_error"},
        },
        "test_name": "stunreachability",
        "verification_status": "verified",
    }


class OONIStorageTests(unittest.TestCase):
    def test_normalization_preserves_complete_public_summary(self):
        original = source()
        row = normalize_event(original)
        self.assertEqual(row["source_summary"], original)
        self.assertEqual(row["input_domain"], "stun.voipgate.com")
        self.assertEqual(row["measurement_start_day"], "2026-08-25")
        validate_event(row)

    def test_deterministic_gzip(self):
        payload = b"evidence\n"
        first = deterministic_gzip(payload)
        second = deterministic_gzip(payload)
        self.assertEqual(first, second)
        self.assertEqual(first[4:8], b"\x00\x00\x00\x00")
        self.assertEqual(gzip.decompress(first), payload)

    def test_merge_deduplicates_by_content_addressed_event_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            row = normalize_event(source())
            merge_events(root, [row, row])
            path = event_path(root, "2026-08-25")
            first_bytes = path.read_bytes()
            self.assertEqual(len(read_event_file(path)), 1)
            changed = merge_events(root, [row])
            self.assertEqual(changed, set())
            self.assertEqual(path.read_bytes(), first_bytes)

    def test_source_summary_tampering_is_detected(self):
        row = normalize_event(source())
        row["source_summary"]["probe_asn"] = "AS0"
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            validate_event(row)


if __name__ == "__main__":
    unittest.main()
