import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from check_freshness import check  # noqa: E402
from update_ooni import write_last_success  # noqa: E402


class FreshnessTests(unittest.TestCase):
    def heartbeat(self, root: Path, completed_at: str = "2026-08-27T12:00:00Z") -> Path:
        path = root / "state" / "last-success.json"
        path.parent.mkdir()
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "completed_at": completed_at,
                    "recent_window": {
                        "since": "2026-08-24",
                        "until": "2026-08-28",
                    },
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_fresh_heartbeat_passes(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = self.heartbeat(Path(temporary))
            result = check(
                path,
                now=datetime(2026, 8, 28, 6, 0, tzinfo=timezone.utc),
                max_age_hours=30,
            )
            self.assertTrue(result["fresh"])
            self.assertEqual(result["age_hours"], 18.0)

    def test_stale_heartbeat_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = self.heartbeat(Path(temporary))
            with self.assertRaisesRegex(ValueError, "31.00 hours old"):
                check(
                    path,
                    now=datetime(2026, 8, 28, 19, 0, tzinfo=timezone.utc),
                    max_age_hours=30,
                )

    def test_malformed_heartbeat_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = self.heartbeat(Path(temporary))
            path.write_text('{"schema_version": 1}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "completed_at"):
                check(
                    path,
                    now=datetime(2026, 8, 27, 13, 0, tzinfo=timezone.utc),
                    max_age_hours=30,
                )

    def test_failed_collector_fails_even_with_fresh_heartbeat(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = self.heartbeat(Path(temporary))
            with self.assertRaisesRegex(ValueError, "failure"):
                check(
                    path,
                    now=datetime(2026, 8, 27, 13, 0, tzinfo=timezone.utc),
                    max_age_hours=30,
                    collector_conclusion="failure",
                )

    def test_collector_writes_exact_recent_window(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state" / "last-success.json"
            heartbeat = write_last_success(
                path,
                datetime(2026, 8, 27, 12, 52, 1, 123456, tzinfo=timezone.utc),
                datetime(2026, 8, 24).date(),
                datetime(2026, 8, 28).date(),
            )
            self.assertEqual(heartbeat["completed_at"], "2026-08-27T12:52:01Z")
            self.assertEqual(
                heartbeat["recent_window"],
                {"since": "2026-08-24", "until": "2026-08-28"},
            )
            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8")),
                heartbeat,
            )


if __name__ == "__main__":
    unittest.main()
