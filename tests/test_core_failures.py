from __future__ import annotations

import tempfile
import time
import unittest
import sqlite3
import os
from pathlib import Path
from unittest.mock import patch

from queue_core import (
    CURRENT_SCHEMA_VERSION,
    DeviceQueue,
    DeviceState,
    JobStore,
    QueueConfig,
    QueueUnavailable,
)


class CoreFailureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.config = QueueConfig(
            log_dir=Path(self.temp.name), default_timeout=2, max_timeout=10,
            process_poll_interval=0.01, stop_grace_sec=0.1,
            reset_cmd="/bin/true", health_check_cmd="/bin/true",
        )
        self.store = JobStore(self.config.db_path)
        self.queue = DeviceQueue(self.config, self.store)

    def tearDown(self) -> None:
        self.queue.initiate_shutdown()
        self.queue.join(2)
        self.temp.cleanup()

    def test_submission_is_not_published_when_insert_fails(self) -> None:
        with patch.object(self.store, "create_job", side_effect=OSError("disk full")):
            with self.assertRaises(QueueUnavailable):
                self.queue.submit("true")
        self.assertEqual(self.queue.status()["pending"], [])
        self.assertEqual(list(Path(self.temp.name).glob("*/output")), [])

    def test_completed_jobs_are_evicted_from_memory(self) -> None:
        self.queue.start()
        job, _, _ = self.queue.submit("true")
        deadline = time.time() + 3
        while time.time() < deadline:
            stored = self.queue.get_job(job.id)
            if stored and stored.status == "done" and job.id not in self.queue._jobs:
                break
            time.sleep(0.01)
        self.assertEqual(self.queue.get_job(job.id).status, "done")
        self.assertNotIn(job.id, self.queue._jobs)

    def test_transient_transition_failure_recovers_without_losing_job(self) -> None:
        job, _, _ = self.queue.submit("true")
        original = self.store.update_job
        calls = 0

        def flaky(value):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("temporary storage fault")
            return original(value)

        with patch.object(self.store, "update_job", side_effect=flaky):
            self.queue.start()
            deadline = time.time() + 4
            while time.time() < deadline:
                stored = self.queue.get_job(job.id)
                if stored and stored.status == "done":
                    break
                time.sleep(0.02)
        self.assertEqual(self.queue.get_job(job.id).status, "done")
        self.assertIsNone(self.queue.status()["worker"]["degraded_reason"])

    def test_monitoring_failure_kills_and_reaps_job_process(self) -> None:
        self.queue.start()
        pid_file = Path(self.temp.name) / "child.pid"
        original = self.queue._drain_fd
        raised = False

        def fail_once(fd, log):
            nonlocal raised
            if not raised:
                raised = True
                raise OSError("simulated log write failure")
            return original(fd, log)

        with patch.object(self.queue, "_drain_fd", side_effect=fail_once):
            job, _, _ = self.queue.submit(f"echo $$ > {pid_file}; echo ready; sleep 30")
            deadline = time.time() + 4
            while time.time() < deadline:
                result = self.queue.get_job(job.id)
                if result and result.status == "done":
                    break
                time.sleep(0.02)
        self.assertTrue(raised)
        self.assertEqual(self.queue.get_job(job.id).exit_code, -1)
        child_pid = int(pid_file.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(child_pid, 0)

    def test_queue_depth_is_bounded(self) -> None:
        config = QueueConfig(
            log_dir=Path(self.temp.name) / "bounded", max_queued_jobs=2,
        )
        queue = DeviceQueue(config, JobStore(config.db_path))
        queue.submit("true")
        queue.submit("true")
        with self.assertRaises(QueueUnavailable):
            queue.submit("true")

    def test_new_boot_clears_dead_state_and_advances_epoch(self) -> None:
        store = JobStore((Path(self.temp.name) / "boot" / "jobs.sqlite3"))
        store.save_device_state(DeviceState(
            state="dead", reset_epoch=4, boot_id="previous-boot",
            dead_reason="reboot required",
        ))
        config = QueueConfig(log_dir=Path(self.temp.name) / "boot")
        with patch("queue_core.current_boot_id", return_value="new-boot"):
            queue = DeviceQueue(config, store)
        status = queue.status()["device"]
        self.assertEqual(status["state"], "healthy")
        self.assertEqual(status["reset_epoch"], 5)

    def test_same_boot_dead_state_drains_recovered_queued_jobs(self) -> None:
        path = Path(self.temp.name) / "dead-recovery"
        store = JobStore(path / "jobs.sqlite3")
        config = QueueConfig(log_dir=path)
        queue = DeviceQueue(config, store)
        job, _, _ = queue.submit("true")
        dead = DeviceState(
            state="dead", reset_epoch=3, boot_id=queue._state.boot_id,
            dead_reason="simulated unrecoverable device",
        )
        store.save_device_state(dead)

        recovered = DeviceQueue(config, store)
        result = recovered.get_job(job.id)
        self.assertEqual(result.status, "done")
        self.assertEqual(result.exit_code, -1)
        self.assertEqual(result.error, "simulated unrecoverable device")
        self.assertEqual(recovered.status()["pending"], [])
        self.assertIn("simulated unrecoverable device", Path(result.output_file).read_text())

    def test_v1_device_state_is_rebuilt_to_current_schema(self) -> None:
        path = Path(self.temp.name) / "migration" / "jobs.sqlite3"
        path.parent.mkdir()
        connection = sqlite3.connect(path)
        connection.execute("""
            CREATE TABLE device_state (
                singleton INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                reset_epoch INTEGER NOT NULL,
                reset_pending INTEGER NOT NULL,
                boot_id TEXT NOT NULL,
                last_reset_at REAL,
                dead_since REAL,
                dead_reason TEXT,
                legacy_payload TEXT,
                updated_at REAL NOT NULL
            )
        """)
        connection.execute(
            "INSERT INTO device_state VALUES (1, 'dead', 4, 0, 'boot', NULL, 2.0, 'reason', 'old', 3.0)"
        )
        connection.execute("PRAGMA user_version=1")
        connection.commit()
        connection.close()

        store = JobStore(path)
        state = store.load_device_state()
        self.assertEqual(state.state, "dead")
        self.assertEqual(state.reset_epoch, 4)
        with sqlite3.connect(path) as connection:
            columns = [row[1] for row in connection.execute("PRAGMA table_info(device_state)")]
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        self.assertNotIn("legacy_payload", columns)
        self.assertEqual(version, CURRENT_SCHEMA_VERSION)

    def test_incompatible_original_database_is_rejected(self) -> None:
        path = Path(self.temp.name) / "legacy" / "jobs.sqlite3"
        path.parent.mkdir()
        connection = sqlite3.connect(path)
        connection.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, status TEXT)")
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(RuntimeError, "incompatible jobs database"):
            JobStore(path)


if __name__ == "__main__":
    unittest.main()
