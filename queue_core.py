"""Durable, bounded single-device job queue internals.

The HTTP and MCP surfaces intentionally live elsewhere.  This module owns the
state machine, persistence, scheduling, process lifecycle, and log bounds.
"""

from __future__ import annotations

import contextlib
import json
import os
import select
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator


DEFAULT_CLIENT_ID = "anon"
CURRENT_SCHEMA_VERSION = 2
MAX_CLIENT_ID_LEN = 128
DEFAULT_ITER_ESTIMATE_SEC = 10.0
STOP_GRACE_SEC = 8.0
REBOOT_REQUIRED_MSG = (
    "DEVICE UNRECOVERABLE: reset and health verification failed. "
    "A host reboot or operator recovery is required. All queued jobs were aborted."
)


def format_timestamp(value: float | None) -> str | None:
    if value is None:
        return None
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def current_boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        # Non-Linux/test fallback. Stable enough for one process, deliberately
        # not persisted as proof of a reboot.
        return "unknown"


@dataclass(frozen=True)
class QueueConfig:
    log_dir: Path
    default_timeout: int = 3600
    max_timeout: int = 86400
    max_repeat: int = 10_000
    max_queued_jobs: int = 1_000
    max_request_bytes: int = 1 << 20
    max_log_read: int = 64 << 10
    max_log_bytes: int = 16 << 20
    max_cmd_chars: int = 64 << 10
    max_cwd_chars: int = 4096
    max_env_entries: int = 256
    max_env_chars: int = 256 << 10
    process_poll_interval: float = 0.2
    stop_grace_sec: float = STOP_GRACE_SEC
    reset_preempts_current: bool = True
    reset_cmd: str = ""
    deep_reset_cmd: str = ""
    health_check_cmd: str = ""
    reset_retries: int = 1
    control_cmd_timeout: int = 60
    retention_days: int = 30
    max_completed_jobs: int = 10_000

    def __post_init__(self) -> None:
        if not 1 <= self.default_timeout <= self.max_timeout:
            raise ValueError("default timeout must be between 1 and max timeout")
        for name in (
            "max_repeat", "max_queued_jobs", "max_request_bytes", "max_log_read",
            "max_log_bytes", "max_cmd_chars", "max_cwd_chars", "max_env_entries", "max_env_chars",
            "control_cmd_timeout",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        if self.process_poll_interval <= 0 or self.stop_grace_sec < 0:
            raise ValueError("poll interval must be positive and stop grace non-negative")
        if self.reset_retries < 0 or self.retention_days < 0 or self.max_completed_jobs < 0:
            raise ValueError("retry and retention values must be non-negative")

    @property
    def db_path(self) -> Path:
        return self.log_dir / "jobs.sqlite3"

    @classmethod
    def from_env(cls, repo_root: Path) -> "QueueConfig":
        tt_smi = os.path.expanduser("~/tenstorrent/.venv/bin/tt-smi")

        def integer(name: str, default: int) -> int:
            return int(os.environ.get(name, str(default)))

        def floating(name: str, default: float) -> float:
            return float(os.environ.get(name, str(default)))

        def boolean(name: str, default: bool) -> bool:
            raw = os.environ.get(name)
            return default if raw is None else raw.lower() not in {"0", "false", "no"}

        return cls(
            log_dir=Path(os.environ.get("TT_DEVICE_LOG_DIR", repo_root / "logs")).resolve(),
            default_timeout=integer("TT_DEVICE_DEFAULT_TIMEOUT", 3600),
            max_timeout=integer("TT_DEVICE_MAX_TIMEOUT", 86400),
            max_repeat=integer("TT_DEVICE_MAX_REPEAT", 10_000),
            max_queued_jobs=integer("TT_DEVICE_MAX_QUEUED_JOBS", 1_000),
            max_request_bytes=integer("TT_DEVICE_MAX_REQUEST_BYTES", 1 << 20),
            max_log_read=integer("TT_DEVICE_MAX_LOG_READ", 64 << 10),
            max_log_bytes=integer("TT_DEVICE_MAX_LOG_BYTES", 16 << 20),
            max_cmd_chars=integer("TT_DEVICE_MAX_CMD_CHARS", 64 << 10),
            process_poll_interval=floating("TT_DEVICE_PROCESS_POLL_INTERVAL", 0.2),
            stop_grace_sec=floating("TT_DEVICE_STOP_GRACE_SEC", STOP_GRACE_SEC),
            reset_preempts_current=boolean("TT_DEVICE_RESET_PREEMPTS_CURRENT", True),
            reset_cmd=os.environ.get("TT_DEVICE_RESET_CMD", f"{tt_smi} -r"),
            # Privileged deep reset is deliberately opt-in. Running arbitrary
            # jobs and a sudo-capable reset broker under one Unix identity is
            # not a security boundary.
            deep_reset_cmd=os.environ.get("TT_DEVICE_DEEP_RESET_CMD", ""),
            health_check_cmd=os.environ.get("TT_DEVICE_HEALTH_CHECK_CMD", f"{tt_smi} -s"),
            reset_retries=integer("TT_DEVICE_RESET_RETRIES", 1),
            control_cmd_timeout=integer("TT_DEVICE_HEALTH_CMD_TIMEOUT", 60),
            retention_days=integer("TT_DEVICE_RETENTION_DAYS", 30),
            max_completed_jobs=integer("TT_DEVICE_MAX_COMPLETED_JOBS", 10_000),
        )


@dataclass
class Job:
    id: str
    cmd: str
    cwd: str
    timeout: int
    repeat: int
    env: dict[str, str] = field(default_factory=dict)
    mode: str = "run"
    client_id: str = DEFAULT_CLIENT_ID
    reset_epoch: int = 0
    submitted: float = field(default_factory=time.time)
    status: str = "queued"
    exit_code: int | None = None
    elapsed: float | None = None
    output_file: str = ""
    started_at: float | None = None
    finished_at: float | None = None
    repeat_current: int = 0
    repeat_completed: int = 0
    current_iteration_started_at: float | None = None
    first_iteration_elapsed: float | None = None
    per_iter_estimate_sec: float = DEFAULT_ITER_ESTIMATE_SEC
    stop_requested_at: float | None = None
    stop_escalated_at: float | None = None
    log_size: int = 0
    log_truncated: bool = False
    dropped_log_bytes: int = 0
    timed_out: bool = False
    error: str | None = None


@dataclass
class DeviceState:
    state: str = "healthy"
    reset_epoch: int = 0
    reset_pending: bool = False
    boot_id: str = ""
    last_reset_at: float | None = None
    dead_since: float | None = None
    dead_reason: str | None = None


class QueueUnavailable(RuntimeError):
    pass


class DeviceDeadError(QueueUnavailable):
    pass


class JobStore:
    """SQLite metadata store. Logs intentionally remain canonical files."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._initialize()
        try:
            self.db_path.chmod(0o600)
        except OSError:
            pass

    @contextlib.contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            with conn:
                yield conn
        finally:
            conn.close()

    @staticmethod
    def _create_device_state_table(conn: sqlite3.Connection) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS device_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                state TEXT NOT NULL,
                reset_epoch INTEGER NOT NULL,
                reset_pending INTEGER NOT NULL,
                boot_id TEXT NOT NULL,
                last_reset_at REAL,
                dead_since REAL,
                dead_reason TEXT,
                updated_at REAL NOT NULL
            )
        """)

    def _initialize(self) -> None:
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version > CURRENT_SCHEMA_VERSION:
                raise RuntimeError(f"database schema version {version} is newer than this server")
            existing_jobs = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'"
            ).fetchone()
            if existing_jobs:
                columns = {
                    row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
                }
                required = {"seq", "id", "status", "log_truncated", "dropped_log_bytes"}
                if not required.issubset(columns):
                    raise RuntimeError(
                        "incompatible jobs database; use a separate TT_DEVICE_LOG_DIR "
                        "instead of an original v1 queue database"
                    )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
                    cmd TEXT NOT NULL,
                    cwd TEXT NOT NULL,
                    timeout INTEGER NOT NULL,
                    repeat INTEGER NOT NULL,
                    env_json TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    reset_epoch INTEGER NOT NULL,
                    submitted REAL NOT NULL,
                    status TEXT NOT NULL,
                    exit_code INTEGER,
                    elapsed REAL,
                    output_file TEXT NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    repeat_current INTEGER NOT NULL,
                    repeat_completed INTEGER NOT NULL,
                    first_iteration_elapsed REAL,
                    per_iter_estimate_sec REAL NOT NULL,
                    stop_requested_at REAL,
                    stop_escalated_at REAL,
                    log_size INTEGER NOT NULL,
                    log_truncated INTEGER NOT NULL,
                    dropped_log_bytes INTEGER NOT NULL,
                    timed_out INTEGER NOT NULL,
                    error TEXT,
                    updated_at REAL NOT NULL
                )
            """)
            self._create_device_state_table(conn)
            if version == 1:
                columns = (
                    "singleton", "state", "reset_epoch", "reset_pending", "boot_id",
                    "last_reset_at", "dead_since", "dead_reason", "updated_at",
                )
                projection = ", ".join(columns)
                conn.execute("ALTER TABLE device_state RENAME TO device_state_v1")
                self._create_device_state_table(conn)
                conn.execute(
                    f"INSERT INTO device_state ({projection}) "
                    f"SELECT {projection} FROM device_state_v1"
                )
                conn.execute("DROP TABLE device_state_v1")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_status_seq ON jobs(status, seq)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_finished ON jobs(finished_at DESC)"
            )
            conn.execute(f"PRAGMA user_version={CURRENT_SCHEMA_VERSION}")

    @staticmethod
    def _values(job: Job) -> tuple[Any, ...]:
        return (
            job.id, job.cmd, job.cwd, job.timeout, job.repeat,
            json.dumps(job.env, sort_keys=True), job.mode, job.client_id,
            job.reset_epoch, job.submitted, job.status, job.exit_code,
            job.elapsed, job.output_file, job.started_at, job.finished_at,
            job.repeat_current, job.repeat_completed, job.first_iteration_elapsed,
            job.per_iter_estimate_sec, job.stop_requested_at, job.stop_escalated_at,
            job.log_size, int(job.log_truncated), job.dropped_log_bytes,
            int(job.timed_out), job.error, time.time(),
        )

    def create_job(self, job: Job) -> None:
        with self.connect() as conn:
            conn.execute("""
                INSERT INTO jobs (
                    id, cmd, cwd, timeout, repeat, env_json, mode, client_id,
                    reset_epoch, submitted, status, exit_code, elapsed, output_file,
                    started_at, finished_at, repeat_current, repeat_completed,
                    first_iteration_elapsed, per_iter_estimate_sec,
                    stop_requested_at, stop_escalated_at, log_size, log_truncated,
                    dropped_log_bytes, timed_out, error, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, self._values(job))

    def update_job(self, job: Job) -> None:
        values = self._values(job)
        with self.connect() as conn:
            cursor = conn.execute("""
                UPDATE jobs SET
                    cmd=?, cwd=?, timeout=?, repeat=?, env_json=?, mode=?, client_id=?,
                    reset_epoch=?, submitted=?, status=?, exit_code=?, elapsed=?,
                    output_file=?, started_at=?, finished_at=?, repeat_current=?,
                    repeat_completed=?, first_iteration_elapsed=?, per_iter_estimate_sec=?,
                    stop_requested_at=?, stop_escalated_at=?, log_size=?, log_truncated=?,
                    dropped_log_bytes=?, timed_out=?, error=?, updated_at=?
                WHERE id=?
            """, (*values[1:], values[0]))
            if cursor.rowcount != 1:
                raise KeyError(job.id)

    def load_job(self, job_id: str) -> Job | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row_to_job(row) if row else None

    def probe(self) -> None:
        with self.connect() as conn:
            conn.execute("SELECT 1").fetchone()

    def load_queued(self) -> list[Job]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status='queued' ORDER BY seq"
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def load_running(self) -> list[Job]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status='running' ORDER BY seq"
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def recent_completed(self, limit: int = 10) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("""
                SELECT * FROM jobs WHERE status='done'
                ORDER BY finished_at DESC, seq DESC LIMIT ?
            """, (limit,)).fetchall()
        result = []
        for row in reversed(rows):
            result.append({
                "id": row["id"],
                "cmd": row["cmd"][:120],
                "exit_code": row["exit_code"],
                "elapsed": row["elapsed"],
                "finished": format_timestamp(row["finished_at"]),
                "output_file": row["output_file"],
                "repeat": row["repeat"],
                "mode": row["mode"],
                "client": row["client_id"],
                "repeat_completed": row["repeat_completed"],
                "per_iter_estimate_sec": round(row["per_iter_estimate_sec"], 2),
                "timed_out": bool(row["timed_out"]),
                "log_truncated": bool(row["log_truncated"]),
            })
        return result

    def load_device_state(self) -> DeviceState | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM device_state WHERE singleton=1"
            ).fetchone()
        if not row:
            return None
        return DeviceState(
            state=row["state"], reset_epoch=row["reset_epoch"],
            reset_pending=bool(row["reset_pending"]), boot_id=row["boot_id"],
            last_reset_at=row["last_reset_at"], dead_since=row["dead_since"],
            dead_reason=row["dead_reason"],
        )

    def save_device_state(self, state: DeviceState) -> None:
        with self.connect() as conn:
            conn.execute("""
                INSERT INTO device_state (
                    singleton, state, reset_epoch, reset_pending, boot_id,
                    last_reset_at, dead_since, dead_reason, updated_at
                ) VALUES (1,?,?,?,?,?,?,?,?)
                ON CONFLICT(singleton) DO UPDATE SET
                    state=excluded.state, reset_epoch=excluded.reset_epoch,
                    reset_pending=excluded.reset_pending, boot_id=excluded.boot_id,
                    last_reset_at=excluded.last_reset_at, dead_since=excluded.dead_since,
                    dead_reason=excluded.dead_reason, updated_at=excluded.updated_at
            """, (
                state.state, state.reset_epoch, int(state.reset_pending), state.boot_id,
                state.last_reset_at, state.dead_since, state.dead_reason, time.time(),
            ))

    def prune_completed(self, cutoff: float, keep: int) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute("""
                SELECT id, output_file FROM jobs WHERE status='done'
                AND (finished_at < ? OR id NOT IN (
                    SELECT id FROM jobs WHERE status='done'
                    ORDER BY finished_at DESC, seq DESC LIMIT ?
                ))
            """, (cutoff, max(0, keep))).fetchall()
            if rows:
                conn.executemany("DELETE FROM jobs WHERE id=?", [(r["id"],) for r in rows])
        return [row["output_file"] for row in rows]

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        try:
            env = json.loads(row["env_json"] or "{}")
        except json.JSONDecodeError:
            env = {}
        return Job(
            id=row["id"], cmd=row["cmd"], cwd=row["cwd"], timeout=row["timeout"],
            repeat=row["repeat"], env=env, mode=row["mode"],
            client_id=row["client_id"], reset_epoch=row["reset_epoch"],
            submitted=row["submitted"], status=row["status"],
            exit_code=row["exit_code"], elapsed=row["elapsed"],
            output_file=row["output_file"], started_at=row["started_at"],
            finished_at=row["finished_at"], repeat_current=row["repeat_current"],
            repeat_completed=row["repeat_completed"],
            first_iteration_elapsed=row["first_iteration_elapsed"],
            per_iter_estimate_sec=row["per_iter_estimate_sec"],
            stop_requested_at=row["stop_requested_at"],
            stop_escalated_at=row["stop_escalated_at"], log_size=row["log_size"],
            log_truncated=bool(row["log_truncated"]),
            dropped_log_bytes=row["dropped_log_bytes"],
            timed_out=bool(row["timed_out"]), error=row["error"],
        )


class BoundedJobLog:
    def __init__(self, job: Job, maximum: int, mode: str = "wb"):
        self.job = job
        self.maximum = max(1024, maximum)
        self._file = open(job.output_file, mode, buffering=0)
        if "a" in mode:
            self.job.log_size = self._file.tell()

    def __enter__(self) -> "BoundedJobLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.finish()
        self._file.close()

    def append(self, data: bytes) -> None:
        if not data:
            return
        remaining = max(0, self.maximum - self.job.log_size)
        if remaining:
            written = data[:remaining]
            self._file.write(written)
            self.job.log_size += len(written)
        dropped = len(data) - min(len(data), remaining)
        if dropped:
            self.job.log_truncated = True
            self.job.dropped_log_bytes += dropped

    def finish(self) -> None:
        if not self.job.log_truncated:
            return
        marker = (
            f"\n[tt-device-queue] Output truncated; dropped at least "
            f"{self.job.dropped_log_bytes} bytes\n"
        ).encode()
        # Preserve the bound by replacing the tail with the marker.
        start = max(0, self.maximum - len(marker))
        self._file.seek(start)
        self._file.write(marker[-self.maximum:])
        self._file.truncate(self.maximum)
        self.job.log_size = self.maximum


class DeviceQueue:
    def __init__(self, config: QueueConfig, store: JobStore):
        self.config = config
        self.store = store
        self.config.log_dir.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, Job] = {}  # active jobs only
        self._pending: dict[str, deque[str]] = {}
        self._rr_clients: deque[str] = deque()
        self._current: Job | None = None
        self._current_proc: subprocess.Popen[bytes] | None = None
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._shutdown = threading.Event()
        self._worker: threading.Thread | None = None
        self._worker_heartbeat = time.monotonic()
        self._degraded_reason: str | None = None
        self._maintenance_warning: str | None = None
        self._dirty_jobs: dict[str, Job] = {}
        self._dirty_state = False
        self._last_prune = 0.0
        self._state = self._recover_state()

    def _recover_state(self) -> DeviceState:
        boot = current_boot_id()
        state = self.store.load_device_state()
        if state is None:
            state = DeviceState(boot_id=boot)
            self.store.save_device_state(state)
        elif state.boot_id != boot and boot != "unknown":
            state = DeviceState(
                state="healthy", reset_epoch=state.reset_epoch + 1,
                boot_id=boot, last_reset_at=state.last_reset_at,
            )
            self.store.save_device_state(state)
        elif state.state == "resetting":
            # Same-boot crash during reset: rerun recovery before dispatch.
            state.reset_pending = True

        now = time.time()
        for job in self.store.load_running():
            self._append_recovery_message(job)
            job.status = "done"
            job.exit_code = -1
            job.finished_at = now
            job.elapsed = round(now - job.started_at, 2) if job.started_at else None
            job.error = "server restarted while job was running"
            self.store.update_job(job)

        queued = self.store.load_queued()
        for job in queued:
            if state.state == "dead":
                self._append_terminal_message(job, state.dead_reason or REBOOT_REQUIRED_MSG)
                job.status = "done"
                job.exit_code = -1
                job.finished_at = now
                job.error = state.dead_reason or REBOOT_REQUIRED_MSG
                self.store.update_job(job)
                continue
            self._jobs[job.id] = job
            if job.client_id not in self._pending:
                self._pending[job.client_id] = deque()
                self._rr_clients.append(job.client_id)
            self._pending[job.client_id].append(job.id)
        return state

    def _append_recovery_message(self, job: Job) -> None:
        self._append_terminal_message(
            job, "Server restarted while this job was running", leading_newline=True
        )

    def _append_terminal_message(
        self, job: Job, message: str, *, leading_newline: bool = False
    ) -> None:
        try:
            path = Path(job.output_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            with BoundedJobLog(job, self.config.max_log_bytes, "ab") as log:
                prefix = "\n" if leading_newline else ""
                log.append(f"{prefix}[tt-device-queue] {message}\n".encode())
        except OSError:
            pass

    def start(self) -> None:
        with self._lock:
            if self._worker and self._worker.is_alive():
                return
            self._worker = threading.Thread(
                target=self._worker_loop, name="device-queue-worker", daemon=False
            )
            self._worker.start()

    def initiate_shutdown(self) -> None:
        self._shutdown.set()
        with self._cond:
            proc = self._current_proc
            job = self._current
            if proc and job and job.mode == "run" and job.stop_requested_at is None:
                job.stop_requested_at = time.time()
            self._cond.notify_all()
        if proc:
            self._signal_group(proc, signal.SIGINT)

    def join(self, timeout: float = 15.0) -> None:
        if self._worker:
            self._worker.join(timeout)

    def _set_degraded(self, reason: str) -> None:
        with self._cond:
            self._degraded_reason = reason
            self._cond.notify_all()

    def _persist_job(self, job: Job) -> bool:
        try:
            self.store.update_job(job)
            with self._lock:
                self._dirty_jobs.pop(job.id, None)
            return True
        except Exception as exc:
            with self._lock:
                self._dirty_jobs[job.id] = job
            self._set_degraded(f"persistence failure: {type(exc).__name__}: {exc}")
            return False

    def _persist_state(self) -> bool:
        try:
            self.store.save_device_state(self._state)
            with self._lock:
                self._dirty_state = False
            return True
        except Exception as exc:
            with self._lock:
                self._dirty_state = True
            self._set_degraded(f"device-state persistence failure: {type(exc).__name__}: {exc}")
            return False

    def _try_recover_persistence(self) -> bool:
        """Flush failed metadata transitions before dispatching more work."""
        with self._lock:
            dirty_jobs = list(self._dirty_jobs.values())
            dirty_state = self._dirty_state
        try:
            self.store.probe()
            for job in dirty_jobs:
                self.store.update_job(job)
            if dirty_state:
                self.store.save_device_state(self._state)
        except Exception:
            return False
        with self._cond:
            for job in dirty_jobs:
                self._dirty_jobs.pop(job.id, None)
            self._dirty_state = False
            if self._degraded_reason and (
                self._degraded_reason.startswith("persistence failure")
                or self._degraded_reason.startswith("device-state persistence failure")
            ):
                self._degraded_reason = None
            self._cond.notify_all()
        return True

    def _validate_submission(
        self, cmd: Any, cwd: Any, timeout: Any, repeat: Any,
        env: Any, client_id: Any, mode: Any,
    ) -> tuple[str, str, int, int, dict[str, str], str]:
        if not isinstance(cmd, str) or not cmd.strip():
            raise ValueError("cmd must be a non-empty string")
        cmd = cmd.strip()
        if len(cmd) > self.config.max_cmd_chars:
            raise ValueError(f"cmd must be at most {self.config.max_cmd_chars} characters")
        if not isinstance(cwd, str):
            raise ValueError("cwd must be a string")
        if len(cwd) > self.config.max_cwd_chars:
            raise ValueError(f"cwd must be at most {self.config.max_cwd_chars} characters")
        if cwd and (not os.path.isdir(cwd) or not os.access(cwd, os.X_OK)):
            raise ValueError(f"cwd is not an accessible directory: {cwd}")
        if isinstance(repeat, bool) or not isinstance(repeat, int):
            raise ValueError("repeat must be an integer")
        if not 1 <= repeat <= self.config.max_repeat:
            raise ValueError(f"repeat must be between 1 and {self.config.max_repeat}")
        if timeout is None or timeout == 0:
            timeout = self.config.default_timeout
        if isinstance(timeout, bool) or not isinstance(timeout, int):
            raise ValueError("timeout must be an integer")
        if not 1 <= timeout <= self.config.max_timeout:
            raise ValueError(f"timeout must be between 1 and {self.config.max_timeout}")
        if mode != "run":
            raise ValueError("mode must be 'run'")
        if env is None:
            env = {}
        if not isinstance(env, dict):
            raise ValueError("env must be an object mapping names to values")
        if len(env) > self.config.max_env_entries:
            raise ValueError(f"env may contain at most {self.config.max_env_entries} entries")
        env_chars = 0
        checked_env: dict[str, str] = {}
        for key, value in env.items():
            if not isinstance(key, str) or not key or "=" in key or "\0" in key:
                raise ValueError("env names must be non-empty strings without '=' or NUL")
            if not isinstance(value, str) or "\0" in value:
                raise ValueError("env values must be strings without NUL")
            env_chars += len(key) + len(value)
            checked_env[key] = value
        if env_chars > self.config.max_env_chars:
            raise ValueError(f"env is larger than {self.config.max_env_chars} characters")
        if not isinstance(client_id, str) or not client_id.strip():
            raise ValueError("client_id must be a non-empty string")
        client_id = client_id.strip()
        if len(client_id) > MAX_CLIENT_ID_LEN:
            raise ValueError(f"client_id must be at most {MAX_CLIENT_ID_LEN} characters")
        return cmd, cwd, timeout, repeat, checked_env, client_id

    def submit(
        self, cmd: Any, cwd: Any = "", timeout: Any = None, repeat: Any = 1,
        mode: Any = "run", env: Any = None, client_id: Any = DEFAULT_CLIENT_ID,
    ) -> tuple[Job, int, int]:
        cmd, cwd, timeout, repeat, env, client_id = self._validate_submission(
            cmd, cwd, timeout, repeat, env, client_id, mode
        )
        with self._lock:
            self._ensure_available_locked()
            if sum(map(len, self._pending.values())) >= self.config.max_queued_jobs:
                raise QueueUnavailable("queue is full")
            epoch = self._state.reset_epoch

        job_id = uuid.uuid4().hex
        output_dir = self.config.log_dir / job_id
        output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        job = Job(
            id=job_id, cmd=cmd, cwd=cwd, timeout=timeout, repeat=repeat,
            env=env, client_id=client_id, reset_epoch=epoch,
            output_file=str(output_dir / "output"),
        )
        try:
            self.store.create_job(job)
        except Exception as exc:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise QueueUnavailable(
                f"could not persist job submission: {type(exc).__name__}: {exc}"
            ) from exc

        with self._cond:
            try:
                self._ensure_available_locked()
                if sum(map(len, self._pending.values())) >= self.config.max_queued_jobs:
                    raise QueueUnavailable("queue is full")
            except QueueUnavailable as exc:
                job.status = "done"
                job.exit_code = -1
                job.finished_at = time.time()
                job.error = str(exc)
                self._persist_job(job)
                raise
            self._jobs[job.id] = job
            if client_id not in self._pending:
                self._pending[client_id] = deque()
                self._rr_clients.append(client_id)
            self._pending[client_id].append(job.id)
            order = self._dispatch_order_locked()
            position = order.index(job.id)
            wait = self._wait_for_prefix_locked(order, position)
            jobs_ahead = position + (1 if self._current else 0)
            self._cond.notify_all()
        return job, jobs_ahead, wait

    def _ensure_available_locked(self) -> None:
        if self._degraded_reason:
            raise QueueUnavailable(self._degraded_reason)
        if self._shutdown.is_set():
            raise QueueUnavailable("queue server is shutting down")
        if self._state.state == "dead":
            raise DeviceDeadError(self._state.dead_reason or REBOOT_REQUIRED_MSG)

    def _dispatch_order_locked(self) -> list[str]:
        queues = {client: deque(ids) for client, ids in self._pending.items() if ids}
        clients = deque(client for client in self._rr_clients if client in queues)
        order: list[str] = []
        while clients:
            client = clients.popleft()
            order.append(queues[client].popleft())
            if queues[client]:
                clients.append(client)
        return order

    def _take_next_locked(self) -> Job | None:
        while self._rr_clients:
            client = self._rr_clients.popleft()
            queue = self._pending.get(client)
            if not queue:
                self._pending.pop(client, None)
                continue
            job_id = queue.popleft()
            if queue:
                self._rr_clients.append(client)
            else:
                self._pending.pop(client, None)
            return self._jobs[job_id]
        return None

    def _remaining_locked(self, job: Job, now: float | None = None) -> int:
        now = time.time() if now is None else now
        if job.status == "done":
            return 0
        estimate = max(0.1, job.per_iter_estimate_sec)
        if job.status == "queued":
            return round(job.repeat * estimate)
        started = job.current_iteration_started_at or job.started_at or now
        current = max(0.0, estimate - max(0.0, now - started))
        after = max(0, job.repeat - job.repeat_current) * estimate
        return round(current + after)

    def _wait_for_prefix_locked(self, order: list[str], count: int) -> int:
        now = time.time()
        total = self._remaining_locked(self._current, now) if self._current else 0
        for job_id in order[:count]:
            total += self._remaining_locked(self._jobs[job_id], now)
        return total

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            active = self._jobs.get(job_id)
        return active if active is not None else self.store.load_job(job_id)

    def snapshot(self, job: Job) -> dict[str, Any]:
        with self._lock:
            data = self._snapshot_base(job)
            if job.status == "queued":
                order = self._dispatch_order_locked()
                try:
                    position = order.index(job.id)
                except ValueError:
                    position = -1
                data.update({
                    "position": position + 1,
                    "estimated_wait_sec": self._wait_for_prefix_locked(order, max(position, 0)),
                    "estimated_remaining_sec": self._remaining_locked(job),
                })
            elif job.status == "running":
                data.update({
                    "position": 0, "estimated_wait_sec": 0,
                    "estimated_remaining_sec": self._remaining_locked(job),
                    "running_sec": round(time.time() - (job.started_at or job.submitted), 1),
                })
            else:
                data["estimated_remaining_sec"] = 0
        return data

    @staticmethod
    def _snapshot_base(job: Job) -> dict[str, Any]:
        data = {
            "job_id": job.id, "status": job.status, "client_id": job.client_id,
            "cmd": job.cmd, "cwd": job.cwd, "timeout": job.timeout,
            "repeat": job.repeat, "mode": job.mode,
            "repeat_current": job.repeat_current,
            "repeat_completed": job.repeat_completed,
            "first_iteration_elapsed": job.first_iteration_elapsed,
            "per_iter_estimate_sec": round(job.per_iter_estimate_sec, 2),
            "submitted_at": format_timestamp(job.submitted),
            "started_at": format_timestamp(job.started_at),
            "finished_at": format_timestamp(job.finished_at),
            "output_file": job.output_file, "exit_code": job.exit_code,
            "elapsed": job.elapsed, "timed_out": job.timed_out,
            "log_size": job.log_size, "log_truncated": job.log_truncated,
            "dropped_log_bytes": job.dropped_log_bytes, "error": job.error,
        }
        if job.timed_out:
            data["timeout_message"] = f"Command timed out after {job.timeout}s; the queue sent SIGKILL."
        if job.stop_requested_at is not None:
            data["stop_requested_at"] = format_timestamp(job.stop_requested_at)
        return data

    def status(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            current = None
            if self._current:
                job = self._current
                current = {
                    "id": job.id, "cmd": job.cmd[:120], "client": job.client_id,
                    "running_sec": round(now - (job.started_at or job.submitted), 1),
                    "estimated_remaining_sec": self._remaining_locked(job, now),
                    "repeat": job.repeat, "mode": job.mode,
                    "repeat_current": job.repeat_current,
                    "repeat_completed": job.repeat_completed,
                }
            order = self._dispatch_order_locked()
            cumulative = self._remaining_locked(self._current, now) if self._current else 0
            pending = []
            for job_id in order:
                job = self._jobs[job_id]
                run_estimate = self._remaining_locked(job, now)
                pending.append({
                    "id": job.id, "cmd": job.cmd[:120], "client": job.client_id,
                    "waiting_sec": round(now - job.submitted, 1),
                    "estimated_wait_sec": cumulative,
                    "estimated_run_sec": run_estimate,
                    "repeat": job.repeat, "mode": job.mode,
                })
                cumulative += run_estimate
            state = asdict(self._state)
            state.update({
                "last_reset_at": format_timestamp(self._state.last_reset_at),
                "dead_since": format_timestamp(self._state.dead_since),
                "queue_disabled": bool(self._degraded_reason),
                "disabled_reason": self._degraded_reason,
            })
            worker = self._worker
            worker_info = {
                "alive": bool(worker and worker.is_alive()),
                "heartbeat_age_sec": round(time.monotonic() - self._worker_heartbeat, 2),
                "degraded_reason": self._degraded_reason,
                "maintenance_warning": self._maintenance_warning,
            }
        try:
            recent = self.store.recent_completed(10)
        except Exception as exc:
            recent = []
            self._set_degraded(f"persistence failure: {type(exc).__name__}: {exc}")
        return {
            "current": current, "pending": pending,
            "recent": recent,
            "device": state, "worker": worker_info,
        }

    def read_logs(self, job: Job, offset: int, limit: int) -> dict[str, Any]:
        offset = max(0, int(offset))
        limit = max(1, min(int(limit), self.config.max_log_read))
        path = Path(job.output_file)
        try:
            size = path.stat().st_size
            with open(path, "rb") as stream:
                stream.seek(min(offset, size))
                data = stream.read(limit + 1)
        except FileNotFoundError:
            size, data = 0, b""
        truncated = len(data) > limit or offset + len(data) < size
        data = data[:limit]
        next_offset = offset + len(data)
        return {
            "job_id": job.id, "status": job.status, "output_file": job.output_file,
            "offset": offset, "next_offset": next_offset,
            "content": data.decode("utf-8", errors="replace"),
            "truncated": truncated, "complete": job.status == "done" and next_offset >= size,
            "log_size": size, "log_truncated": job.log_truncated,
            "dropped_log_bytes": job.dropped_log_bytes,
        }

    def stop_job(self, job_id: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            job, proc = self._current, self._current_proc
            if not job or not proc or job.mode != "run":
                return None
            if job_id and job.id != job_id:
                raise ValueError(f"Job {job_id} is not currently running")
            if job.stop_requested_at is None:
                job.stop_requested_at = time.time()
                self._persist_job(job)
            info = {"id": job.id, "cmd": job.cmd[:120], "signal": "SIGINT"}
        self._signal_group(proc, signal.SIGINT)
        return info

    def request_reset(self, job_id: str | None) -> dict[str, Any]:
        reported = self.get_job(job_id) if job_id else None
        if job_id and reported is None:
            raise KeyError(job_id)
        proc_to_stop = None
        with self._cond:
            self._ensure_available_locked()
            epoch = self._state.reset_epoch
            if reported and reported.reset_epoch < epoch:
                return {
                    "action": "already_reset", "device_state": self._state.state,
                    "reset_epoch": epoch,
                    "hint": "Device was already reset after this job ran. Resubmit the job.",
                }
            if self._state.state == "resetting" or self._state.reset_pending:
                return {
                    "action": "joined", "device_state": self._state.state,
                    "reset_epoch": epoch,
                    "hint": "A reset is already pending or in progress.",
                }
            self._state.reset_pending = True
            if not self._persist_state():
                raise QueueUnavailable(self._degraded_reason or "failed to persist reset request")
            if (
                self.config.reset_preempts_current and self._current
                and self._current.mode == "run"
                and (reported is None or reported.id == self._current.id)
            ):
                self._current.stop_requested_at = time.time()
                self._persist_job(self._current)
                proc_to_stop = self._current_proc
            self._cond.notify_all()
        if proc_to_stop:
            self._signal_group(proc_to_stop, signal.SIGINT)
        return {
            "action": "scheduled", "device_state": self._state.state,
            "reset_epoch": epoch,
            "hint": "Reset is scheduled before the next job.",
        }

    def _worker_loop(self) -> None:
        while not self._shutdown.is_set():
            self._worker_heartbeat = time.monotonic()
            try:
                work, job = self._next_work()
                if work == "shutdown":
                    return
                if work == "reset":
                    self._execute_reset()
                elif job:
                    self._execute_job(job)
            except Exception as exc:
                self._set_degraded(f"worker failure: {type(exc).__name__}: {exc}")
                with self._cond:
                    self._cond.wait(timeout=1.0)

    def _next_work(self) -> tuple[str, Job | None]:
        with self._cond:
            while True:
                self._worker_heartbeat = time.monotonic()
                if self._shutdown.is_set():
                    return "shutdown", None
                if self._degraded_reason:
                    self._cond.wait(timeout=1.0)
                    self._try_recover_persistence()
                    continue
                if self._state.reset_pending:
                    self._state.reset_pending = False
                    self._state.state = "resetting"
                    if not self._persist_state():
                        continue
                    return "reset", None
                if self._state.state == "healthy":
                    job = self._take_next_locked()
                    if job:
                        job.status = "running"
                        job.started_at = time.time()
                        job.reset_epoch = self._state.reset_epoch
                        self._current = job
                        if not self._persist_job(job):
                            job.status = "queued"
                            job.started_at = None
                            self._current = None
                            queue = self._pending.setdefault(job.client_id, deque())
                            queue.appendleft(job.id)
                            if job.client_id not in self._rr_clients:
                                self._rr_clients.appendleft(job.client_id)
                            return "none", None
                        return "job", job
                self._cond.wait(timeout=1.0)

    def _execute_job(self, job: Job) -> None:
        exit_code = -1
        try:
            with BoundedJobLog(job, self.config.max_log_bytes) as log:
                deadline = (job.started_at or time.time()) + job.timeout
                for iteration in range(1, job.repeat + 1):
                    job.repeat_current = iteration
                    job.current_iteration_started_at = time.time()
                    self._persist_job(job)
                    if job.repeat > 1:
                        log.append(f"\n[tt-device-queue] Repeat {iteration}/{job.repeat}\n".encode())
                    proc = None
                    try:
                        proc = subprocess.Popen(
                            ["/bin/bash", "-lc", self._job_shell_script(job.cmd)],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            cwd=job.cwd or None, env=self._job_env(job.env),
                            start_new_session=True,
                        )
                        with self._lock:
                            self._current_proc = proc
                            stop_already_requested = job.stop_requested_at is not None
                        if stop_already_requested:
                            self._signal_group(proc, signal.SIGINT)
                        exit_code = self._wait_process(job, proc, log, deadline)
                    finally:
                        if proc is not None:
                            self._cleanup_process(proc)
                        with self._lock:
                            self._current_proc = None
                    if exit_code != 0:
                        break
                    elapsed = time.time() - (job.current_iteration_started_at or time.time())
                    job.repeat_completed = iteration
                    if job.first_iteration_elapsed is None:
                        job.first_iteration_elapsed = round(elapsed, 2)
                        job.per_iter_estimate_sec = max(0.1, elapsed)
                    job.current_iteration_started_at = None
                    self._persist_job(job)
                if exit_code == 0:
                    job.repeat_completed = job.repeat
                if job.timed_out:
                    log.append(
                        f"\n[tt-device-queue] Command timed out after {job.timeout}s; "
                        "the queue sent SIGKILL.\n".encode()
                    )
        except Exception as exc:
            job.error = f"{type(exc).__name__}: {exc}"
            exit_code = -1
            try:
                with BoundedJobLog(job, self.config.max_log_bytes, "ab") as log:
                    log.append(f"\n[tt-device-queue] Error: {job.error}\n".encode())
            except OSError:
                pass
        finally:
            now = time.time()
            job.status = "done"
            job.exit_code = exit_code
            job.elapsed = round(now - (job.started_at or now), 2)
            job.finished_at = now
            job.current_iteration_started_at = None
            self._persist_job(job)
            self._write_meta(job)
            with self._cond:
                self._current = None
                self._current_proc = None
                self._jobs.pop(job.id, None)
                self._cond.notify_all()
            self._maybe_prune()

    def _wait_process(
        self, job: Job, proc: subprocess.Popen[bytes], log: BoundedJobLog,
        deadline: float,
    ) -> int:
        assert proc.stdout is not None
        fd = proc.stdout.fileno()
        os.set_blocking(fd, False)
        poller = select.poll()
        poller.register(fd, select.POLLIN | select.POLLHUP | select.POLLERR)
        while True:
            self._worker_heartbeat = time.monotonic()
            now = time.time()
            if now >= deadline:
                job.timed_out = True
                self._signal_group(proc, signal.SIGKILL)
            if self._shutdown.is_set() and job.stop_requested_at is None:
                job.stop_requested_at = now
                self._signal_group(proc, signal.SIGINT)
            if job.stop_requested_at and not job.stop_escalated_at:
                if now - job.stop_requested_at >= self.config.stop_grace_sec:
                    job.stop_escalated_at = now
                    self._signal_group(proc, signal.SIGKILL)
            events = poller.poll(max(1, int(self.config.process_poll_interval * 1000)))
            if events:
                self._drain_fd(fd, log)
            return_code = proc.poll()
            if return_code is not None:
                self._drain_fd(fd, log)
                # A shell can exit while leaving descendants (or briefly,
                # zombies) in its process group. The tracked command is done;
                # clean up the remainder rather than letting an orphan wedge
                # the single device worker forever.
                if self._group_alive(proc.pid):
                    self._signal_group(proc, signal.SIGKILL)
                if job.timed_out or job.stop_escalated_at:
                    return -9
                return return_code

    @staticmethod
    def _drain_fd(fd: int, log: BoundedJobLog) -> None:
        while True:
            try:
                data = os.read(fd, 64 << 10)
            except BlockingIOError:
                return
            if not data:
                return
            log.append(data)

    def _execute_reset(self) -> None:
        job = self._new_reset_job()
        healthy = False
        try:
            with BoundedJobLog(job, self.config.max_log_bytes) as log:
                attempts = max(1, self.config.reset_retries + 1)
                for attempt in range(1, attempts + 1):
                    log.append(f"[tt-device-queue] Reset attempt {attempt}/{attempts}\n".encode())
                    if self._run_control(self.config.reset_cmd, log) == 0:
                        healthy = self._verify_health(log)
                        if healthy:
                            break
                if not healthy and self.config.deep_reset_cmd:
                    log.append(b"[tt-device-queue] Escalating to configured deep reset broker\n")
                    if self._run_control(self.config.deep_reset_cmd, log) == 0:
                        if self._run_control(self.config.reset_cmd, log) == 0:
                            healthy = self._verify_health(log)
                log.append(
                    b"[tt-device-queue] Device healthy after reset\n"
                    if healthy else f"[tt-device-queue] {REBOOT_REQUIRED_MSG}\n".encode()
                )
        except Exception as exc:
            job.error = f"reset error: {type(exc).__name__}: {exc}"
            healthy = False
        now = time.time()
        job.status = "done"
        interrupted = self._shutdown.is_set()
        job.exit_code = -1 if interrupted else (0 if healthy else 1)
        job.finished_at = now
        job.elapsed = round(now - (job.started_at or now), 2)
        if interrupted:
            job.error = "server shut down during reset; reset will retry on restart"
        self._persist_job(job)
        with self._cond:
            self._current = None
            self._jobs.pop(job.id, None)
            if interrupted:
                self._state.state = "resetting"
                self._state.reset_pending = True
            elif healthy:
                self._state.state = "healthy"
                self._state.reset_epoch += 1
                self._state.last_reset_at = now
                self._state.dead_since = None
                self._state.dead_reason = None
            else:
                self._state.state = "dead"
                self._state.dead_since = now
                self._state.dead_reason = REBOOT_REQUIRED_MSG
            self._persist_state()
            self._cond.notify_all()
        if interrupted:
            return
        if not healthy:
            self._drain_pending(REBOOT_REQUIRED_MSG)

    def _new_reset_job(self) -> Job:
        job_id = uuid.uuid4().hex
        output_dir = self.config.log_dir / job_id
        output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        job = Job(
            id=job_id, cmd=f"[device reset] {self.config.reset_cmd}", cwd="",
            timeout=self.config.control_cmd_timeout, repeat=1, mode="reset",
            client_id="system", reset_epoch=self._state.reset_epoch,
            output_file=str(output_dir / "output"), status="running",
            started_at=time.time(),
        )
        self.store.create_job(job)
        with self._lock:
            self._jobs[job.id] = job
            self._current = job
        return job

    def _run_control(self, command: str, log: BoundedJobLog) -> int:
        if not command:
            log.append(b"[tt-device-queue] No command configured\n")
            return 1
        log.append(f"[tt-device-queue] $ {command}\n".encode())
        proc = None
        try:
            proc = subprocess.Popen(
                ["/bin/bash", "-lc", self._parent_guarded_script(command)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                env=self._job_env(), start_new_session=True,
            )
            deadline = time.time() + self.config.control_cmd_timeout
            assert proc.stdout is not None
            os.set_blocking(proc.stdout.fileno(), False)
            poller = select.poll()
            poller.register(proc.stdout.fileno(), select.POLLIN | select.POLLHUP | select.POLLERR)
            while proc.poll() is None:
                self._worker_heartbeat = time.monotonic()
                if time.time() >= deadline or self._shutdown.is_set():
                    self._signal_group(proc, signal.SIGKILL)
                if poller.poll(max(1, int(self.config.process_poll_interval * 1000))):
                    self._drain_fd(proc.stdout.fileno(), log)
            self._drain_fd(proc.stdout.fileno(), log)
            return proc.returncode if proc.returncode is not None else -9
        finally:
            if proc is not None:
                self._cleanup_process(proc)

    def _verify_health(self, log: BoundedJobLog) -> bool:
        if not self.config.health_check_cmd:
            return True
        log.append(b"[tt-device-queue] Verifying device health\n")
        return self._run_control(self.config.health_check_cmd, log) == 0

    def _drain_pending(self, message: str) -> None:
        with self._cond:
            jobs = [self._jobs[jid] for queue in self._pending.values() for jid in queue]
            self._pending.clear()
            self._rr_clients.clear()
        for job in jobs:
            self._append_terminal_message(job, message)
            job.status = "done"
            job.exit_code = -1
            job.finished_at = time.time()
            job.error = message
            self._persist_job(job)
            with self._lock:
                self._jobs.pop(job.id, None)

    def _maybe_prune(self) -> None:
        now = time.time()
        if now - self._last_prune < 300:
            return
        self._last_prune = now
        cutoff = (
            now - self.config.retention_days * 86400
            if self.config.retention_days else 0
        )
        try:
            paths = self.store.prune_completed(cutoff, self.config.max_completed_jobs)
            root = self.config.log_dir.resolve()
            for output in paths:
                directory = Path(output).resolve().parent
                if directory.parent == root:
                    shutil.rmtree(directory, ignore_errors=True)
        except Exception as exc:
            # Retention failure should be visible but must not stop device work.
            self._maintenance_warning = f"retention failure: {type(exc).__name__}: {exc}"

    def _write_meta(self, job: Job) -> None:
        try:
            path = Path(job.output_file).parent / "meta.json"
            temp = path.with_suffix(".tmp")
            temp.write_text(json.dumps(self._snapshot_base(job), indent=2, sort_keys=True) + "\n")
            os.replace(temp, path)
        except OSError:
            pass

    @staticmethod
    def _job_shell_script(cmd: str) -> str:
        return DeviceQueue._parent_guarded_script("\n".join([
            "printf '%s\\n' \"${TT_DEVICE_CHILD_OOM_SCORE_ADJ:-500}\" "
            "> /proc/$$/oom_score_adj 2>/dev/null || true",
            cmd,
        ]))

    @staticmethod
    def _parent_guarded_script(cmd: str) -> str:
        """Kill the process group if a manually-run queue server disappears.

        systemd's KillMode=control-group already provides this in production,
        but the guard prevents orphaned hardware users in manual/dev launches.
        """
        if os.environ.get("INVOCATION_ID"):
            return cmd
        server_pid = os.getpid()
        return "\n".join([
            f"_ttdq_server_pid={server_pid}",
            "(while kill -0 \"$_ttdq_server_pid\" 2>/dev/null; do sleep 0.2; done; "
            "kill -KILL 0) &",
            "_ttdq_watchdog=$!",
            "trap 'kill \"$_ttdq_watchdog\" 2>/dev/null || true' EXIT",
            cmd,
        ])

    @staticmethod
    def _job_env(extra: dict[str, str] | None = None) -> dict[str, str]:
        env = os.environ.copy()
        # Never pass reset-broker configuration or server-only settings into
        # arbitrary queued commands.
        for key in list(env):
            if key.startswith("TT_DEVICE_DEEP_RESET") or key.startswith("TT_DEVICE_RESET_CMD"):
                env.pop(key, None)
        if extra:
            env.update(extra)
        parts = [part for part in env.get("PYTHONPATH", "").split(os.pathsep) if part]
        if "." not in parts:
            env["PYTHONPATH"] = os.pathsep.join([".", *parts])
        return env

    @staticmethod
    def _signal_group(proc: subprocess.Popen[bytes], sig: signal.Signals) -> None:
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            try:
                proc.send_signal(sig)
            except (ProcessLookupError, PermissionError):
                pass

    def _cleanup_process(self, proc: subprocess.Popen[bytes]) -> None:
        if proc.poll() is None:
            self._signal_group(proc, signal.SIGKILL)
        try:
            proc.wait(timeout=max(1.0, self.config.stop_grace_sec))
        except subprocess.TimeoutExpired:
            self._signal_group(proc, signal.SIGKILL)
            try:
                proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                self._set_degraded(f"process {proc.pid} could not be reaped")
        finally:
            if proc.stdout:
                proc.stdout.close()
        if self._group_alive(proc.pid):
            self._signal_group(proc, signal.SIGKILL)
            deadline = time.monotonic() + 1.0
            while self._group_alive(proc.pid) and time.monotonic() < deadline:
                time.sleep(0.01)
            if self._group_alive(proc.pid):
                self._set_degraded(f"process group {proc.pid} survived cleanup")

    @staticmethod
    def _group_alive(pgid: int) -> bool:
        try:
            os.killpg(pgid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
