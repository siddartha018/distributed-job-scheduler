from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from common.models import (
    HeartbeatRequest,
    JobLeaseResponse,
    JobRecord,
    JobResultRequest,
    JobStatus,
    JobSubmitRequest,
    Priority,
    WorkerInfo,
    WorkerStatus,
)


def _priority_rank(p: Priority) -> int:
    return {Priority.high: 0, Priority.medium: 1, Priority.low: 2}[p]


@dataclass(frozen=True)
class StateManager:
    sqlite_path: Path

    def _connect(self) -> sqlite3.Connection:
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.sqlite_path, timeout=30.0, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  job_id TEXT PRIMARY KEY,
                  job_type TEXT NOT NULL,
                  priority TEXT NOT NULL,
                  priority_rank INTEGER NOT NULL,
                  payload_json TEXT NOT NULL,
                  retries_max INTEGER NOT NULL,
                  retries_used INTEGER NOT NULL,
                  status TEXT NOT NULL,
                  created_at_ms INTEGER NOT NULL,
                  updated_at_ms INTEGER NOT NULL,
                  leased_to_worker_id TEXT,
                  lease_expires_at_ms INTEGER,
                  last_error TEXT,
                  execution_time_ms INTEGER
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(status, priority_rank, created_at_ms);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs(status, lease_expires_at_ms);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_worker ON jobs(leased_to_worker_id, status);")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workers (
                  worker_id TEXT PRIMARY KEY,
                  host TEXT,
                  metadata_json TEXT NOT NULL,
                  status TEXT NOT NULL,
                  last_heartbeat_at_ms INTEGER NOT NULL,
                  updated_at_ms INTEGER NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status, last_heartbeat_at_ms);")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_attempts (
                  attempt_id TEXT PRIMARY KEY,
                  job_id TEXT NOT NULL,
                  worker_id TEXT NOT NULL,
                  started_at_ms INTEGER NOT NULL,
                  ended_at_ms INTEGER,
                  status TEXT NOT NULL,
                  error TEXT,
                  stdout TEXT,
                  stderr TEXT,
                  FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_job ON job_attempts(job_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_worker ON job_attempts(worker_id);")

    def insert_job(self, req: JobSubmitRequest, now_ms_: int) -> str:
        job_id = req.job_id or str(uuid.uuid4())
        payload_json = json.dumps(req.payload or {})
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                conn.execute(
                    """
                    INSERT INTO jobs(
                      job_id, job_type, priority, priority_rank, payload_json,
                      retries_max, retries_used, status, created_at_ms, updated_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        job_id,
                        req.job_type.value,
                        req.priority.value,
                        _priority_rank(req.priority),
                        payload_json,
                        int(req.retries),
                        0,
                        JobStatus.queued.value,
                        now_ms_,
                        now_ms_,
                    ),
                )
                conn.execute("COMMIT;")
            except sqlite3.IntegrityError:
                conn.execute("ROLLBACK;")
                raise ValueError(f"job_id already exists: {job_id}")
        return job_id

    def upsert_worker(self, worker_id: str, host: str | None, metadata: dict[str, Any] | None, now_ms_: int) -> None:
        # Important: callers like `lease_job_to_worker()` may not know host/metadata.
        # We must not overwrite existing values with NULL/empty in that case.
        meta = json.dumps(metadata) if metadata is not None else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO workers(worker_id, host, metadata_json, status, last_heartbeat_at_ms, updated_at_ms)
                VALUES (?, ?, COALESCE(?, '{}'), ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                  host=COALESCE(excluded.host, workers.host),
                  metadata_json=COALESCE(excluded.metadata_json, workers.metadata_json),
                  status=?,
                  last_heartbeat_at_ms=excluded.last_heartbeat_at_ms,
                  updated_at_ms=excluded.updated_at_ms;
                """,
                (
                    worker_id,
                    host,
                    meta,
                    WorkerStatus.alive.value,
                    now_ms_,
                    now_ms_,
                    WorkerStatus.alive.value,
                ),
            )

    def lease_job_to_worker(
        self,
        worker_id: str,
        now_ms_: int,
        lease_ttl_seconds: int,
        heartbeat_timeout_seconds: int,
    ) -> JobLeaseResponse | None:
        self.upsert_worker(worker_id=worker_id, host=None, metadata=None, now_ms_=now_ms_)
        lease_expires_at_ms = now_ms_ + int(lease_ttl_seconds * 1000)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                # refuse leasing to dead workers
                row = conn.execute("SELECT status, last_heartbeat_at_ms FROM workers WHERE worker_id=?;", (worker_id,)).fetchone()
                if row is None:
                    conn.execute("ROLLBACK;")
                    return None
                last_hb = int(row["last_heartbeat_at_ms"])
                if now_ms_ - last_hb > int(heartbeat_timeout_seconds * 1000):
                    conn.execute(
                        "UPDATE workers SET status=?, updated_at_ms=? WHERE worker_id=?;",
                        (WorkerStatus.dead.value, now_ms_, worker_id),
                    )
                    conn.execute("COMMIT;")
                    return None

                # Enforce least-loaded worker policy (among alive workers).
                loads = {
                    str(r["leased_to_worker_id"]): int(r["c"])
                    for r in conn.execute(
                        """
                        SELECT leased_to_worker_id, COUNT(*) AS c
                        FROM jobs
                        WHERE leased_to_worker_id IS NOT NULL AND status IN (?, ?)
                        GROUP BY leased_to_worker_id;
                        """,
                        (JobStatus.leased.value, JobStatus.running.value),
                    ).fetchall()
                }
                alive_workers = conn.execute(
                    "SELECT worker_id FROM workers WHERE status=?;",
                    (WorkerStatus.alive.value,),
                ).fetchall()
                if alive_workers:
                    min_load = min(loads.get(str(r["worker_id"]), 0) for r in alive_workers)
                    my_load = loads.get(worker_id, 0)
                    if my_load > min_load:
                        conn.execute("COMMIT;")
                        return None

                job_row = conn.execute(
                    """
                    SELECT job_id, job_type, priority, payload_json, retries_max, retries_used, status,
                           created_at_ms, updated_at_ms
                    FROM jobs
                    WHERE status=?
                    ORDER BY priority_rank ASC, created_at_ms ASC
                    LIMIT 1;
                    """,
                    (JobStatus.queued.value,),
                ).fetchone()

                if job_row is None:
                    conn.execute("COMMIT;")
                    return None

                job_id = str(job_row["job_id"])
                updated = conn.execute(
                    """
                    UPDATE jobs
                    SET status=?,
                        leased_to_worker_id=?,
                        lease_expires_at_ms=?,
                        updated_at_ms=?
                    WHERE job_id=? AND status=?;
                    """,
                    (
                        JobStatus.leased.value,
                        worker_id,
                        lease_expires_at_ms,
                        now_ms_,
                        job_id,
                        JobStatus.queued.value,
                    ),
                )
                if updated.rowcount != 1:
                    conn.execute("COMMIT;")
                    return None

                attempt_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO job_attempts(attempt_id, job_id, worker_id, started_at_ms, status)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (attempt_id, job_id, worker_id, now_ms_, JobStatus.running.value),
                )
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise

        job = JobRecord(
            job_id=job_id,
            job_type=job_row["job_type"],
            priority=job_row["priority"],
            retries=int(job_row["retries_max"]),
            retries_used=int(job_row["retries_used"]),
            status=JobStatus.leased,
            payload=json.loads(job_row["payload_json"]),
            execution_time_ms=None,
        )
        return JobLeaseResponse(job=job, attempt_id=attempt_id, lease_expires_at_ms=lease_expires_at_ms)

    def complete_job_attempt(self, req: JobResultRequest, now_ms_: int) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                attempt = conn.execute(
                    "SELECT attempt_id, job_id, status FROM job_attempts WHERE attempt_id=? AND job_id=?;",
                    (req.attempt_id, req.job_id),
                ).fetchone()
                if attempt is None:
                    conn.execute("COMMIT;")
                    return False
                if attempt["status"] in (JobStatus.succeeded.value, JobStatus.failed.value):
                    conn.execute("COMMIT;")
                    return False

                # Update attempt record
                conn.execute(
                    """
                    UPDATE job_attempts
                    SET ended_at_ms=?, status=?, error=?, stdout=?, stderr=?
                    WHERE attempt_id=?;
                    """,
                    (
                        now_ms_,
                        req.status.value,
                        req.error,
                        req.stdout,
                        req.stderr,
                        req.attempt_id,
                    ),
                )

                job_row = conn.execute(
                    "SELECT retries_max, retries_used FROM jobs WHERE job_id=?;",
                    (req.job_id,),
                ).fetchone()
                if job_row is None:
                    conn.execute("COMMIT;")
                    return False
                retries_max = int(job_row["retries_max"])
                retries_used = int(job_row["retries_used"])

                if req.status == JobStatus.succeeded:
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status=?,
                            updated_at_ms=?,
                            lease_expires_at_ms=NULL,
                            leased_to_worker_id=NULL,
                            last_error=NULL,
                            execution_time_ms=?
                        WHERE job_id=?;
                        """,
                        (JobStatus.succeeded.value, now_ms_, req.execution_time_ms, req.job_id),
                    )
                    conn.execute("COMMIT;")
                    return True

                # failed
                retries_used_next = retries_used + 1
                if retries_used_next <= retries_max:
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status=?,
                            retries_used=?,
                            updated_at_ms=?,
                            leased_to_worker_id=NULL,
                            lease_expires_at_ms=NULL,
                            last_error=?
                        WHERE job_id=?;
                        """,
                        (
                            JobStatus.queued.value,
                            retries_used_next,
                            now_ms_,
                            req.error,
                            req.job_id,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status=?,
                            retries_used=?,
                            updated_at_ms=?,
                            leased_to_worker_id=NULL,
                            lease_expires_at_ms=NULL,
                            last_error=?
                        WHERE job_id=?;
                        """,
                        (
                            JobStatus.failed.value,
                            retries_used_next,
                            now_ms_,
                            req.error,
                            req.job_id,
                        ),
                    )
                conn.execute("COMMIT;")
                return True
            except Exception:
                conn.execute("ROLLBACK;")
                raise

    def watchdog_sweep(self, now_ms_: int, heartbeat_timeout_seconds: int) -> dict[str, int]:
        hb_timeout_ms = int(heartbeat_timeout_seconds * 1000)
        workers_marked_dead = 0
        jobs_requeued = 0
        jobs_failed_due_to_retries = 0

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                dead_rows = conn.execute(
                    """
                    SELECT worker_id FROM workers
                    WHERE status=? AND (? - last_heartbeat_at_ms) > ?;
                    """,
                    (WorkerStatus.alive.value, now_ms_, hb_timeout_ms),
                ).fetchall()
                if dead_rows:
                    conn.execute(
                        """
                        UPDATE workers SET status=?, updated_at_ms=?
                        WHERE status=? AND (? - last_heartbeat_at_ms) > ?;
                        """,
                        (WorkerStatus.dead.value, now_ms_, WorkerStatus.alive.value, now_ms_, hb_timeout_ms),
                    )
                    workers_marked_dead = len(dead_rows)

                # Requeue expired leases or jobs leased to dead workers
                expired = conn.execute(
                    """
                    SELECT job_id, retries_max, retries_used
                    FROM jobs
                    WHERE status IN (?, ?) AND (
                      (lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms < ?)
                      OR (leased_to_worker_id IS NOT NULL AND leased_to_worker_id IN (SELECT worker_id FROM workers WHERE status=?))
                    );
                    """,
                    (JobStatus.leased.value, JobStatus.running.value, now_ms_, WorkerStatus.dead.value),
                ).fetchall()

                for r in expired:
                    job_id = str(r["job_id"])
                    retries_max = int(r["retries_max"])
                    retries_used = int(r["retries_used"]) + 1
                    if retries_used <= retries_max:
                        conn.execute(
                            """
                            UPDATE jobs
                            SET status=?, retries_used=?, updated_at_ms=?,
                                leased_to_worker_id=NULL, lease_expires_at_ms=NULL,
                                last_error=COALESCE(last_error, 'requeued_due_to_worker_failure_or_lease_expiry')
                            WHERE job_id=?;
                            """,
                            (JobStatus.queued.value, retries_used, now_ms_, job_id),
                        )
                        jobs_requeued += 1
                    else:
                        conn.execute(
                            """
                            UPDATE jobs
                            SET status=?, retries_used=?, updated_at_ms=?,
                                leased_to_worker_id=NULL, lease_expires_at_ms=NULL,
                                last_error=COALESCE(last_error, 'failed_due_to_retries_exhausted')
                            WHERE job_id=?;
                            """,
                            (JobStatus.failed.value, retries_used, now_ms_, job_id),
                        )
                        jobs_failed_due_to_retries += 1

                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise

        return {
            "workers_marked_dead": workers_marked_dead,
            "jobs_requeued": jobs_requeued,
            "jobs_failed_due_to_retries": jobs_failed_due_to_retries,
        }

    def list_workers(self, now_ms_: int, heartbeat_timeout_seconds: int) -> list[WorkerInfo]:
        hb_timeout_ms = int(heartbeat_timeout_seconds * 1000)
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM workers ORDER BY worker_id ASC;").fetchall()

            loads = {
                str(r["leased_to_worker_id"]): int(r["c"])
                for r in conn.execute(
                    """
                    SELECT leased_to_worker_id, COUNT(*) AS c
                    FROM jobs
                    WHERE leased_to_worker_id IS NOT NULL AND status IN (?, ?)
                    GROUP BY leased_to_worker_id;
                    """,
                    (JobStatus.leased.value, JobStatus.running.value),
                ).fetchall()
            }

        out: list[WorkerInfo] = []
        for r in rows:
            last_hb = int(r["last_heartbeat_at_ms"])
            effective_status = WorkerStatus.dead if (now_ms_ - last_hb) > hb_timeout_ms else WorkerStatus(r["status"])
            meta = json.loads(r["metadata_json"])
            out.append(
                WorkerInfo(
                    worker_id=str(r["worker_id"]),
                    host=r["host"],
                    status=effective_status,
                    last_heartbeat_at_ms=last_hb,
                    in_flight_jobs=loads.get(str(r["worker_id"]), 0),
                    metadata=meta,
                )
            )
        return out

    def job_counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS c FROM jobs GROUP BY status;").fetchall()
        return {str(r["status"]): int(r["c"]) for r in rows}

    def list_recent_completed_jobs(self, limit: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT job_id, status, execution_time_ms
                FROM jobs
                WHERE status IN (?, ?) AND execution_time_ms IS NOT NULL
                ORDER BY updated_at_ms DESC
                LIMIT ?;
                """,
                (JobStatus.succeeded.value, JobStatus.failed.value, int(limit)),
            ).fetchall()
        return [
            {"job_id": str(r["job_id"]), "status": str(r["status"]), "execution_time_ms": int(r["execution_time_ms"])}
            for r in rows
        ]

