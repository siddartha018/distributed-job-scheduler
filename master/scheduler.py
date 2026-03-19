from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.models import (
    HeartbeatRequest,
    JobLeaseResponse,
    JobResultRequest,
    JobStatus,
    JobSubmitRequest,
    MasterStatusResponse,
    Priority,
    WorkerStatus,
)
from master.state_manager import StateManager


@dataclass(frozen=True)
class Scheduler:
    state: StateManager
    lease_ttl_seconds: int
    heartbeat_timeout_seconds: int
    log: Any

    def submit_job(self, req: JobSubmitRequest, now_ms_: int) -> str:
        job_id = self.state.insert_job(req=req, now_ms_=now_ms_)
        self.log.info("job_submitted", extra={"job_id": job_id, "priority": req.priority, "job_type": req.job_type})
        return job_id

    def heartbeat(self, req: HeartbeatRequest, now_ms_: int) -> None:
        self.state.upsert_worker(worker_id=req.worker_id, host=req.host, metadata=req.metadata, now_ms_=now_ms_)

    def lease_next_job(self, worker_id: str, now_ms_: int) -> JobLeaseResponse | None:
        lease = self.state.lease_job_to_worker(
            worker_id=worker_id,
            now_ms_=now_ms_,
            lease_ttl_seconds=self.lease_ttl_seconds,
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
        )
        if lease is None:
            return None
        self.log.info(
            "job_leased",
            extra={"job_id": lease.job.job_id, "worker_id": worker_id, "lease_expires_at_ms": lease.lease_expires_at_ms},
        )
        return lease

    def record_job_result(self, req: JobResultRequest, now_ms_: int) -> None:
        updated = self.state.complete_job_attempt(req=req, now_ms_=now_ms_)
        if updated:
            self.log.info(
                "job_result_recorded",
                extra={
                    "job_id": req.job_id,
                    "worker_id": req.worker_id,
                    "attempt_id": req.attempt_id,
                    "status": req.status,
                },
            )

    def watchdog(self, now_ms_: int) -> None:
        summary = self.state.watchdog_sweep(
            now_ms_=now_ms_,
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
        )
        if summary["workers_marked_dead"] or summary["jobs_requeued"] or summary["jobs_failed_due_to_retries"]:
            self.log.warning("watchdog_sweep", extra=summary)

    def status(self, now_ms_: int, metrics_binary_path: Path) -> MasterStatusResponse:
        workers = self.state.list_workers(now_ms_=now_ms_, heartbeat_timeout_seconds=self.heartbeat_timeout_seconds)
        counts = self.state.job_counts()
        completed = self.state.list_recent_completed_jobs(limit=500)

        cpp_metrics: dict[str, Any] | None = None
        if metrics_binary_path.exists() and metrics_binary_path.is_file():
            try:
                payload = json.dumps({"jobs": completed}).encode("utf-8")
                proc = subprocess.run(
                    [str(metrics_binary_path)],
                    input=payload,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=2.0,
                    check=False,
                )
                if proc.returncode == 0:
                    cpp_metrics = json.loads(proc.stdout.decode("utf-8"))
                else:
                    self.log.warning(
                        "cpp_metrics_failed",
                        extra={"returncode": proc.returncode, "stderr": proc.stderr.decode("utf-8", errors="replace")},
                    )
            except Exception:
                self.log.exception("cpp_metrics_exception")

        return MasterStatusResponse(
            now_ms=now_ms_,
            job_counts=counts,
            workers=workers,
            cpp_metrics=cpp_metrics,
        )

