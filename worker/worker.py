from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from common.models import HeartbeatRequest, JobLeaseResponse, JobResultRequest
from common.utils import ensure_dirs, now_ms, setup_rotating_logger
from worker.executor import ExecutorConfig, execute_job


def _load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass(frozen=True)
class WorkerConfig:
    master_url: str
    auth_token: str
    heartbeat_interval_seconds: float
    poll_interval_seconds: float
    job_timeout_seconds: int
    random_failure_rate: float
    random_sleep_min_seconds: int
    random_sleep_max_seconds: int


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--worker-id", required=True)
    args = parser.parse_args()

    config = _load_config(args.config)["worker"]
    cfg = WorkerConfig(
        master_url=str(config["master_url"]).rstrip("/"),
        auth_token=str(config["auth_token"]),
        heartbeat_interval_seconds=float(config["heartbeat_interval_seconds"]),
        poll_interval_seconds=float(config["poll_interval_seconds"]),
        job_timeout_seconds=int(config["job_timeout_seconds"]),
        random_failure_rate=float(config["random_failure_rate"]),
        random_sleep_min_seconds=int(config["random_sleep_min_seconds"]),
        random_sleep_max_seconds=int(config["random_sleep_max_seconds"]),
    )

    ensure_dirs([Path("logs")])
    log = setup_rotating_logger(f"worker-{args.worker_id}", Path(f"logs/worker-{args.worker_id}.log"))

    executor_cfg = ExecutorConfig(
        random_failure_rate=cfg.random_failure_rate,
        random_sleep_min_seconds=cfg.random_sleep_min_seconds,
        random_sleep_max_seconds=cfg.random_sleep_max_seconds,
        job_timeout_seconds=cfg.job_timeout_seconds,
    )

    worker_id = str(args.worker_id)
    host = socket.gethostname()

    log.info("worker_starting", extra={"worker_id": worker_id, "master_url": cfg.master_url})

    next_hb = 0.0
    with httpx.Client(timeout=10.0) as client:
        while True:
            now = time.time()
            if now >= next_hb:
                _send_heartbeat(client=client, cfg=cfg, worker_id=worker_id, host=host, log=log)
                next_hb = now + cfg.heartbeat_interval_seconds

            lease = _poll_for_job(client=client, cfg=cfg, worker_id=worker_id, log=log)
            if lease is None:
                time.sleep(cfg.poll_interval_seconds)
                continue

            log.info("job_received", extra={"job_id": lease.job.job_id, "attempt_id": lease.attempt_id})
            result = execute_job(lease.job, executor_cfg, log=log)
            _send_result(
                client=client,
                cfg=cfg,
                worker_id=worker_id,
                job_id=lease.job.job_id,
                attempt_id=lease.attempt_id,
                result=result,
                log=log,
            )


def _send_heartbeat(client: httpx.Client, cfg: WorkerConfig, worker_id: str, host: str, log: Any) -> None:
    req = HeartbeatRequest(worker_id=worker_id, host=host, metadata={"pid": os.getpid()})
    try:
        r = client.post(f"{cfg.master_url}/heartbeat", headers=_headers(cfg.auth_token), json=req.model_dump())
        if r.status_code >= 400:
            log.warning("heartbeat_failed", extra={"status_code": r.status_code, "body": r.text})
    except Exception as e:
        log.warning("heartbeat_exception", extra={"error": str(e)})


def _poll_for_job(client: httpx.Client, cfg: WorkerConfig, worker_id: str, log: Any) -> JobLeaseResponse | None:
    try:
        r = client.get(f"{cfg.master_url}/get_job", headers=_headers(cfg.auth_token), params={"worker_id": worker_id})
        if r.status_code == 204:
            return None
        if r.status_code >= 400:
            log.warning("get_job_failed", extra={"status_code": r.status_code, "body": r.text})
            return None
        return JobLeaseResponse.model_validate(r.json())
    except Exception as e:
        log.warning("get_job_exception", extra={"error": str(e)})
        return None


def _send_result(
    client: httpx.Client,
    cfg: WorkerConfig,
    worker_id: str,
    job_id: str,
    attempt_id: str,
    result: Any,
    log: Any,
) -> None:
    req = JobResultRequest(
        job_id=job_id,
        worker_id=worker_id,
        attempt_id=attempt_id,
        status=result.status,
        execution_time_ms=result.execution_time_ms,
        error=result.error,
        stdout=result.stdout,
        stderr=result.stderr,
    )
    try:
        r = client.post(f"{cfg.master_url}/job_result", headers=_headers(cfg.auth_token), json=req.model_dump())
        if r.status_code >= 400:
            log.warning("job_result_failed", extra={"status_code": r.status_code, "body": r.text, "job_id": job_id})
        else:
            log.info("job_result_sent", extra={"job_id": job_id, "status": req.status.value})
    except Exception as e:
        log.warning("job_result_exception", extra={"error": str(e), "job_id": job_id})


if __name__ == "__main__":
    main()

