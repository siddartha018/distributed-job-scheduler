from __future__ import annotations

from pathlib import Path

import pytest

from common.models import JobStatus, JobSubmitRequest, JobType, Priority
from master.state_manager import StateManager


@pytest.fixture()
def state(tmp_path: Path) -> StateManager:
    sm = StateManager(sqlite_path=tmp_path / "scheduler.db")
    sm.init_db()
    return sm


def test_priority_ordering(state: StateManager) -> None:
    now = 1_000_000
    state.insert_job(JobSubmitRequest(job_type=JobType.compute, priority=Priority.low, retries=0, payload={}), now)
    state.insert_job(JobSubmitRequest(job_type=JobType.compute, priority=Priority.high, retries=0, payload={}), now + 1)
    state.insert_job(JobSubmitRequest(job_type=JobType.compute, priority=Priority.medium, retries=0, payload={}), now + 2)

    lease = state.lease_job_to_worker("w1", now_ms_=now + 3, lease_ttl_seconds=10, heartbeat_timeout_seconds=100)
    assert lease is not None
    assert lease.job.priority == Priority.high


def test_lease_expiry_requeues_and_counts_retries(state: StateManager) -> None:
    now = 1_000_000
    job_id = state.insert_job(
        JobSubmitRequest(job_type=JobType.compute, priority=Priority.high, retries=1, payload={}),
        now,
    )
    lease = state.lease_job_to_worker("w1", now_ms_=now + 1, lease_ttl_seconds=1, heartbeat_timeout_seconds=100)
    assert lease is not None
    assert lease.job.job_id == job_id

    # after lease expiry, watchdog should requeue and increment retries_used
    summary = state.watchdog_sweep(now_ms_=now + 2_500, heartbeat_timeout_seconds=100)
    assert summary["jobs_requeued"] == 1


def test_retry_exhaustion_marks_failed(state: StateManager) -> None:
    now = 1_000_000
    job_id = state.insert_job(
        JobSubmitRequest(job_type=JobType.compute, priority=Priority.high, retries=0, payload={}),
        now,
    )
    lease = state.lease_job_to_worker("w1", now_ms_=now + 1, lease_ttl_seconds=1, heartbeat_timeout_seconds=100)
    assert lease is not None
    summary = state.watchdog_sweep(now_ms_=now + 3_000, heartbeat_timeout_seconds=100)
    assert summary["jobs_failed_due_to_retries"] == 1

