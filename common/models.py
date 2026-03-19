from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobType(str, Enum):
    compute = "compute"
    io = "io"
    long_task = "long_task"
    flow = "flow"


class Priority(str, Enum):
    high = "high"
    medium = "medium"
    low = "low"


class JobStatus(str, Enum):
    queued = "queued"
    leased = "leased"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class WorkerStatus(str, Enum):
    alive = "alive"
    dead = "dead"


class JobSubmitRequest(BaseModel):
    job_id: str | None = Field(default=None, min_length=1)
    job_type: JobType
    priority: Priority
    retries: int = Field(default=3, ge=0, le=50)
    payload: dict[str, Any] | None = None


class JobRecord(BaseModel):
    job_id: str
    job_type: JobType | str
    priority: Priority | str
    retries: int
    retries_used: int
    status: JobStatus
    payload: dict[str, Any]
    execution_time_ms: int | None


class JobLeaseResponse(BaseModel):
    job: JobRecord
    attempt_id: str
    lease_expires_at_ms: int


class JobResultRequest(BaseModel):
    job_id: str = Field(min_length=1)
    worker_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    status: JobStatus
    execution_time_ms: int = Field(ge=0)
    error: str | None = None
    stdout: str | None = None
    stderr: str | None = None


class HeartbeatRequest(BaseModel):
    worker_id: str = Field(min_length=1)
    host: str | None = None
    metadata: dict[str, Any] | None = None


class WorkerInfo(BaseModel):
    worker_id: str
    host: str | None
    status: WorkerStatus
    last_heartbeat_at_ms: int
    in_flight_jobs: int
    metadata: dict[str, Any]


class MasterStatusResponse(BaseModel):
    now_ms: int
    job_counts: dict[str, int]
    workers: list[WorkerInfo]
    cpp_metrics: dict[str, Any] | None = None

