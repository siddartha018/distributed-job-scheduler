from __future__ import annotations

import json
import random
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.models import JobRecord, JobStatus, JobType


class ExecutionError(RuntimeError):
    pass


class FlowDefinitionError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecutionResult:
    status: JobStatus
    execution_time_ms: int
    error: str | None
    stdout: str | None
    stderr: str | None


@dataclass(frozen=True)
class ExecutorConfig:
    random_failure_rate: float
    random_sleep_min_seconds: int
    random_sleep_max_seconds: int
    job_timeout_seconds: int


def execute_job(job: JobRecord, cfg: ExecutorConfig, log: Any) -> ExecutionResult:
    start = time.time()
    stdout_acc: list[str] = []
    stderr_acc: list[str] = []

    try:
        if job.job_type == JobType.flow or (isinstance(job.payload, dict) and "flow" in job.payload):
            flow_stdout, flow_stderr = _execute_flow(job=job, cfg=cfg, log=log)
            if flow_stdout:
                stdout_acc.append(flow_stdout)
            if flow_stderr:
                stderr_acc.append(flow_stderr)
        else:
            _simulate_work(job=job, cfg=cfg, log=log)
            tcl_script = (job.payload or {}).get("tcl_script")
            if isinstance(tcl_script, str) and tcl_script.strip():
                out, err = _run_tcl(Path(tcl_script), timeout_s=cfg.job_timeout_seconds)
                stdout_acc.append(out)
                stderr_acc.append(err)

        _maybe_fail(cfg.random_failure_rate)
        status = JobStatus.succeeded
        err_msg = None
    except Exception as e:
        status = JobStatus.failed
        err_msg = str(e)
        log.warning("job_execution_failed", extra={"job_id": job.job_id, "error": err_msg})

    elapsed_ms = int((time.time() - start) * 1000)
    return ExecutionResult(
        status=status,
        execution_time_ms=max(elapsed_ms, 0),
        error=err_msg,
        stdout="\n".join([s for s in stdout_acc if s]),
        stderr="\n".join([s for s in stderr_acc if s]),
    )


def _simulate_work(job: JobRecord, cfg: ExecutorConfig, log: Any) -> None:
    if job.job_type == JobType.long_task:
        sleep_s = random.randint(cfg.random_sleep_min_seconds + 2, cfg.random_sleep_max_seconds + 6)
    elif job.job_type == JobType.io:
        sleep_s = random.randint(cfg.random_sleep_min_seconds, cfg.random_sleep_max_seconds)
    else:
        sleep_s = random.randint(cfg.random_sleep_min_seconds, cfg.random_sleep_max_seconds)
    log.info("job_simulate_sleep", extra={"job_id": job.job_id, "sleep_s": sleep_s, "job_type": str(job.job_type)})
    time.sleep(sleep_s)


def _maybe_fail(rate: float) -> None:
    if rate <= 0:
        return
    if random.random() < rate:
        raise ExecutionError("simulated_random_failure")


def _run_tcl(script_path: Path, timeout_s: int) -> tuple[str, str]:
    proc = subprocess.run(
        ["tclsh", str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=float(timeout_s),
        check=False,
        text=True,
    )
    out = proc.stdout or ""
    err = proc.stderr or ""
    if proc.returncode != 0:
        raise ExecutionError(f"tcl_failed(returncode={proc.returncode}): {err.strip()}")
    return out, err


def _execute_flow(job: JobRecord, cfg: ExecutorConfig, log: Any) -> tuple[str, str]:
    payload = job.payload or {}
    flow = payload.get("flow")
    if not isinstance(flow, dict):
        raise FlowDefinitionError("flow payload must be an object at payload.flow")
    steps = flow.get("steps")
    if not isinstance(steps, list) or not steps:
        raise FlowDefinitionError("flow.steps must be a non-empty list")

    normalized_steps: dict[str, dict[str, Any]] = {}
    deps: dict[str, set[str]] = {}

    for raw in steps:
        if not isinstance(raw, dict):
            raise FlowDefinitionError("each flow step must be an object")
        step_id = raw.get("id")
        if not isinstance(step_id, str) or not step_id.strip():
            raise FlowDefinitionError("each flow step must have non-empty string id")
        if step_id in normalized_steps:
            raise FlowDefinitionError(f"duplicate step id: {step_id}")
        step_deps = raw.get("deps", [])
        if step_deps is None:
            step_deps = []
        if not isinstance(step_deps, list) or any(not isinstance(d, str) or not d.strip() for d in step_deps):
            raise FlowDefinitionError(f"step.deps must be list[str] for step {step_id}")
        normalized_steps[step_id] = raw
        deps[step_id] = set(step_deps)

    for step_id, step_deps in deps.items():
        missing = [d for d in step_deps if d not in normalized_steps]
        if missing:
            raise FlowDefinitionError(f"step {step_id} depends on unknown steps: {missing}")

    order = topo_sort_step_deps(deps)
    results: list[dict[str, Any]] = []

    for step_id in order:
        step = normalized_steps[step_id]
        step_type = step.get("type", "simulate")
        if not isinstance(step_type, str):
            raise FlowDefinitionError(f"step.type must be a string for step {step_id}")

        started_ms = int(time.time() * 1000)
        log.info("flow_step_start", extra={"job_id": job.job_id, "step_id": step_id, "step_type": step_type})
        step_stdout = ""
        step_stderr = ""
        status = "succeeded"
        error: str | None = None
        try:
            if step_type == "simulate":
                seconds = step.get("sleep_seconds")
                if seconds is None:
                    seconds = random.randint(cfg.random_sleep_min_seconds, cfg.random_sleep_max_seconds)
                if not isinstance(seconds, (int, float)) or seconds < 0:
                    raise FlowDefinitionError(f"step.sleep_seconds must be non-negative number for step {step_id}")
                time.sleep(float(seconds))
                step_stdout = f"simulate: slept {seconds}s"
            elif step_type == "tcl":
                script = step.get("script") or (job.payload or {}).get("tcl_script")
                if not isinstance(script, str) or not script.strip():
                    raise FlowDefinitionError(f"tcl step requires script for step {step_id}")
                out, err = _run_tcl(Path(script), timeout_s=cfg.job_timeout_seconds)
                step_stdout = out
                step_stderr = err
            else:
                raise FlowDefinitionError(f"unknown step type '{step_type}' for step {step_id}")

            # optional per-step failure injection (CAD flows often have flaky tools)
            step_fail_rate = step.get("random_failure_rate")
            if step_fail_rate is not None:
                if not isinstance(step_fail_rate, (int, float)) or step_fail_rate < 0 or step_fail_rate > 1:
                    raise FlowDefinitionError(f"step.random_failure_rate must be in [0,1] for step {step_id}")
                _maybe_fail(float(step_fail_rate))
        except Exception as e:
            status = "failed"
            error = str(e)
            log.warning("flow_step_failed", extra={"job_id": job.job_id, "step_id": step_id, "error": error})

        ended_ms = int(time.time() * 1000)
        results.append(
            {
                "id": step_id,
                "type": step_type,
                "status": status,
                "started_at_ms": started_ms,
                "ended_at_ms": ended_ms,
                "duration_ms": max(ended_ms - started_ms, 0),
                "error": error,
                "stdout": step_stdout,
                "stderr": step_stderr,
            }
        )
        if status != "succeeded":
            raise ExecutionError(f"flow_failed_at_step={step_id}: {error}")

    return json.dumps({"flow_results": results}, ensure_ascii=False), ""


def topo_sort_step_deps(deps: dict[str, set[str]]) -> list[str]:
    # Kahn's algorithm (stable-ish ordering by step_id for determinism)
    incoming = {k: set(v) for k, v in deps.items()}
    ready = sorted([k for k, v in incoming.items() if not v])
    out: list[str] = []

    while ready:
        n = ready.pop(0)
        out.append(n)
        for m, m_deps in incoming.items():
            if n in m_deps:
                m_deps.remove(n)
        newly_ready = sorted([k for k, v in incoming.items() if not v and k not in out and k not in ready])
        for k in newly_ready:
            if k not in ready:
                ready.append(k)

    if len(out) != len(incoming):
        cycle_nodes = sorted([k for k, v in incoming.items() if v])
        raise FlowDefinitionError(f"flow_has_cycle_or_unresolved_deps: {cycle_nodes}")
    return out

