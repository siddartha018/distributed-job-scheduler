from __future__ import annotations

import argparse
import asyncio
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
import yaml
from fastapi import Depends, FastAPI, Header, HTTPException, Response, status

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from common.models import (
    HeartbeatRequest,
    JobLeaseResponse,
    JobResultRequest,
    JobSubmitRequest,
    MasterStatusResponse,
)
from common.utils import ensure_dirs, now_ms, setup_rotating_logger
from master.scheduler import Scheduler
from master.state_manager import StateManager


def _load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _auth_dependency(expected_token: str):
    def _check(authorization: str | None = Header(default=None)) -> None:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Bearer token")
        token = authorization.removeprefix("Bearer ").strip()
        if token != expected_token:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

    return _check


def create_app(config: dict[str, Any]) -> FastAPI:
    master_cfg = config["master"]
    ensure_dirs([Path("logs"), Path("state")])
    log = setup_rotating_logger("master", Path("logs/master.log"))

    state = StateManager(sqlite_path=Path(master_cfg["sqlite_path"]))
    state.init_db()
    scheduler = Scheduler(
        state=state,
        lease_ttl_seconds=int(master_cfg["lease_ttl_seconds"]),
        heartbeat_timeout_seconds=int(master_cfg["heartbeat_timeout_seconds"]),
        log=log,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        async def watchdog_loop() -> None:
            interval = float(master_cfg["watchdog_interval_seconds"])
            while True:
                try:
                    scheduler.watchdog(now_ms())
                except Exception:
                    log.exception("watchdog_failed")
                await asyncio.sleep(interval)

        task = asyncio.create_task(watchdog_loop())
        try:
            yield
        finally:
            task.cancel()

    app = FastAPI(title="Distributed Job Scheduler Master", version="1.0", lifespan=lifespan)

    require_auth = Depends(_auth_dependency(str(master_cfg["auth_token"])))

    @app.post("/submit_job")
    async def submit_job(req: JobSubmitRequest, _: None = require_auth) -> dict[str, Any]:
        job_id = scheduler.submit_job(req, now_ms())
        return {"job_id": job_id}

    @app.get("/get_job", response_model=JobLeaseResponse)
    async def get_job(worker_id: str, _: None = require_auth) -> Response | JobLeaseResponse:
        leased = scheduler.lease_next_job(worker_id=worker_id, now_ms_=now_ms())
        if leased is None:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        return leased

    @app.post("/job_result")
    async def job_result(req: JobResultRequest, _: None = require_auth) -> dict[str, Any]:
        scheduler.record_job_result(req, now_ms())
        return {"ok": True}

    @app.post("/heartbeat")
    async def heartbeat(req: HeartbeatRequest, _: None = require_auth) -> dict[str, Any]:
        scheduler.heartbeat(req, now_ms())
        return {"ok": True}

    @app.get("/status", response_model=MasterStatusResponse)
    async def status_endpoint(_: None = require_auth) -> MasterStatusResponse:
        return scheduler.status(now_ms_=now_ms(), metrics_binary_path=Path(master_cfg["metrics_binary_path"]))

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()

    config = _load_config(args.config)
    master_cfg = config["master"]
    app = create_app(config)
    uvicorn.run(app, host=master_cfg["host"], port=int(master_cfg["port"]))


if __name__ == "__main__":
    main()

