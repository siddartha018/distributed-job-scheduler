from __future__ import annotations

import argparse
import json
import random
import time
from typing import Any

import httpx
import yaml


def _load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--jobs", type=int, default=50)
    args = parser.parse_args()

    cfg = _load_config(args.config)
    master = cfg["master"]
    url = f"http://{master['host']}:{int(master['port'])}"
    token = str(master["auth_token"])

    job_types = ["compute", "io", "long_task"]
    priorities = ["high", "medium", "low"]

    with httpx.Client(timeout=10.0) as client:
        for i in range(int(args.jobs)):
            body = {
                "job_type": random.choice(job_types),
                "priority": random.choice(priorities),
                "retries": random.randint(0, 3),
                "payload": {"kind": "soak", "i": i},
            }
            r = client.post(
                f"{url}/submit_job",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                content=json.dumps(body),
            )
            r.raise_for_status()
            time.sleep(0.02)

        # poll status a few times
        for _ in range(5):
            s = client.get(f"{url}/status", headers={"Authorization": f"Bearer {token}"})
            s.raise_for_status()
            print(json.dumps(s.json(), indent=2))
            time.sleep(2)


if __name__ == "__main__":
    main()

