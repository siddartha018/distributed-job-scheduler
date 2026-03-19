from __future__ import annotations

import json
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path


def now_ms() -> int:
    return int(time.time() * 1000)


def ensure_dirs(paths: list[Path]) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


class _JsonishFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: dict[str, object] = {
            "ts_ms": now_ms(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Capture "extra" fields passed via logging's `extra=...`.
        reserved = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
        }
        for k, v in record.__dict__.items():
            if k in reserved:
                continue
            if k.startswith("_"):
                continue
            # Avoid clobbering base fields.
            if k in base:
                continue
            base[k] = v
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, ensure_ascii=False)


def setup_rotating_logger(name: str, log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    handler.setFormatter(_JsonishFormatter())
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(_JsonishFormatter())
    logger.addHandler(console)
    return logger

