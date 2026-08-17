"""Structured logging, configured once, with no user data in it.

Everything this tool logs is a count, a duration, a stage id, or a configuration key. Task
metrics are aggregates by construction, but a diagnosis also sees *key values* when it names
the hot keys in a skewed join, and those are business data. Hot keys therefore appear in
reports, which are artifacts under the same access control as the job, and never in logs.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

_RESERVED = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
    "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
    "relativeCreated", "thread", "threadName", "processName", "process", "taskName",
    "message", "asctime",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            try:
                json.dumps(value)
            except (TypeError, ValueError):
                value = f"<unserialisable {type(value).__name__}>"
            payload[key] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info).splitlines()[-1]
        return json.dumps(payload, sort_keys=True)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        extras = " ".join(
            f"{k}={v}" for k, v in sorted(record.__dict__.items())
            if k not in _RESERVED and not k.startswith("_")
        )
        head = f"{record.levelname:<7} {record.getMessage()}"
        return f"{head}  {extras}" if extras else head


def configure(level: str = "INFO", fmt: str = "json") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root = logging.getLogger("skewdoc")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name if name.startswith("skewdoc") else f"skewdoc.{name}")
